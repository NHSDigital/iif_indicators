# Databricks notebook source
# MAGIC %md 
# MAGIC ## IIF ACSC Indicators

# COMMAND ----------

# MAGIC %run ./functions/01_create_denominator

# COMMAND ----------

# MAGIC %run ./functions/02_create_numerator

# COMMAND ----------

# MAGIC %run ./functions/03_apply_standardisation

# COMMAND ----------

# MAGIC %run ./functions/04_create_outputs

# COMMAND ----------

# MAGIC %run ./functions/utils

# COMMAND ----------

# MAGIC %run ./constants/parameters

# COMMAND ----------

# MAGIC %run ./constants/field_definitions

# COMMAND ----------

import pyspark.sql.functions as F
import pandas as pd

# COMMAND ----------

# DBTITLE 1,Configure dates
#Enter report date here
report_date = '2022-09-30'

date_obj = reportDate(report_date)

RPSD = date_obj.financial_year_start
RPED = pd.to_datetime(date_obj.reporting_period_end).date()
report_period_month_start = date_obj.reporting_month_01

baseline_RPSD = date_obj.get_baseline_financial_year_start()
baseline_RPED = date_obj.baseline_report_period_end()
baseline_month_start = date_obj.get_baseline_month_01()

financial_year_yyyy = baseline_RPSD.strftime('%y')+RPSD.strftime('%y')

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Notebook/process description: 
# MAGIC 1. Create PCN mapping and list size count tables for acheivment period (denominator).  
# MAGIC 2. Filter HES data and create cohorts for numerator. 
# MAGIC 3. Create Standardised numerator count
# MAGIC 4. Prepares CQRS compatible outputs.

# COMMAND ----------

# DBTITLE 1,Create denominator
current_pcn_mapping_df = create_pcn_mapping(report_date)
create_table('pcn_mapping', 'iif_indicators_collab', current_pcn_mapping_df)

current_list_size_df = retrieve_pcn_list_size(report_period_month_start, current_pcn_mapping_df, denominator_indicator_id)
baseline_list_size_df = retrieve_pcn_list_size(baseline_month_start, current_pcn_mapping_df, baseline_denominator_indicator_id)
list_size = current_list_size_df.unionByName(baseline_list_size_df)

create_table('list_size', 'iif_indicators_collab', list_size)

england_list_size_df = retrieve_england_list_size_by_pcn(RPSD)
create_table('list_size_england', 'iif_indicators_collab', england_list_size_df)

denominator_df = create_denominator(report_date, current_pcn_mapping_df)
create_table('acsc_cqrs_denominator', 'iif_indicators_collab', denominator_df)

# COMMAND ----------

# DBTITLE 1,Create numerator
current_fy_numerator_df = create_numerator(database, current_fy_table, RPSD, RPED, report_period_month_start)
create_table('amb_care_pats', 'iif_indicators_collab', current_fy_numerator_df)

previous_fy_numerator_df = create_numerator(database, previous_fy_table, baseline_RPSD, baseline_RPED, baseline_month_start)
create_table('baseline_amb_care_pats', 'iif_indicators_collab', previous_fy_numerator_df)

# COMMAND ----------

# DBTITLE 1,Apply standardisation
attribute_codes = get_attribute_codes()
current_year_standardised_numerator_df = generate_standardised_numerator('iif_indicators_collab', 'amb_care_pats', standardised_numerator_id, report_period_month_start)
baseline_year_standardised_numerator_df = generate_standardised_numerator('iif_indicators_collab', 'baseline_amb_care_pats', baseline_standardised_numerator_indicator_id, baseline_month_start)

standardised_numerator_df = current_year_standardised_numerator_df.unionByName(baseline_year_standardised_numerator_df, allowMissingColumns=True)
create_table("acsc_cqrs_numerator", "iif_indicators_collab", standardised_numerator_df)

# COMMAND ----------

# DBTITLE 1,Create CQRS output
cqrs_denominator_df = prepare_cqrs_udal_data('acsc_cqrs_denominator', 'PCN_CODE', RPED, denominator_cqrs_dict, count_column = 'COUNT')
cqrs_numerator_df = prepare_cqrs_udal_data('acsc_cqrs_numerator', 'PCN_CODE', RPED, standardised_cqrs_dict, count_column = 'standardised_admissions')
acsc_indicator_landing_table_df = cqrs_denominator_df.union(cqrs_numerator_df)

# Compare landing table to archive, write to landing table if new 
archive_landing_table(acsc_indicator_landing_table_df,'acsc_indicator_cqrs_archive_table')

# Update cqrs indicator table
update_indicators_for_cqrs_or_udal_table(acsc_indicator_landing_table_df, 'cqrs')

# COMMAND ----------

display(spark.table('iif_indicators_collab.indicators_for_cqrs').filter(F.col('ORG_ID')=='U99355'))

# COMMAND ----------

# DBTITLE 1,Create publication output
pub_denominator_df = prepare_publication_data('acsc_cqrs_denominator', 'PCN', 'PCN_CODE', 'PCN_NAME', financial_year_yyyy, report_date, 'NCD022', denominator_pub_dict, count_column = 'COUNT')
pub_numerator_df = prepare_publication_data('acsc_cqrs_numerator', 'PCN', 'PCN_CODE', 'PCN_NAME', financial_year_yyyy, report_date, 'NCD022', standardised_pub_dict, count_column = 'standardised_admissions')
pub_non_standardised_numerator_df = prepare_publication_data('acsc_cqrs_numerator', 'PCN', 'PCN_CODE', 'PCN_NAME', financial_year_yyyy, report_date, 'NCD022', non_standardised_pub_dict, count_column = 'admissions')

pub_dfs_to_union = [
  pub_denominator_df, 
  pub_numerator_df, 
  pub_non_standardised_numerator_df
  ]

acsc_indicator_publication_landing_table_df = reduce(DataFrame.union, pub_dfs_to_union)

# Archive then update publication table
archive_landing_table(acsc_indicator_publication_landing_table_df,'acsc_indicator_pub_archive_table')
update_indicators_for_publication_table(acsc_indicator_publication_landing_table_df)

# COMMAND ----------

# DBTITLE 1,Create UDAL output
udal_denominator_df = prepare_cqrs_udal_data('acsc_cqrs_denominator', 'PCN_CODE', RPED, denominator_udal_dict, count_column = 'COUNT')
udal_numerator_df = prepare_cqrs_udal_data('acsc_cqrs_numerator', 'PCN_CODE', RPED, standardised_udal_dict, count_column = 'standardised_admissions')
udal_non_standardised_numerator_df = prepare_cqrs_udal_data('acsc_cqrs_numerator', 'PCN_CODE', RPED, non_standardised_udal_dict, count_column = 'admissions')

udal_dfs_to_union = [
  udal_denominator_df, 
  udal_numerator_df, 
  udal_non_standardised_numerator_df
  ]

acsc_indicator_udal_landing_table_df = reduce(DataFrame.union, udal_dfs_to_union)

# Archive then update publication table
archive_landing_table(acsc_indicator_udal_landing_table_df,'acsc_indicator_udal_archive_table')
update_indicators_for_cqrs_or_udal_table(acsc_indicator_udal_landing_table_df, 'udal')

# COMMAND ----------

