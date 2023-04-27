# Databricks notebook source
# MAGIC %md
# MAGIC # IIF Indicators
# MAGIC ### Data requirements for both indicators:
# MAGIC - Appointment Status: IN ('Booked', 'Attended', 'Seen', 'DNA')
# MAGIC - Appointment Slot Type: *
# MAGIC - Healthcare Professional: *
# MAGIC - Mode of appointment: *
# MAGIC - Service setting: *
# MAGIC 
# MAGIC ## ACC-08
# MAGIC Percentage of Patients who had to wait 2 weeks or less for a GP Appointment. Record numerator (appointments within 2 weeks) and denominator (total appointments)
# MAGIC 
# MAGIC ### Data requirements
# MAGIC - GP appointment categories context type: Care related encounter
# MAGIC - Care related encounter slot types:
# MAGIC   - General Consultation Acute
# MAGIC   - General Consultation Routine
# MAGIC   - Unplanned Clinical Activity
# MAGIC   - Clinical Triage
# MAGIC   - Walk-in
# MAGIC   - Home Visit
# MAGIC   - Care Home Visit
# MAGIC   - Care Related Encounter but does not fit into any category
# MAGIC 
# MAGIC ## EHCH-04
# MAGIC Number of patient contacts as part of weekly care home round per care home resident aged 18+
# MAGIC 
# MAGIC Numerator only
# MAGIC 
# MAGIC ### Data requirements
# MAGIC - GP appointment categories context type: Care related encounter
# MAGIC - Care related counter slot type: Category 12: Patient contact during Care Home Round

# COMMAND ----------

# DBTITLE 1,Import dependencies
# MAGIC %run ./functions/iif_dependencies

# COMMAND ----------

# First run = True will create necessary ref tables used in this pipeline. This setup will only be necessary once.
first_run = False

if first_run == True:
  create_ref_tables()

# COMMAND ----------

#Manual date overrides so notebook can be run headless
dbutils.widgets.text("report_end", defaultValue="yyyy-mm-dd", label="2. Override end date")
dbutils.widgets.text("start_financial_year", defaultValue="yyyy-mm-dd", label="1. Override start date")

if dbutils.widgets.get("report_end") != "yyyy-mm-dd":
  value = dbutils.widgets.get("report_end")
  try:
    report_end = datetime.datetime.strptime(value, "%Y-%m-%d").date()
  except Exception as ex:
    print(ex)
    raise Exception("Reporting period override is not in iso format yyyy-mm-dd. Aborting")


date_obj = reportDate(str(report_end))
start_financial_year = date_obj.financial_year_start

assert report_end, "%Y-%m-%d" >= start_financial_year
print('Reporting for the period between {0} and {1}'.format(start_financial_year, report_end))

# COMMAND ----------

# DBTITLE 1,Select base data
basedata_df = get_base_data_df()

gp_practices = get_gp_practices()

# COMMAND ----------

# DBTITLE 1,ACC-08
national_slot_types = ['General Consultation Acute', 'General Consultation Routine', 'Unplanned Clinical Activity', 'Clinical Triage', 'Walk-in', 'Home Visit', 'Care Home Visit', 'Care Related Encounter but does not fit into any other category']
 
acc_08_base_df = basedata_df.filter(basedata_df['National_Slot_Type'].isin(national_slot_types))
acc_08_denominator_df = sum_and_prettify(acc_08_base_df, 'GP_Code', 'Appointments', acc_08_denom_label)
 
acc_08_nominator_df = acc_08_base_df.filter(acc_08_base_df['Wait_Time'].isin(['Same Day', '1 Day', '2 to 7 Days', '8 to 14 Days']))
acc_08_nominator_df = sum_and_prettify(acc_08_nominator_df, 'GP_Code', 'Appointments', acc_08_num_label)

# COMMAND ----------

# DBTITLE 1,ECHC-04
echc_04_base_df = basedata_df.filter(basedata_df['National_Slot_Type'] == 'Patient contact during Care Home Round')
echc_04_output_df = sum_and_prettify(echc_04_base_df, 'GP_Code', 'Appointments', echc_04_label)

# COMMAND ----------

# DBTITLE 1,Put indicators into one table
acc_08_output_df = acc_08_denominator_df.union(acc_08_nominator_df)
indicators_df = acc_08_output_df.union(echc_04_output_df)

display(indicators_df)

# COMMAND ----------

# DBTITLE 1,Join on mappings and PCN Mapping
# Remove practices not registered with a PCN
current_pcn_mapping_df = create_pcn_mapping(report_end)

reduced_df = create_reduced_df(current_pcn_mapping_df,indicators_df)

display(reduced_df)

# COMMAND ----------

# DBTITLE 1,Save to archive table
#archive function increments everytime it is run

export_df = (
           reduced_df
            .select(["ORG_ID", "ACHIEVEMENT_DATE", "FRIENDLY_IDENTIFIER", "DATA_REFERENCE_IDENTIFIER", "ATTRIBUTE_IDENTIFIER", "ATTRIBUTE_VALUE", "ATTRIBUTE_TYPE", "CREATED_AT"])
            .orderBy(F.col("ORG_ID"))
)
    
add_to_table(export_df, "iif_indicators_collab.rap_acc08_echc04_archive")

# COMMAND ----------

# DBTITLE 1,Save landing table
# only need the landing table to be saved once

export_df = export_df.drop("FRIENDLY_IDENTIFIER")
create_table('rap_acc08_echc04_landing', 'iif_indicators_collab', export_df)

# COMMAND ----------

# DBTITLE 1,EHCH-04 output tables
ehch04_df = spark.table('iif_indicators_collab.acc08_echc04_landing').filter(F.col('DATA_REFERENCE_IDENTIFIER')=='FB4334A8-2E9C-43AB-9D1F-51C4A4F14E6B')
update_indicators_for_cqrs_or_udal_table(ehch04_df, 'cqrs_ehch04')
update_indicators_for_cqrs_or_udal_table(ehch04_df, 'udal_ehch04')

# COMMAND ----------

# DBTITLE 1,ACC-08 output table
acc08_df = spark.table('iif_indicators_collab.acc08_echc04_landing').filter(F.col('DATA_REFERENCE_IDENTIFIER')=='283A5FDD-D4BA-4DF7-AC70-B59C83285D3E')
update_indicators_for_cqrs_or_udal_table(acc08_df, 'cqrs_acc08')
update_indicators_for_cqrs_or_udal_table(acc08_df, 'udal_acc08')

# COMMAND ----------

# MAGIC %run ./dq_checks/acc_0_dq_checks

# COMMAND ----------

# MAGIC %run ./dq_checks/ehch_04_dq_checks