# Databricks notebook source
import pyspark.sql.functions as F

# COMMAND ----------

# DBTITLE 1,ACSC logic
acsc_logic = {
  
  'no_exclusions_group':{
    
    'additional_criteria': True,
    'inclusion_criteria': eval("(F.col('DIAG_3_01').isin(no_exclusion_primary_diagnosis)) | (F.col('DIAG_4_01').isin(no_exclusion_primary_diagnosis))"),
    'and_or': eval('operator.or_'),
    'exclusion_criteria': eval('F.arrays_overlap(F.col("DIAG_4_CONCAT"), convert_list_to_array(diabetes_diag_04))') 
    
  },  
  
  'cellulitis': {
    'additional_criteria': True, 
    'inclusion_criteria': eval("(F.col('DIAG_3_01').isin(cell_diagnosis_code)) | (F.col('DIAG_4_01').isin(cell_diagnosis_code))"),
    'and_or': eval('operator.and_'),
    'exclusion_criteria': eval("""((~F.col('OPERTN_3_01').substr(1,1).isin(cellu_excl_opcs_01)) &
                                (~F.col('OPERTN_3_01').substr(1,2).isin(cellu_excl_opcs_02)) &
                                (~F.col('OPERTN_3_01').isin(cellu_excl_opcs_03)) 
                               )"""),
  },
  
  'congestive_heart_failure': {
    'additional_criteria': True, 
    'inclusion_criteria': eval("(F.col('DIAG_3_01').isin(chf_primary_diagnosis)) | (F.col('DIAG_4_01').isin(chf_primary_diagnosis))"),
    'and_or': eval('operator.and_'),
    'exclusion_criteria': eval("(~F.col('OPERTN_3_01').isin(chf_excl_opcs_03))")
    
  },
  
  'copd':{
    'additional_criteria': True, 
    'inclusion_criteria': eval("F.col('DIAG_3_01').isin(copd_diag_03)"),
    'and_or': eval('operator.or_'),
    'exclusion_criteria': eval("((F.col('DIAG_3_01').isin(copd_secondary_diag_03)) & (F.arrays_overlap(F.col('DIAG_3_CONCAT'), convert_list_to_array(copd_diag_03))))")
    
  },
  
  'hypertension': {
    'additional_criteria': True, 
    'inclusion_criteria': eval('(F.col("DIAG_3_01").isin(hyperten_primary_diag)) | (F.col("DIAG_4_01").isin(hyperten_primary_diag))'),
    'and_or': eval('operator.and_'),
    'exclusion_criteria': eval('~F.col("OPERTN_3_01").isin(hyperten_excl_opcs_03)'),
    
  },
  
  'influenza_pneumonia': {
    'additional_criteria': True, 
    'inclusion_criteria': eval("((F.col('DIAG_3_01').isin(ip_primary_diagnosis)) | (F.col('DIAG_4_01').isin(ip_primary_diagnosis)))"),
    'and_or': eval('operator.and_'),
    'exclusion_criteria': eval("~(F.arrays_overlap(F.col('DIAG_3_CONCAT'), convert_list_to_array(ip_secondary_diagnosis)))"),
    
  },
  
}

# COMMAND ----------

# DBTITLE 1,Non-standardised dict
standardised_cqrs_dict = {
  'D3C64D53-20CF-43E7-B15B-1AA8613F7B74':'D3C64D53-20CF-43E7-B15B-1AA8613F7B74',
  '0871C126-B4C3-4054-A0C0-2481525A940F':'0871C126-B4C3-4054-A0C0-2481525A940F'
}

denominator_cqrs_dict = {
  'D388FDB3-E3D7-44F2-80C7-0832B448C9B4':'D388FDB3-E3D7-44F2-80C7-0832B448C9B4',
  '88337490-8A59-4C40-8C13-FF0D9D01F48D':'88337490-8A59-4C40-8C13-FF0D9D01F48D'
}

non_standardised_pub_dict = {
  'D3C64D53-20CF-43E7-B15B-1AA8613F7B74':'Numerator - Non-standardised',
  '0871C126-B4C3-4054-A0C0-2481525A940F':'Base Year Numerator - Non-standardised'
}

standardised_pub_dict = {
  'D3C64D53-20CF-43E7-B15B-1AA8613F7B74':'Numerator',
  '0871C126-B4C3-4054-A0C0-2481525A940F':'Base Year Numerator'
}

denominator_pub_dict = {
  'D388FDB3-E3D7-44F2-80C7-0832B448C9B4':'Denominator',
  '88337490-8A59-4C40-8C13-FF0D9D01F48D':'Base Year Denominator'
}

non_standardised_udal_dict = {
  'D3C64D53-20CF-43E7-B15B-1AA8613F7B74':'d4909566-9bc8-11ed-a8fc-0242ac120002',
  '0871C126-B4C3-4054-A0C0-2481525A940F':'d4909836-9bc8-11ed-a8fc-0242ac120002'
}

standardised_udal_dict = {
  'D3C64D53-20CF-43E7-B15B-1AA8613F7B74':'D3C64D53-20CF-43E7-B15B-1AA8613F7B74',
  '0871C126-B4C3-4054-A0C0-2481525A940F':'0871C126-B4C3-4054-A0C0-2481525A940F'
}

denominator_udal_dict = {
  'D388FDB3-E3D7-44F2-80C7-0832B448C9B4':'D388FDB3-E3D7-44F2-80C7-0832B448C9B4',
  '88337490-8A59-4C40-8C13-FF0D9D01F48D':'88337490-8A59-4C40-8C13-FF0D9D01F48D'
}