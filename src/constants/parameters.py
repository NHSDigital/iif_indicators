# Databricks notebook source
# DBTITLE 1,Ingestion
database = 'hes_ahas'
current_fy_table = 'hes_apc_2223'
previous_fy_table = 'hes_apc_2122'

# COMMAND ----------

# DBTITLE 1,AC-02 HES filters
# Constants used in initial filtering of HES data. 
invalid_DOBs = ['011900', '011901', '011800']
valid_age = ['0','120']
valid_sex = ['1','2']
valid_epistat = ['3']
valid_epitype = ['1']
valid_epiorder = ['1']
excluded_specialties = ['501','560','610']
valid_admission_methods = ['21', '23', '24', '28', '2A', '2B', '2C', '2D']

# COMMAND ----------

# DBTITLE 1,AC-02 conditions codes
# ACSC condition codes 

## Parameters for code acceptance- codes are grouped into 3 letter and 4 letter codes in order to query the specific column  
ASTHMA = ['J45', 'J46']
ENT_INFECTIONS = ['H66', 'H67', 'J02', 'J03', 'J06', 'J312']
PYELONEPHRITIS = ['N10', 'N11', 'N12', 'N136']
CONVULTIONS_AND_EPILEPSY = ['G40', 'G41', 'R56', 'O15']

no_exclusion_primary_diagnosis = ASTHMA + ENT_INFECTIONS + PYELONEPHRITIS + CONVULTIONS_AND_EPILEPSY

# Diabetes complications (E10.0-E10.8, E11.0-E11.8, E12.0-E12.8, E13.0-E13.8, E14.0-E14.8)- any diagnosis field - no exclusions 
diabetes_diag_04 = letter_number_generator([10,11,12,13,14],'E',9)

# Congestive heart failure (I110, I50, J81, Exclude: K0, K1, K2, K3, K4, K50, K52, K55, K56, K57, K60, K61, K66, K67, K68, K69, K71)
chf_primary_diagnosis = ['I50','J81', 'I110' ]
chf_excl_opcs_03 = ['K50','K52','K55','K56','K57','K60','K61','K66','K67','K68','K69','K71'] + letter_number_generator([0,1,2,3,4],'K',10)

# COPD (J20, J41, J42, J43, J44, J47, J20 if a secondary diagnosis ICD10 code: J41, J42, J43, J44, J47)
copd_diag_03 = ['J41','J42','J43','J44','J47']
copd_secondary_diag_03 = ['J20'] 

# Hypertension (I10, I119, Exclude: K0, K1, K2, K3, K4, K50, K52, K55, K56, K57, K60, K61, K66, K67, K68, K69, K71)
hyperten_primary_diag = ['I10', 'I119']
hyperten_excl_opcs_03 = chf_excl_opcs_03

# Influenza and Pneumonia ( J10, J11, J13, J14, J153, J154, J157, J159, J168, J181, J188, Exclude: D57 )
ip_primary_diagnosis = ['J10','J11','J13','J14', 'J153', 'J154', 'J157', 'J159', 'J168', 'J181', 'J188']
ip_secondary_diagnosis = ['D57']

# Cellulitis - 01 and 02 codes are accepted here where previosuly we created 03 codes for the sake of reducing computing requirements
# (L03, L04, L080, L088, L089, L88, L980, Exclude: A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S1, S2, S3, S41, S42, S43, S44, S45, S47, S48, S49, T, V, W, X0, X1, X2, X4, X5)  

cell_diagnosis_code = ['L03','L04','L88','L080','L088','L089','L980']
cellu_excl_opcs_01 = ['A','B','C','D','E','F','G','H','I','J','K','L','M','N','O','P','Q','R','T','V','W']
cellu_excl_opcs_02 = ['S1','S2','S3','X0','X1','X2','X4','X5']
cellu_excl_opcs_03 = ['S41','S42','S43','S44','S45','S47','S48','S49']



# COMMAND ----------

# DBTITLE 1,Indicator Codes
# These are used identify indicator when they're generated for CQRS

#baseline_denominator = '88337490-8A59-4C40-8C13-FF0D9D01F48D'
#current_denominator = 'D388FDB3-E3D7-44F2-80C7-0832B448C9B4'
#baseline_numerator = '0871C126-B4C3-4054-A0C0-2481525A940F'
#current_numerator = 'D3C64D53-20CF-43E7-B15B-1AA8613F7B74'

# COMMAND ----------

denominator_indicator_id = 'D388FDB3-E3D7-44F2-80C7-0832B448C9B4'
standardised_numerator_id = 'D3C64D53-20CF-43E7-B15B-1AA8613F7B74'
baseline_denominator_indicator_id = '88337490-8A59-4C40-8C13-FF0D9D01F48D'
baseline_standardised_numerator_indicator_id = '0871C126-B4C3-4054-A0C0-2481525A940F'
non_standardised_numerator_id = 'ACSC Emergency Admissions non-standardised numerator'