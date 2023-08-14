# IIF AC-02

Code for the following IIF indicators: 
AC-02: Emergency admissions for specified Ambulatory Care Sensitive Conditions per registered patient.
ACC-08: Number of general practice appointments for which the time from booking to appointment was two weeks or less.
EHCH-04: Number of general practice appointments categorised as 'patient contact as part of weekly care home round'.

## Links to Publication Website & Metadata
Network Contract DES (MI) publication - https://digital.nhs.uk/data-and-information/publications/statistical/mi-network-contract-des

## Monthly run guide
If running for the first time in a new environment, alter the first_run variable to 'True'. This will run the 'create_ref_tables()' function, setting up any internal reference tables. Any lines of code that refer to archiving will fail but can be deleted. 

To begin, in the 'iif-indicators' agreement, navigate to Workspaces> iif_indicators_collab> main> x_main.py with x being the indicator(s) you wish to run.

In ac_02_main, enter the reporting period end date of the timeframe you wish to produce in the format 'yyyy-mm-dd', and then click 'Run All' at the top. This will run the pipeline, and update the neccessary tables with the outputs. A seperate DMS pipeline will then send the data to CQRS and UDAL as required. 

In acc_08_ehch_04_main, enter the reporting period end date of the timeframe you wish to produce in the format 'yyyy-mm-dd' into the 'Override end date' widget at the top, and then click 'Run All' at the top. This will run the pipeline, and update the neccessary tables with the outputs. A seperate DMS pipeline will then send the data to CQRS and UDAL as required. 

## Description
The Investment and Impact Fund (IIF) is an incentive scheme focussed on supporting PCNs to deliver high quality care to their population, and the delivery of the priority objectives articulated in the NHS Long Term Plan and in Investment and Evolution; a five-year GP contract framework.

The scheme contains indicators that focus on where PCNs can contribute significantly towards the ‘triple aim’:
-improving health and saving lives (e.g. through improvements in medicines safety).
-improving the quality of care for people with multiple morbidities (e.g. through increasing referrals to social prescribing services).
-helping to make the NHS more sustainable.

Each pipeline will be run via the x_main.py notebooks (replacing x with the indicator(s) you wish to run), which will use functions stored in the functions notebooks. These functions have been divided into approximate 'steps' of the pipeline, for ease of access and convinience.

### AC-02 ###
The AC-02 indicator produced in this pipeline refers to emergency admissions for specified Ambulatory Care Sensitive Conditions per registered patient. Data is produced as a cumulative count from the start of the finanical yaer to the reporting period end date specified by the user. The data is aggregated to and stored at PCN level. The data source for this indicator is HES - Admitted patient care (APC). [Hospital Episode Statistics TOS](https://digital.nhs.uk/data-and-information/data-tools-and-services/data-services/hospital-episode-statistics/hospital-episode-statistics-data-dictionary)

This indicator is directly standardised at PCN level, based on sex (male/female) and age (5 year age bands: 0-4, 5-9….90-94, 95+). This allows comparison between years. The standard population used in this standardisation is the number of registered patients by age and gender for England (see more about this data source here: https://digital.nhs.uk/data-and-information/publications/statistical/patients-registered-at-a-gp-practice). To generate the standardised admission count, the calculated standardised rate is multiplied by the denominator, the number of registered patients at the PCN.

This pipeline will produce 3 tables, each with different end points. One will go to CQRS, one to UDAL, and one to the publication website (TBC). Each output will contain the same numbers, however the UDAL output is the only to contain non-standardised indicator numbers. We therefore reccomend any external users of this code to use the UDAL table (indicators_for_udal_{indicator name}) for their analysis. 

Numerator = Number of emergency admissions for specified Ambulatory Care Sensitive Conditions identified in HES dataset by PCN

Standardised numerator = PCN Directly Standardised Rate x PCN registered patient count 

Denominator = PCN registered patient count 

### ACC-08 and EHCH-04 ###

The ACC-08 indicator produced in this pipeline refers to the number of general practice appointments for which the time from booking to appointment was two weeks or less. The EHCH-04 indicator produced in this pipeline refers to the number of general practice appointments categorised as 'patient contact as part of weekly care home round'. The data source for these indicators is the 'Appointments in general practice' publication source data. [Appointments in General Practice](https://digital.nhs.uk/data-and-information/publications/statistical/appointments-in-general-practice) 

For both indicators, data is produced as a cumulative count from the start of the finanical year to the reporting period end date specified by the user. The data is presented at GP practice level.

ACC-08 numerator = Of the denominator, the number for which the time from booking to appointment was two weeks or less

ACC-08 denominator = Number of appointments provided by the general practice that were mapped to one of the following eight national categories: General Consultation Acute, General Consultation Routine, Unplanned Clinical Activity, Walk in, Triage, Home Visit, Care Home Visit, Care Related Encounter but does not fit into any other category

EHCH-04 numerator = Number of general practice appointments categorised as 'patient contact as part of weekly care home round'. 

This pipeline will produce 4 tables, 2 per indicator, each with different end points. Per indicator, one will go to CQRS, and one to UDAL. Each output will contain the same numbers.

```
IIF_indicators/src
│   ac_02_main.py (AC-02 master notebook)
|   acc_08_ehch_04_main.py (ACC-08 & EHCH-04 master notebook) 	   
│          
└───constants 
│   │   field_definitions.py
│   │   parameters.py
│  
└───dq_checks
|   |   ac_02_dq_checks.py 
|   |   acc_08_dq_checks.py 
|   |   ehch_04_dq_checks.py 
|
└───functions  
│   │   00_ingest.py
│   │   01_create_denominator.py
│   │   02_create_numerator.py  
│   │   03_apply_standardisation.py
│   │   04_create_outputs.py
│   │   iif_dependencies.py
│   │   utils.py
│    
└───tests
│    └───lib
│    │  │	nutter
│    │	│	chispa 
│    │		
│    └───test_data
│    │	│	pcn_counts_jan_22.py
│    │	 
│    │  filtering_tests.py
│    │  init_tests.py
│    │  manual_dsr_tests.py
│    │  mapping_tests.py
│    │  run_tests.py
│    │  utils_test.py
```
The nuttr and chispa packages have been recreated in the tests folder as a workaround for loading the packages in DAE. 

### Data reference ID lookup table
The following is a lookup table for codes used in the outut tables of the code.

| Indicator | Indicator description | Data refernce ID | Attribute ID |
| ------------- | ------------- | ------------- | ------------- |
| AC-02 Numerator  | Standardised number of emergency admissions for specified Ambulatory Care Sensitive Conditions* for patients in the denominator  | E1B4261F-8649-42D0-A40E-208AA41F8EAE  | D3C64D53-20CF-43E7-B15B-1AA8613F7B74 |
| AC-02 Denominator  | Total number of registered patients  | E1B4261F-8649-42D0-A40E-208AA41F8EAE  | D388FDB3-E3D7-44F2-80C7-0832B448C9B4 |
| AC-02 Baseline Numerator  | Standardised number of emergency admissions for specified Ambulatory Care Sensitive Conditions* for patients in the denominator  | E1B4261F-8649-42D0-A40E-208AA41F8EAE  | 0871C126-B4C3-4054-A0C0-2481525A940F |
| AC-02 Baseline Denominator  | Total number of registered patients  | E1B4261F-8649-42D0-A40E-208AA41F8EAE  | 88337490-8A59-4C40-8C13-FF0D9D01F48D |
| AC-02b Numerator  | Non-Standardised number of emergency admissions for specified Ambulatory Care Sensitive Conditions* for patients in the denominator  | E1B4261F-8649-42D0-A40E-208AA41F8EAE  | d4909566-9bc8-11ed-a8fc-0242ac120002 |
| AC-02b Baseline Numerator  | Non-Standardised number of emergency admissions for specified Ambulatory Care Sensitive Conditions* for patients in the denominator.  | E1B4261F-8649-42D0-A40E-208AA41F8EAE  | d4909836-9bc8-11ed-a8fc-0242ac120002 |
| EHCH-04 Numerator  | Number of general practice appointments categorised as 'patient contact as part of weekly care home round'.  | FB4334A8-2E9C-43AB-9D1F-51C4A4F14E6B  | 982C9A99-A4EA-48FB-ABE9-B60B3E2F1719 |
| ACC-08 Numerator  | Of the denominator, the number for which the time from booking to appointment was two weeks or less  | 283A5FDD-D4BA-4DF7-AC70-B59C83285D3E  | 767390F9-FD7E-4B01-A8AE-82B41DE00A02 |
| ACC-08 Denominator  |  Number of appointments provided by the general practice that were mapped to one of the following eight national categories: General Consultation Acute, General Consultation Routine, Unplanned Clinical Activity, Walk in, Triage, Home Visit, Care Home Visit, Care Related Encounter but does not fit into any other category  | 283A5FDD-D4BA-4DF7-AC70-B59C83285D3E  | FB4D2185-044E-44A4-9C11-02A0A4034A02 |

## Installation
This pipeline has been created in an NHS Digital DataBricks environment, and requires no setup when run on internal clusters. Cluster runtime version: 10.4 LTS (includes Apache Spark  3.2.1, Scala 2.12).
Package imports are specified at the top of each notebook.  

## Support
If you have any questions about this repo, or suggestions on how we can improve it, please get in touch here: gpdata.enquiries@nhs.net

## License
The IIF_indicators codebase is released under the MIT License.

The documentation is © Crown copyright and available under the terms of the Open Government 3.0 licence.
