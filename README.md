# IIF AC-02

Code for IIF indicator AC-02: emergency admissions for specified Ambulatory Care Sensitive Conditions per registered patient

## Links to Publication Website & Metadata
TBC

## Monthly run quick guide
To begin, in the 'iif-indicators' agreemnent, navigate to Workspaces> iif_indicators_collab> AC-02 ACSC> main> main.py

In Cmd 16, enter the reporting period end date of the timeframe you wish to produce in the format 'yyyy-mm-dd', and then click 'Run All' at the top. This will run the pipeline, and update the neccessary tables with the outputs. A seperate DMS pipeline will then send the data to CQRS and UDAL as required. 


## Description
The Investment and Impact Fund (IIF) is an incentive scheme focussed on supporting PCNs to deliver high quality care to their population, and the delivery of the priority objectives articulated in the NHS Long Term Plan and in Investment and Evolution; a five-year GP contract framework.

The scheme contains indicators that focus on where PCNs can contribute significantly towards the ‘triple aim’:
-improving health and saving lives (e.g. through improvements in medicines safety).
-improving the quality of care for people with multiple morbidities (e.g. through increasing referrals to social prescribing services).
-helping to make the NHS more sustainable.

The AC-02 indicator produced in this pipeline refers to emergency admissions for specified Ambulatory Care Sensitive Conditions per registered patient. Data is produced as a cumulative count from the start of the finanical yaer to the reporting period end date specified by the user. The data is aggregated to and stored at PCN level.

This indicator is directly standardised at PCN level, based on sex (male/female) and age (5 year age bands: 0-4, 5-9….90-94, 95+). This allows comparison between years. The standard population used in this standardisation is the number of registered patients by age and gender for England (see more about this data source here: https://digital.nhs.uk/data-and-information/publications/statistical/patients-registered-at-a-gp-practice). To generate the standardised admission count, the calculated standardised rate is multiplied by the denominator, the number of registered patients at the PCN.

Numerator = Number of emergency admissions for specified Ambulatory Care Sensitive Conditions identified in HES dataset by PCN

Standardised numerator = PCN Directly Standardised Rate x PCN registered patient count 

Denominator = PCN registered patient count 

The pipeline will be run via the main.py notebook, which will use functions stored in the functions notebooks. These functions have been divided into approximate 'steps' of the pipeline, for ease of access and convinience.

```
AC-02 ACSC/src
│   main.py (master notebook) 
│ 	dq_checks.py (master notebook)   
│          
└───constants 
│   │   field_definitions.py
│   │   parameters.py
│  
└───functions  
│   │   01_create_denominator.py
│   │   02_create_numerator.py  
│   │   03_apply_standardisation.py
│   │   04_create_outputs.py
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
## Installation
This pipeline has been created in an NHS Digital DataBricks environment, and requires no setup when run on internal clusters. Imports are specified at the top of each notebook.  

## Support
If you have any questions about this repo, or suggestions on how we can improve it, please get in touch here: gpdata.enquiries@nhs.net

## License
The IIF AC-02 codebase is released under the MIT License.

The documentation is © Crown copyright and available under the terms of the Open Government 3.0 licence.
