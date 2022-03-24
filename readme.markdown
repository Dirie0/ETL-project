# London Business and Cultural Venues ETL Project
This repository is for the manual extraction,transformation and loading into adb of londons business census and cultural venues infrastructure files.There is also a data pull script that sends automated email reports. In order to run these scripts, you must clone this onto your skywalkr home directory and create two folders(original_files and transformed_files) and edit the path directories in the scripts to match yours. The order of the scripts to run is as follows: coordinates_api extract_transform, data_load_to_ADB. Please be aware that the coordinates_api script takes ~ 1hour to run.  

Here's the links for the files to be transformed, download them into the original files folder:

https://data.cdrc.ac.uk/dataset/business-census/resource/business-census-snapshot-2021-v8

https://data.london.gov.uk/dataset/postcode-directory-for-london

https://data.london.gov.uk/download/cultural-infrastructure-map/debe429f-c76b-41d0-bf96-c859fbb8a8a4/site_by_borough.zip


## Coordinates_api
### Input
This script takes in business_census2021.csv
### Output
The output file is cleaned_postcodes_coordinates.csv 



## Extract_transform
### Input 
This script takes in the following:
1. cleaned_postcodes_coordinates.csv, 
2. business_census2021.csv,
3. all_sites.csv,
4. postcodes_directory.csv
### Output 
This scripts outputs the following:
1. cleaned_business_census.csv
2. cleaned_cultural_venue_data.csv
3. mapping_borough.csv



## Data_pull and data_load_to_ADB
### Input
#### There is no input order for these scripts and they only take singular inputs:
1. cleaned_business_census.csv
2. cleaned_cultural_venue_data.csv
3. mapping_borough.csv





