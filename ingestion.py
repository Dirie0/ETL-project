import string
import requests
import json
import numpy as np
import pandas as pd
import time
from configparser import ConfigParser
from Helpers import helper
import logging


logging.basicConfig(level=logging.INFO)



helper.download_csv_from_S3_Bucket('original-etl-files','all_sites.csv')
helper.download_csv_from_S3_Bucket('original-etl-files','business_census.csv')
helper.download_csv_from_S3_Bucket('original-etl-files','london_postcodes.csv')



#extract london postcodes
df = pd.read_csv('business_census.csv')
df.loc[df['posttown'] == 'LONDON']
df = df[['postcode']]
df = df[:1000]
df = df.drop_duplicates(subset='postcode', keep='first')
df=df.replace(' ','+', regex=True)



def postcode_converter(x):
    longitude,latitude= None,None
    new_url='https://api.getthedata.com/postcode/'+str(x)
    new_url=new_url.strip()
    r = requests.get(new_url)
    time.sleep(0.1) 
    r= r.text
    try:
        result= json.loads(r)
        longitude=result['data']['longitude']
        latitude=result['data']['latitude']
    except Exception:
        pass
    return (longitude,latitude)

df['longitude'],df['latitude']=zip(*df['postcode'].apply(postcode_converter))
print('postcode_converter finished')


#cleaning coordinates column by dropping empty rows and changing string format
df['longitude'].replace('',np.nan,inplace=True)
df['latitude'].replace('',np.nan,inplace=True)
df.dropna(subset=['longitude'], inplace =True)
df.dropna(subset=['latitude'], inplace=True)
df['postcode'] = df['postcode'].str.replace('+','')



helper.uploading_csv_to_S3_Bucket(df,'cleaned_postcodes_coordinates.csv','transformed-etl-files')

config = ConfigParser()
config.read('redshift_config.ini')



redshift_client = helper.Redshift(redshift_url=config['redshift']['redshift_url'], 
                            database_name=config['redshift']['database'],
                            redshift_username=config['redshift']['username'],
                            redshift_password=config['redshift']['password'])


redshift_client.load_df_to_table(dataframe=df,table_name='postcodes_coordinates')

print('uploaded cleaned_postcodes.csv to transformed-etl-files S3 Bucket and also redshift')

#filtering out on london postcode
df = pd.read_csv('business_census.csv')
df.loc[df['posttown'] == 'LONDON']


#dropping columns not of interest
columns=['dissolutiondate','country','county','countryoforigin','numgenpartners','numlimpartners', 'companystatus', 'nummortcharges', 'nummortoutstanding', 'nummortpartsatisfied', 'nummortsatisfied','siccode', 'sicdescription', 'return_nextduedate','return_lastmadeupdate', 'confirmation_nextduedate','accountingrefday','accountingrefday','accountingrefmonth','account_nextduedate','account_lastmadeupdate','careof','pobox','confirmation_lastmadeupdate','accountscategory'] 
df.drop(columns, axis=1, inplace=True)

#stripping whitespace
cols = df.select_dtypes(object).columns
df[cols] = df[cols].apply(lambda x: x.str.strip())

#cleaning postcode
df['postcode'].replace('',np.nan,inplace=True)
df.dropna(subset=['postcode'], inplace =True)
df['postcode'] = df['postcode'].str.replace(' ','')
df = df.drop_duplicates(subset='postcode', keep='first')

#cleaning company category
df['companycategory'].replace(to_replace=["PRI/LTD BY GUAR/NSC (Private, limited by guarantee, no share capital)","PRI/LBG/NSC (Private, Limited by guarantee, no share capital, use of 'Limited' exemption)","PRIV LTD SECT. 30 (Private limited company, section 30 of the Companies Act)"], value = "Private Limited Comapany",inplace=True)
df['companycategory'].replace(to_replace="Private Unlimited", value="Private Unlimited Company",inplace=True)
df['companycategory'].replace(to_replace='', value='', inplace=True)
df.dropna(subset=['companycategory'], inplace=True)

df["incorporationdate"] = pd.to_datetime(df["incorporationdate"])

#capitalising addresslines
df['addressline1'] = df['addressline1'].astype(str)
df['addressline2'] = df['addressline2'].astype(str)
df['addressline1'] = df[['addressline1']].applymap(string.capwords)
df['addressline2'] = df[['addressline2']].applymap(string.capwords)
print(len(df))

#merging coordinate columns with dataframe
helper.download_csv_from_S3_Bucket('transformed-etl-files','cleaned_postcodes_coordinates.csv')
coordinates = pd.read_csv('cleaned_postcodes_coordinates.csv')
new_df=pd.merge(df, coordinates, on='postcode', how='right')
print(len(new_df))

#extracting borough code and merging 
borough_code = pd.read_csv('london_postcodes.csv')
borough_code['pcd'] = borough_code['pcd'].str.replace(' ','')
borough_code = borough_code[['pcd','oslaua']]
borough_code = borough_code.rename(columns = {'oslaua':'borough_code'})
borough_code = borough_code.rename(columns = {'pcd':'postcode'})
final_df=pd.merge(new_df, borough_code, on='postcode', how='left')
print(len(final_df))

#changing order of columnms
final_df= final_df[['companynumber','incorporationdate','addressline1','addressline2','posttown','postcode','latitude','longitude','borough_code']]


helper.uploading_csv_to_S3_Bucket(final_df,'cleaned_business_census.csv','transformed-etl-files')
redshift_client.load_df_to_table(dataframe=final_df,table_name='cleaned_business_census')
print('uploaded cleaned_business_census.csv to transformed-etl-files s3 bucket and also redshift')


#cultural infrastructure 
all_sites= pd.read_csv('all_sites.csv')
all_sites = all_sites[:1000]
all_sites=all_sites[['Cultural Venue Type','borough_code','borough_name']]


#dropping empty rows
all_sites['borough_code'].replace('',np.nan,inplace=True)
all_sites['borough_name'].replace('',np.nan,inplace=True)
all_sites.dropna(subset=['borough_code'], inplace=True)
all_sites.dropna(subset=['borough_name'], inplace=True)

#removing whitespace
all_sites['borough_name']=all_sites['borough_name'].astype(str)
all_sites['borough_code']=all_sites['borough_code'].astype(str)
all_sites['Cultural Venue Type']=all_sites['Cultural Venue Type'].astype(str)
cols = all_sites.select_dtypes(object).columns
all_sites[cols] = all_sites[cols].apply(lambda x: x.str.strip())

#Capitalising borough names
all_sites['borough_name']=all_sites['borough_name'].astype(str)
all_sites['borough_name']=all_sites[['borough_name']].applymap(str.title)

#cleaning borough name values
all_sites.replace(to_replace =['City And County Of Th','City And County Of The Cit','City And County Of The City Of London'], value ='City Of London',inplace=True)
all_sites.replace(to_replace =['Hammersmith & Fulham','Hammersmith And Fulha'], value ='Hammersmith And Fulham',inplace=True)
all_sites.replace(to_replace =['Kensington & Chelsea','Kensington And Chelse'], value ='Kensington And Chelsea',inplace=True)
all_sites.replace(to_replace ='Westminster', value ='City Of Westminster',inplace=True)
all_sites.replace(to_replace='Barking', value='Barking And Dagenham',inplace=True)


helper.uploading_csv_to_S3_Bucket(all_sites,'cleaned_cultural_venue_infrastructure.csv','transformed-etl-files')
redshift_client.load_df_to_table(dataframe=all_sites,table_name='cleaned_cultural_venue_infrastructure')
print('uploaded cleaned_venue_infrastructure.csv to transformed-etl-files and also redshift')

#borough mapping
borough_df= pd.read_csv('all_sites.csv')
borough_df=borough_df[['borough_code','borough_name']]


#dropping duplicates
borough_df=borough_df.drop_duplicates(subset='borough_name', keep='last')
borough_df=borough_df.drop_duplicates(subset='borough_code', keep='last')

#stripping whitespace
cols = borough_df.select_dtypes(object).columns
borough_df[cols] = borough_df[cols].apply(lambda x: x.str.strip())

#capitalising borough name
borough_df['borough_name']=borough_df['borough_name'].astype(str)
borough_df['borough_name']=borough_df[['borough_name']].applymap(str.title)

#cleaning borough name strings
borough_df.replace(to_replace = ['City And County Of Th','City And County Of The Cit','City And County Of The City Of London'], value = 'City Of London',regex=True)
borough_df.replace(to_replace = ['Hammersmith & Fulham','Hammersmith And Fulha'], value = 'Hammersmith And Fulham',regex=True)
borough_df.replace(to_replace = ['Kensington & Chelsea','Kensington And Chelse'], value = 'Kensington And Chelsea',regex=True)
borough_df.replace(to_replace = 'Barking', value='Barking And Dagenham',regex=True)
borough_df.replace(to_replace = 'Westminster', value ='City Of Westminster',regex=True)

#dropping empty values
borough_df.replace(to_replace = '', value = np.nan)
borough_df.dropna(subset=['borough_code'], inplace=True)
borough_df.dropna(subset=['borough_name'], inplace=True)

helper.uploading_csv_to_S3_Bucket(borough_df,'borough_mapping.csv','transformed-etl-files')
redshift_client.load_df_to_table(dataframe=borough_df,table_name='borough mapping')
print('uploaded borough_mapping.csv to transformed-etl-files S3 Bucket ')
    
