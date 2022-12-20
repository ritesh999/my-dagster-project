#final code to fetch the data
import pandas as pd
import numpy as np
import os
import sodapy
from sodapy import Socrata
import warnings
warnings.filterwarnings('ignore')



#url and app_token for my use case
url = "data.cityofchicago.org"
app_token = 'Wx9qsxCieySsXx1mHWVlyk0Qw'
#creating client object to interact with the url with the help of app token
soc_client = Socrata(url,app_token)
#data path
crash_dataset = "85ca-t3if"
vehicle_dataset = "68nd-jvt3"
people_dataset = "u6pd-qa9d"
#year wise data
years = ['2022','2021','2020']
#for loop to get data for each year
for year in years:
    q = f"""SELECT * WHERE CRASH_DATE >= '{year}-01-01' and CRASH_DATE
    <='{year}-12-31' LIMIT 100000"""
#get_data

data_crash = soc_client.get(crash_dataset, query = q)
print('crashes')

data_vehicle = soc_client.get(vehicle_dataset, query = q)
print('vehicle')

data_people = soc_client.get(people_dataset,query = q)
print('people')

#loading into dataframe
crash = pd.DataFrame.from_records(data_crash)

vehicle = pd.DataFrame.from_records(data_vehicle)
people = pd.DataFrame.from_records(data_people)


crash.to_json('./Data/crashes.json',orient='records')
vehicle.to_json('./Data/vehicle.json',orient='records')
people.to_json('./Data/people.json',orient='records')
