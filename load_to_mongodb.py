from pymongo import MongoClient
import json



client = MongoClient('localhost', 27017)

mydatabase = client["dap1"]

mycollection1 = mydatabase["crashes"]
mycollection2 = mydatabase["vehicle"]
mycollection3 = mydatabase["people"]
#-----------------------------------------------------------
with open('Data/crashes.json','r') as file:
    file_data1 = json.load(file)


mycollection1.insert_many(file_data1)

#------------------------------------------------------
with open('Data/vehicle.json','r') as file:
    file_data2 = json.load(file)


mycollection2.insert_many(file_data2)

#---------------------------------------------------------
with open('Data/people.json','r') as file:
    file_data3 = json.load(file)


mycollection3.insert_many(file_data3)