from pymongo import MongoClient
from dagster import op, In, Out, DagsterType
from dagster_pandas import PandasColumn, create_dagster_pandas_dataframe_type
from datetime import datetime
import pandas as pd
from pandas import DataFrame

client = MongoClient('localhost', 27017)
mydatabase = client.dap1

vehicle = pd.DataFrame(mydatabase.vehicle.find({}) )

print(vehicle.head())



(ins={'start': In(bool)}, out=Out(CustomersDataFrame) )
def extract_customers(start) -> CustomersDataFrame:
conn = MongoClient(mongo_connection_string)
db = conn["northwind" ]
customers = pd.DataFrame(db.customers.find({}) )
customers.drop(
columns=["address", "postalCode", "phone", "fax", "region" ],
axis=1,
inplace=True
)
customers.rename(
columns=customer_columns,
inplace=True
)
conn.close()
return customers