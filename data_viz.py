from dagster import op, Out, In, get_dagster_logger
from sqlalchemy import create_engine, exc
from sqlalchemy.pool import NullPool
import pandas as pd
import psycopg2
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px

engine = create_engine("postgresql://postgres:root\
@127.0.0.1:5432/dap1")
#conn = psycopg2.connect(database = "dap1", user = "postgres", password = "root", host = "127.0.0.1", port = "5432")
print("Opened database successfully")



#1. How many crashes occured when the maneuver of the VEHICLE was STRAIGHT AHEAD
crashesdf = pd.read_sql_query('select * from public.crashes_dimension',con=engine)
vehicledf = pd.read_sql_query('select * from public.vehicle_dimension',con=engine)
peopledf = pd.read_sql_query('select * from public.people_dimension',con=engine)
factdf = pd.read_sql_query('select * from public.crash_vehicle_people_fact',con=engine)

print(crashesdf.head())
print(vehicledf.head())
print(peopledf.head())
print(factdf.head())

print(crashesdf.isnull().sum())
print(vehicledf.isnull().sum())
print(peopledf.isnull().sum())
print(factdf.isnull().sum())





#Q.1
df1 = pd.read_sql_query('select maneuver,count(public.crash_vehicle_people_fact.crash_record_id) as Count from public.crash_vehicle_people_fact join public.vehicle_dimension on vehicle_dimension.crash_record_id = public.crash_vehicle_people_fact.crash_record_id group by maneuver;',con=engine)
print(df1)
#df1.loc[df1['count'] , 'maneuver'] # Represent only large countries
fig = px.pie(df1, values='count', names='maneuver', title='maneuver count')
fig.show()

#Q.2
df2 = pd.read_sql_query('select physical_condition,month,count(public.crash_vehicle_people_fact.injuries_total) as Count from public.crash_vehicle_people_fact join public.people_dimension on public.people_dimension.crash_record_id = public.crash_vehicle_people_fact.crash_record_id join crashes_dimension on public.crashes_dimension.crash_record_id = public.crash_vehicle_people_fact.crash_record_id group by physical_condition,month;',con=engine)
print(df2)

fig = px.bar(df2, x="month", y="count", color="physical_condition", title="")
fig.show()


#Q.3
df3 = pd.read_sql_query('select day,count(public.crash_vehicle_people_fact.injuries_total) as Count from public.crash_vehicle_people_fact join public.crashes_dimension on crashes_dimension.crash_record_id = public.crash_vehicle_people_fact.crash_record_id group by day;',con=engine)
print(df3)
fig = px.bar(df3, x='day', y='count',title="")
fig.show()


#Q.4
df4 = pd.read_sql_query('select day,crash_type,sum(public.crash_vehicle_people_fact.injuries_fatal) as injuries_fatal from public.crash_vehicle_people_fact join public.crashes_dimension on crashes_dimension.crash_record_id = public.crash_vehicle_people_fact.crash_record_id group by day,crash_type;',con=engine)
print(df4)
fig = px.line(df4, x='day', y='injuries_fatal', color='crash_type', title="")
fig.show()


#Q.5
df5 = pd.read_sql_query('select * from (select day,weather_condition,sum(public.crash_vehicle_people_fact.injuries_total) as injuries_total from public.crash_vehicle_people_fact join public.crashes_dimension on public.crashes_dimension.crash_record_id = public.crash_vehicle_people_fact.crash_record_id group by day,weather_condition) as a where injuries_total > 10;',con=engine)
print(df5)
fig = px.line(df5, x='day', y='injuries_total', color='weather_condition', title="")
fig.show()

#Q.6
df6 = pd.read_sql_query("select * from (select sex,age,sum(public.crash_vehicle_people_fact.injuries_total) as injuries_total from public.crash_vehicle_people_fact join public.people_dimension on public.people_dimension.crash_record_id = public.crash_vehicle_people_fact.crash_record_id group by age,sex) as a where sex != 'X' ",con=engine)
print(df6)
fig = px.line(df6, x='age', y='injuries_total', color='sex', title="")
fig.show()


#Q.7
df7 = pd.read_sql_query("select * from (select sex,zipcode,sum(public.crash_vehicle_people_fact.injuries_total) as injuries_total from public.crash_vehicle_people_fact join public.people_dimension on public.people_dimension.crash_record_id = public.crash_vehicle_people_fact.crash_record_id group by zipcode,sex) as a where sex !='X' and injuries_total > 10; ",con=engine)
print(df7)
fig = px.bar(df7, x='zipcode', y='injuries_total', color='sex', title="")
fig.show()











