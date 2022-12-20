

from dagster import op, Out, In, get_dagster_logger
from sqlalchemy import create_engine, exc
from sqlalchemy.pool import NullPool
import pandas as pd
import psycopg2

engine = create_engine("postgresql://postgres:root\
@127.0.0.1:5432/dap1")
#conn = psycopg2.connect(database = "dap1", user = "postgres", password = "root", host = "127.0.0.1", port = "5432")
print("Opened database successfully")


#----------------------------crashes
@op(ins={'empty': In(None)}, out=Out(bool))
def load_crashes_dimension(empty):
    logger = get_dagster_logger()
    crashes = pd.read_csv("staging/transformed_crashes.csv", sep="\t")

    try:
        engine.execute("TRUNCATE crashes_dimension;")
        rowcount = crashes.to_sql(
        name="crashes_dimension",
        con=engine,
        index=False,
        if_exists="append",
        method="multi"
        )
        logger.info("%i crashes records loaded" % rowcount)
        engine.dispose(close=True)
        return rowcount > 0
    except exc.SQLAlchemyError as error:
        logger.error("Error: %s" % error)
        return False


#--------------------------vehicle
@op(ins={'empty': In(None)}, out=Out(bool))
def load_vehicle_dimension(empty):
    logger = get_dagster_logger()
    vehicle = pd.read_csv("staging/transformed_vehicle.csv", sep="\t")

    try:
        
        engine.execute("TRUNCATE vehicle_dimension;")
        rowcount = vehicle.to_sql(
        name="vehicle_dimension",
        con=engine,
        index=False,
        if_exists="append",
        method="multi"
        )
        logger.info("%i vehicle records loaded" % rowcount)
        engine.dispose(close=True)
        return rowcount > 0
    except exc.SQLAlchemyError as error:
        logger.error("Error: %s" % error)
        return False



#---------------------------------------people
@op(ins={'empty': In(None)}, out=Out(bool))
def load_people_dimension(empty):
    logger = get_dagster_logger()
    people = pd.read_csv("staging/transformed_people.csv", sep="\t")
    
    try:
        
        engine.execute("TRUNCATE people_dimension;")
        rowcount = people.to_sql(
        name="people_dimension",
        con=engine,
        index=False,
        if_exists="append",
        method="multi"
        )
        logger.info("%i people records loaded" % rowcount)
        engine.dispose(close=True)
        return rowcount > 0
    except exc.SQLAlchemyError as error:
        logger.error("Error: %s" % error)
        return False


#--------------------------------------------
