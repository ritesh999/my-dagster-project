from dagster import op, Out, In
from dagster_pandas import PandasColumn, create_dagster_pandas_dataframe_type
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
import pandas as pd


TransformedCrashesDataFrame = create_dagster_pandas_dataframe_type(
    name="TransformedCrashesDataFrame",
    columns=[
        PandasColumn.string_column("crash_record_id"),
        PandasColumn.string_column("rd_no"),
        PandasColumn.datetime_column("crash_date"),
        PandasColumn.integer_column("posted_speed_limit"),
        PandasColumn.string_column("traffic_control_device"),
        PandasColumn.string_column("device_condition"),
        PandasColumn.string_column("weather_condition"),
        PandasColumn.string_column("lighting_condition"),
        PandasColumn.string_column("first_crash_type"),
        PandasColumn.string_column("trafficway_type"),
        PandasColumn.string_column("alignment"),
        PandasColumn.string_column("roadway_surface_cond"),
        PandasColumn.string_column("road_defect"),
        PandasColumn.string_column("crash_type"),
        PandasColumn.integer_column("injuries_total"),
        PandasColumn.integer_column("injuries_fatal"),
        PandasColumn.integer_column("injuries_incapacitating"),
        PandasColumn.integer_column("injuries_non_incapacitating"),
        PandasColumn.integer_column("injuries_reported_not_evident"),
        PandasColumn.integer_column("injuries_no_indication"),
        PandasColumn.integer_column("year"),
        PandasColumn.integer_column("month"),
        PandasColumn.integer_column("day")

    ]
)


#-------------vehicle
TransformedVehicleDataFrame = create_dagster_pandas_dataframe_type(
    name="TransformedVehicleDataFrame",
    columns=[
        PandasColumn.numeric_column("crash_unit_id"),
        PandasColumn.string_column("crash_record_id"),
        PandasColumn.string_column("rd_no"),
        PandasColumn.numeric_column("unit_no"),
        PandasColumn.string_column("unit_type"),
        PandasColumn.numeric_column("vehicle_id"),
        PandasColumn.string_column("make"),
        PandasColumn.string_column("model"),
        PandasColumn.string_column("vehicle_defect"),
        PandasColumn.string_column("vehicle_type"),
        PandasColumn.string_column("vehicle_use"),
        PandasColumn.string_column("travel_direction"),
        PandasColumn.string_column("maneuver")

    ]
)


#---------------------------people

TransformedPeopleDataFrame = create_dagster_pandas_dataframe_type(
    name="TransformedPeopleDataFrame",
    columns=[
        PandasColumn.string_column("person_id"),
        PandasColumn.string_column("person_type"),
        PandasColumn.string_column("crash_record_id"),
        PandasColumn.string_column("rd_no"),
        PandasColumn.numeric_column("vehicle_id"),
        PandasColumn.string_column("city"),
        PandasColumn.string_column("state"),
        PandasColumn.string_column("zipcode"),
        PandasColumn.string_column("sex"),
        PandasColumn.integer_column("age"),
        PandasColumn.string_column("drivers_license_state"),
        PandasColumn.string_column("drivers_license_class"),
        PandasColumn.string_column("safety_equipment"),
        PandasColumn.string_column("injury_classification"),
        PandasColumn.string_column("driver_action"),
        PandasColumn.string_column("driver_vision"),
        PandasColumn.string_column("physical_condition")

   ]
)


#-------------------------------------------------------------------------------------------------------------

@op(ins={ 'start' :In(None) }, out=Out(TransformedCrashesDataFrame))
def transform_extracted_crashes(start) -> TransformedCrashesDataFrame:

    crashes = pd.read_csv("staging/crashes.csv", sep="\t")
    crashes.drop(columns=['injuries_unknown'],axis=1,inplace=True)
    crashes.dropna(inplace=True)
    crashes['crash_date'] = pd.to_datetime(crashes['crash_date'])
    crashes['year'] = crashes['crash_date'].dt.year
    crashes['month'] = crashes['crash_date'].dt.month
    crashes['day'] = crashes['crash_date'].dt.day
    crashes['injuries_total'] = crashes['injuries_total'].astype(int)
    crashes['injuries_fatal'] = crashes['injuries_fatal'].astype(int)
    crashes['injuries_incapacitating'] = crashes['injuries_incapacitating'].astype(int)
    crashes['injuries_non_incapacitating'] = crashes['injuries_non_incapacitating'].astype(int)
    crashes['injuries_reported_not_evident'] = crashes['injuries_reported_not_evident'].astype(int)
    crashes['injuries_no_indication'] = crashes['injuries_no_indication'].astype(int)
    return crashes


@op(ins={'crashes': In(TransformedCrashesDataFrame)}, out=Out(None) )
def stage_transformed_crashes(crashes):
    crashes.to_csv(
    "staging/transformed_crashes.csv",
    sep="\t",
    index=False
    )



#----------------------------------- vehicle
@op(ins={ 'start' :In(None) }, out=Out(TransformedVehicleDataFrame))
def transform_extracted_vehicle(start) -> TransformedVehicleDataFrame:

    vehicle = pd.read_csv("staging/vehicle.csv", sep="\t")
    vehicle.drop(columns=['vehicle_year','crash_date'],axis=1,inplace=True)
    vehicle.dropna(
    inplace=True
    )
    vehicle['crash_unit_id'] = vehicle['crash_unit_id'].astype(int)
    vehicle['unit_no'] = vehicle['unit_no'].astype(int)
    vehicle['vehicle_id'] = vehicle['vehicle_id'].astype(int)
    return vehicle


@op(ins={'vehicle': In(TransformedVehicleDataFrame)}, out=Out(None) )
def stage_transformed_vehicle(vehicle):
    vehicle.to_csv(
    "staging/transformed_vehicle.csv",
    sep="\t",
    index=False
    )



#--------------------------------------------------------------------------people

@op(ins={'start' : In(None) }, out=Out(TransformedPeopleDataFrame) )
def transform_extracted_people(start) -> TransformedPeopleDataFrame:

    people = pd.read_csv("staging/people.csv", sep="\t")
    people.drop(columns=['crash_date'],axis=1,inplace=True)
    people.dropna(
    inplace=True
    )
    people['age'] = people['age'].astype(int)
    return people


@op(ins={'people': In(TransformedPeopleDataFrame)}, out=Out(None) )
def stage_transformed_people(people):
    people.to_csv("staging/transformed_people.csv",sep="\t",index=False)

    