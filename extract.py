from pymongo import MongoClient
from dagster import op, In, Out, DagsterType
from dagster_pandas import PandasColumn, create_dagster_pandas_dataframe_type
from datetime import datetime
import pandas as pd
from pandas import DataFrame

client = MongoClient('localhost', 27017)
mydatabase = client.dap1

#------------------------------crashes
CrashesDataFrame = create_dagster_pandas_dataframe_type(
    name="CrashesDataFrame",
    columns=[
        PandasColumn.string_column("crash_record_id"),
        PandasColumn.string_column("rd_no"),
        PandasColumn.string_column("crash_date"),
        PandasColumn.string_column("posted_speed_limit"),
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
        PandasColumn.string_column("injuries_total"),
        PandasColumn.string_column("injuries_fatal"),
        PandasColumn.string_column("injuries_incapacitating"),
        PandasColumn.string_column("injuries_non_incapacitating"),
        PandasColumn.string_column("injuries_reported_not_evident"),
        PandasColumn.string_column("injuries_no_indication"),
        PandasColumn.string_column("injuries_unknown")
    ]
)


#-------------vehicle
VehicleDataFrame = create_dagster_pandas_dataframe_type(
    name="VehicleDataFrame",
    columns=[
        PandasColumn.string_column("crash_unit_id"),
        PandasColumn.string_column("crash_record_id"),
        PandasColumn.string_column("rd_no"),
        PandasColumn.string_column("crash_date"),
        PandasColumn.string_column("unit_no"),
        PandasColumn.string_column("unit_type"),
        PandasColumn.string_column("vehicle_id"),
        PandasColumn.string_column("make"),
        PandasColumn.string_column("model"),
        PandasColumn.string_column("vehicle_year"),
        PandasColumn.string_column("vehicle_defect"),
        PandasColumn.string_column("vehicle_type"),
        PandasColumn.string_column("vehicle_use"),
        PandasColumn.string_column("travel_direction"),
        PandasColumn.string_column("maneuver")

    ]
)


#---------------------------people

PeopleDataFrame = create_dagster_pandas_dataframe_type(
    name="PeopleDataFrame",
    columns=[
        PandasColumn.string_column("person_id"),
        PandasColumn.string_column("person_type"),
        PandasColumn.string_column("crash_record_id"),
        PandasColumn.string_column("rd_no"),
        PandasColumn.string_column("vehicle_id"),
        PandasColumn.string_column("crash_date"),
        PandasColumn.string_column("city"),
        PandasColumn.string_column("state"),
        PandasColumn.string_column("zipcode"),
        PandasColumn.string_column("sex"),
        PandasColumn.string_column("age"),
        PandasColumn.string_column("drivers_license_state"),
        PandasColumn.string_column("drivers_license_class"),
        PandasColumn.string_column("safety_equipment"),
        PandasColumn.string_column("injury_classification"),
        PandasColumn.string_column("driver_action"),
        PandasColumn.string_column("driver_vision"),
        PandasColumn.string_column("physical_condition")

   ]
)


#--------------------------------------------------------------------------------------------------------------------

#--------------crashes

@op(ins={'start': In(bool)}, out=Out(CrashesDataFrame) )
def extract_crashes(start) -> CrashesDataFrame:

    crashes = pd.DataFrame(mydatabase.crashes.find({}) )
    crashes.drop(
    columns=['_id', 'intersection_related_i', 'damage','date_police_notified','report_type','prim_contributory_cause','sec_contributory_cause','street_no',
    'street_direction','street_name', 'beat_of_occurrence','num_units','most_severe_injury','crash_hour',
       'crash_day_of_week', 'crash_month', 'latitude', 'longitude', 'location',
       'hit_and_run_i', 'crash_date_est_i', 'private_property_i',
       'photos_taken_i', 'statements_taken_i', 'work_zone_i', 'dooring_i',
       'work_zone_type', 'workers_present_i', 'lane_cnt'],
    axis=1,
    inplace=True
    )
    client.close()
    return crashes

@op(ins={'crashes': In(CrashesDataFrame)}, out=Out(None) )
def stage_extracted_crashes(crashes):   
    crashes.to_csv("staging/crashes.csv", index=False, sep="\t") 



#--------------------Vehicle

@op(ins={'start': In(bool)}, out=Out(VehicleDataFrame) )
def extract_vehicle(start) -> VehicleDataFrame:

    vehicle = pd.DataFrame(mydatabase.vehicle.find({}) )
    vehicle.drop(
    columns=['_id','towed_i', 'occupant_cnt', 'towed_by', 'towed_to',
       'area_10_i', 'area_11_i', 'area_12_i', 'first_contact_point',
       'num_passengers', 'lic_plate_state', 'area_01_i', 'area_02_i',
       'area_99_i', 'area_04_i', 'area_05_i', 'area_03_i', 'area_08_i',
       'area_09_i', 'area_07_i', 'area_06_i', 'cmrc_veh_i', 'cmv_id',
       'usdot_no', 'commercial_src', 'gvwr', 'carrier_name', 'carrier_state',
       'carrier_city', 'hazmat_present_i', 'hazmat_report_i', 'mcs_report_i',
       'hazmat_vio_cause_crash_i', 'mcs_vio_cause_crash_i', 'trailer1_width',
       'vehicle_config', 'cargo_body_type', 'load_type',
       'hazmat_out_of_service_i', 'mcs_out_of_service_i', 'area_00_i',
       'fire_i', 'ccmc_no', 'total_vehicle_length', 'axle_cnt',
       'trailer1_length', 'hazmat_class', 'ilcc_no', 'hazmat_placards_i',
       'idot_permit_no', 'trailer2_width', 'un_no', 'exceed_speed_limit_i',
       'wide_load_i', 'hazmat_name', 'trailer2_length'],
    axis=1,
    inplace=True
    )
    client.close()
    return vehicle

@op(ins={'vehicle': In(VehicleDataFrame)}, out=Out(None) )
def stage_extracted_vehicle(vehicle):   
    vehicle.to_csv("staging/vehicle.csv", index=False, sep="\t") 


#--------------------People

@op(ins={'start': In(bool)}, out=Out(PeopleDataFrame) )
def extract_people(start) -> PeopleDataFrame:

    people = pd.DataFrame(mydatabase.people.find({}) )
    people.drop(
    columns=['_id','airbag_deployed', 'ejection', 'bac_result', 'seat_no',
       'hospital', 'ems_agency', 'pedpedal_action', 'pedpedal_visibility',
       'pedpedal_location', 'ems_run_no', 'bac_result_value'],
    axis=1,
    inplace=True
    )
    client.close()
    return people

@op(ins={'people': In(PeopleDataFrame)}, out=Out(None) )
def stage_extracted_people(people):   
    people.to_csv("staging/people.csv", index=False, sep="\t") 


