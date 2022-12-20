import webbrowser
from dagster import MetadataValue, Output, asset, repository
from dagster import job
from extract import *
from transform import *
from load import *

@op(out=Out(bool))
def load_crash_vehicle_people_fact(crashesdim,vehicledim,peopledim):
    logger = get_dagster_logger()
    engine = create_engine("postgresql://postgres:root\
    @127.0.0.1:5432/dap1")
    df = pd.read_sql_query('select * from crashes_dimension',con=engine)
    vehicle = pd.read_sql_query('select * from vehicle_dimension',con=engine)
    people = pd.read_sql_query('select * from people_dimension',con=engine)
    
    df.drop(columns=['rd_no', 'crash_date', 'posted_speed_limit',
        'traffic_control_device', 'device_condition', 'weather_condition',
        'lighting_condition', 'first_crash_type', 'trafficway_type',
        'alignment', 'roadway_surface_cond', 'road_defect', 'crash_type',
        'year', 'month', 'day'],axis=1,inplace=True)

    
    try:
        
        engine.execute("TRUNCATE crash_vehicle_people_fact;")
        rowcount = df.to_sql(
        name="crash_vehicle_people_fact",
        con=engine,
        index=False,
        if_exists="append",
        method="multi"
        )
        logger.info("%i crash_vehicle_people_fact records loaded" % rowcount)
        engine.dispose(close=True)
        return rowcount > 0
    except exc.SQLAlchemyError as error:
        logger.error("Error: %s" % error)
        return False


@job
def etl():
    load_crash_vehicle_people_fact(
        
        crashesdim=load_crashes_dimension(
            stage_transformed_crashes(
                transform_extracted_crashes(
                    stage_extracted_crashes(
                        extract_crashes()
                    )
                )
            )
        ),

        vehicledim=load_vehicle_dimension(
            stage_transformed_vehicle(
                transform_extracted_vehicle(
                    stage_extracted_vehicle(
                        extract_vehicle()
                    )
                )
            )
        ),

        peopledim=load_people_dimension(
            stage_transformed_people(
                transform_extracted_people(
                    stage_extracted_people(
                        extract_people()
                    )
                )
            )
        )
    )
