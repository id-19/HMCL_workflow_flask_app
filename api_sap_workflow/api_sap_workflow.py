# get json objects from api endpoint
# update or insert into postgres database

# For typing
from requests.models import Response


from typing import Dict, List, Any, Optional, Type

# For database
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# For requests
import json
import requests

# For env vars
import os
from dotenv import load_dotenv
load_dotenv()

# For airflow
from airflow.sdk import dag, task
from datetime import datetime, timedelta

# For logging
import logging
logger = logging.getLogger(__name__)    
logger.setLevel(logging.INFO)



"""
The final query will look like this:

INSERT INTO users (id, name, email, last_login)
VALUES 
    (123, 'John Doe', 'john@example.com', '2025-07-09 14:45:00'),
    (124, 'Jane Smith', 'jane@example.com', '2025-07-09 15:30:00'),
    (125, 'Bob Johnson', 'bob@example.com', '2025-07-09 16:15:00')
ON CONFLICT (id)
DO UPDATE SET name = EXCLUDED.name, email = EXCLUDED.email, last_login = EXCLUDED.last_login;

"""

from table_schema import Order as schema, OrderModel as validation_schema, bulk_upsert_query_postgres, bulk_upsert_query_mysql

@dag(
    dag_id="api_sap_workflow",
    schedule=timedelta(days=7), # run every week
    catchup=False, # if you miss a day, it's fine
    start_date=datetime(2025,6,20)
)
def api_sap_workflow():

    @task(
        task_id="fetch_order_data_from_sap",
        retries=3,
        retry_delay=timedelta(minutes=5)
    )
    def fetch_order_data_from_sap():
        # TODO: Fetch data from SAP API
        SAP_URL = os.getenv("SAP_URL")
        SAP_AUTH_FILE = os.getenv("SAP_AUTH_FILE")
        assert SAP_URL is not None, "SAP_URL is not set"
        assert SAP_AUTH_FILE is not None, "SAP_AUTH_FILE is not set"
        try:
            auth_file_content = open(SAP_AUTH_FILE, "r").read()
        except FileNotFoundError:
            logger.error("SAP_AUTH_FILE not found") 
            return None
        response: Response = requests.get(SAP_URL, auth=(SAP_AUTH_FILE, auth_file_content))
        assert response.status_code == 200, "Failed to fetch data from SAP"
        # Extract the data from the response
        data = response.content.decode("utf-8")
        return data # leave conversion to next task
        
    @task(
        task_id="initialize_db_session",
        retries=3,
        retry_delay=timedelta(minutes=5)
    )
    def initialize_db_session(database_type = "postgres"):
        # There's no auth on local(for now)
        # token = os.getenv("AIRFLOW_ACCESS_TOKEN") # Auth can be customized
        decoder = json.JSONDecoder()

        # Postgres init
        host = os.getenv("DB_HOST", "localhost")
        database = os.getenv("DB_DATABASE", "postgres")
        user = os.getenv("DB_USER", "postgres")
        password = os.getenv("DB_PASSWORD", "postgres")

        # db init
        if database_type == "postgres":
            url = f"postgresql+psycopg2://{user}:{password}@{host}/{database}"
        elif database_type == "mysql":
            url = f"mysql+pymysql://{user}:{password}@{host}/{database}"
        database_type = database_type
        engine = create_engine(
            url=url
        )
        session = sessionmaker(bind=engine)
        return session 
            

    @task(
        task_id="convert_json_to_data",
        retries=3,
        retry_delay=timedelta(minutes=5)
    )
    def convert_json_to_data(json_string: str| List[Dict[str, Any]])->List[schema]:
        res = []
        if isinstance(json_string, str):
            try:
                objs = json.decoder.JSONDecoder().decode(json_string)
            except json.JSONDecodeError as e:
                logger.error(f"Failed to decode JSON string: {e}")
                return res
            for obj in objs:
                try:
                    validated_data = validation_schema(**obj)
                    if validated_data:
                        res.append(schema(**validated_data.model_dump())) # model_dump() is the accepted method in Pydantic V2
                except Exception as e:
                    logger.error(f"Failed to validate data: {e}")
                    continue
        else:   
            for obj in json_string:
                try:
                    validated_data = validation_schema(**obj)
                    if validated_data: 
                        res.append(schema(**validated_data.model_dump()))
                except Exception as e:
                    logger.error(f"Failed to validate data: {e}")
                    continue
        return res


    @task(
        task_id="upsert_data",
        retries=3,
        retry_delay=timedelta(minutes=5)
    )
    def upsert_data(data: List[schema], session, database_type: str = "postgres"):
        # To insert more than 100 items, use bulk insert
        # TODO: Alter the query execution to insert other fields as well
        if not data:
            logger.warning("No data to upsert")
            return True 
        try:
            if len(data) > 100:
                if database_type == "postgres":
                    session.execute(bulk_upsert_query_postgres, [obj.vendor for obj in data])
                elif database_type == "mysql":
                    session.execute(bulk_upsert_query_mysql, [obj.vendor for obj in data]) 
            else:
                for obj in data:
                    session.merge(obj)
            # session.close()
        except Exception as e:
            logger.error(f"Failed to upsert data: {e}")
            return False
        return True
    
    @task(
        task_id="close_session",
        retries=3,
        retry_delay=timedelta(minutes=5)
    )
    def close_session(session):
        session.commit()
        session.close()

    @task(
        task_id="notify",
        retries=3,
        retry_delay=timedelta(minutes=5)
    )
    def notify(success: bool):
        # TODO: Add proper notification flow(probably send a mail or register an event with some Azure service)
        if success:
            logger.info("Data upserted successfully")
        else:
            logger.error("Failed to upsert data")
        
    session = initialize_db_session()
    json_string = fetch_order_data_from_sap()
    data = convert_json_to_data(json_string) # type: ignore
    success = upsert_data(data, session) # type: ignore
    notify(success) # type: ignore
    close_session(session)
    

api_sap_workflow()
