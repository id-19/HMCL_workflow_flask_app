"""
SAP Workflow Logic

This module provides functionality to fetch data from SAP and insert it into a database.

Run this code first, then replicate the functionality in an airflow DAG to run it periodically.
"""
import requests
# To get environment variables(TODO: implement this later for security)
import os
from dotenv import load_dotenv
load_dotenv()

# Get the data from the SAP rfc by running the curl command given
import requests
import json

# For database
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# For logging
import logging
logger = logging.getLogger(__name__)    
logger.setLevel(logging.INFO)

# TODO: Use .env to store url, headers, (and maybe data_payload), not safe this way
url = "https://mobileapps01.heromotocorp.com/cloudware_uat/GatewayAnalyserJson?ORG_ID=VDC"

headers = {
    "Authorization": "Basic c070d126-09d7-11ee-82f8-00505692957f", # Put this in env vars
    "Content-Type": "application/json"
}

# The data payload as a Python dictionary
# requests will automatically convert this to JSON and set the
# Content-Type header
# if you pass it to the 'json' parameter.
data_payload = {
    "PWSESSIONRS": {
        "PWPROCESSRS": {
            "PWDATA": {
                "RFCCALL": {
                    "app_name": "VendorData",
                    "export_table_data": {},
                    "import_table_name": "[IT_OUTPUT]",
                    "params": {
                        "CODE": "V"
                    },
                    "rfc_name": "ZFI_TURNOVER_VALI",
                    "server": "production_ECC",
                    "log_level": "4"
                }
            },
            "PWERROR": {},
            "PWHEADER": {
                "DEVICE_LONGITUDE": "",
                "LOGIN_ID": "100000",
                "DEVICE_MODEL": "",
                "IMEI_NO": "",
                "USER_ID": "",
                "VERSION_ID": "",
                "PW_SESSION_ID": "6131499513872613",
                "DEVICE_LATITUDE": "",
                "INSTALLATION_ID": "",
                "ORG_ID": "VDC",
                "APP_ID": "VDC",
                "PW_VERSION": "",
                "IS_AUTH": "Y",
                "IN_PROCESS_ID": "3",
                "USER_SESSION_ID": "9cd97d80-642d-11ea-abd8-00505692957f",
                "OS_VERSION": "",
                "SERVER_TIMESTAMP": "",
                "DEVICE_MAKE": "",
                "OUT_PROCESS_ID": "RFCCALL",
                "SIM_ID": "",
                "PW_CLIENT_VERSION": "3.2",
                "PASSWORD": "",
                "DEVICE_TIMESTAMP": "08-07-2017 17:07:52"
            }
        }
    }
}

def get_data_from_sap(url, headers, data_payload)->dict:
    try:
        # Make the POST request
        response = requests.post(url, headers=headers, json=data_payload)

        # Raise an HTTPError for bad responses (4xx or 5xx)
        response.raise_for_status()

        # Print the response content
        print("Status Code:", response.status_code)
        print("Response Headers:", response.headers)
        print("Response Body (JSON):")

        # Try to parse the response as JSON
        try:
            json_response = response.json()
            print(json.dumps(json_response, indent=2)) # Pretty print JSON
            return json_response
        except json.JSONDecodeError as e:
            print("Response is not valid JSON. Raw text response:")
            raise Exception("Failed to parse response as JSON:", e)

    except requests.exceptions.HTTPError as errh:
        print(f"Http Error: {errh}")
        raise errh
    except requests.exceptions.ConnectionError as errc:
        print(f"Error Connecting: {errc}")
        raise errc
    except requests.exceptions.Timeout as errt:
        print(f"Timeout Error: {errt}")
        raise errt
    except requests.exceptions.RequestException as err:
        print(f"Oops: Something Else {err}")
        raise err

# Okay, now we need to parse the data
def parse_sap_data(sap_data:dict)->list[dict]:
    """
    Parse the data returned from SAP
    sap_data: json body of the response from SAP, containing an array of records
    """
    process_data_obj = sap_data['PWSESSIONRS'][0]['PWPROCESSRS']['PWDATA']
    # process_headers = process_data_obj['PWHEADER']
    # process_errors = process_data_obj['PWERROR']
    rfc_call_data = process_data_obj['RFCCALL']
    final_data = rfc_call_data['IT_OUTPUT']
    # rfc_call_errors = rfc_call_data['PWERROR']
    return final_data


# QUERIES
bulk_upsert_query_postgres = """
INSERT INTO "user master" (vcode, name, pan, gst, email)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (vcode) DO UPDATE
SET name = EXCLUDED.name,
    pan = EXCLUDED.pan,
    gst = EXCLUDED.gst,
    email = EXCLUDED.email;
"""

bulk_upsert_query_mysql = """
    INSERT INTO `user master` (vcode, name, pan, gst, email)
    VALUES (%s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
    name = VALUES(name),
    pan = VALUES(pan),
    gst = VALUES(gst),
    email = VALUES(email);
"""


# Upsert the data to the database
def upsert_data_to_db(vendor_data:list[dict], bulk_upsert_threshold = 100, database_type = "postgres", session = None):
    # Initialize the database session
    def initialize_db_session(database_type = "postgres"):
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
        engine = create_engine(
            url=url
        )
        session = sessionmaker(bind=engine)
        return session()
    # Create session if not provided
    if session is None:
        session = initialize_db_session()
    try:
        if len(vendor_data) > bulk_upsert_threshold:
            if database_type == "postgres":
                session.execute(bulk_upsert_query_postgres, [(obj['vcode'], obj['name'], obj['pan'], obj['gst'], obj['email']) for obj in vendor_data])
            elif database_type == "mysql":
                session.execute(bulk_upsert_query_mysql, [(obj['vcode'], obj['name'], obj['pan'], obj['gst'], obj['email']) for obj in vendor_data]) 
        else:
            for obj in vendor_data:
                session.merge(obj)
        session.commit()
    except Exception as e:
        logger.error(f"Failed to upsert data: {e}")
        session.rollback()
        return False
    return True

def main():
    vendor_data = None
    # Get the data from SAP
    try:
        sap_data = get_data_from_sap(url, headers, data_payload)
        vendor_data = parse_sap_data(sap_data)
    except Exception as e:
        print(f"Error: {e}")
        raise e
    if vendor_data is None:
        raise Exception("Unable to get vendor data")
    
    # Upsert the data to the database
    try:
        upsert_data_to_db(vendor_data)
    except Exception as e:
        print(f"Error: {e}")
        raise e


exmaple_data = [
    {
        "vcode": "123000",
        "name": "John Doe",
        "pan": "AIHDS",
        "gst": "AIHDID",
        "email": "john.doe@example.com"
    },
    {
        "vcode": "124000",
        "name": "Jane Doe",
        "pan": "AYOSUDFH",
        "gst": "1234567890",
        "email": "jane.doe@example.com"
    }
]

# For testing 
if __name__ == "__main__":
    main()