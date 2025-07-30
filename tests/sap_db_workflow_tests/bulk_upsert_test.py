"""
Script to test the bulk upsert functionality for SAP workflow data without pytest.
This test is for bulk upserts
"""
import os
import json
from dotenv import load_dotenv
from sqlalchemy import text

# Import the function to test
import sys
import os
# Correctly append the project root to the path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(project_root)
from dags.sap_workflow_dag.sap_workflow_logic import upsert_data_to_db
from generate_test_data import generate_vendor_data
# Load environment variables
load_dotenv()
from test_utils import setup_test_database

table_name = os.getenv("TEST_TABLE_NAME", "user master")

# # Load test data from file
# with open("bulk_upsert_data/original_data.json", "r") as f:
#     original_data_raw = json.load(f)
#     original_data = original_data_raw['data'] # List of records

# with open("bulk_upsert_data/updated_data.json", "r") as f:
#     updated_data_raw = json.load(f)
#     updated_data = updated_data_raw['data'] # List of records

# Generate initial insert data: 200 records starting from vcode 123000
initial_data = generate_vendor_data(num_records=200, starting_vcode=123000, start_user_num=1)

# Generate upsert data: 50 records that update some part of the initial 200 and add some new
# We'll upsert these 50 records:
# - First 30 records overlap with existing vcode from initial_data (e.g., 123010 to 123039) to update those
# - Next 20 records are new (vcode 123200 onwards)
upsert_data_update = generate_vendor_data(num_records=30, starting_vcode=123010, start_user_num=11)
upsert_data_new = generate_vendor_data(num_records=20, starting_vcode=123200, start_user_num=201)

upsert_data = upsert_data_update + upsert_data_new


# Setup the test database
engine, session = setup_test_database()

# Insert original data
upsert_data_to_db(initial_data, session=session, database_type=os.getenv("TEST_DB_TYPE", "postgres"))
# Verify that the data was inserted
for vendor in initial_data:
    result = session.execute(
        text(f'SELECT * FROM "{table_name}" WHERE vcode = :vcode'),
        {'vcode': vendor['vcode']}
    ).fetchone()
    assert result is not None, "Record not found after insert"
    assert result['name'] == vendor['name']
    assert result['pan'] == vendor['pan']
    assert result['gst'] == vendor['gst']
    assert result['email'] == vendor['email']


# Insert updated data
upsert_data_to_db(upsert_data, session=session, database_type=os.getenv("TEST_DB_TYPE", "postgres"))
# verify that the data has been updated/added
for vendor in upsert_data:
    result = session.execute(
        text(f'SELECT * FROM "{table_name}" WHERE vcode = :vcode'),
        {'vcode': vendor['vcode']}
    ).fetchone()
    assert result is not None, "Record not found after update"
    assert result['name'] == vendor['name']
    assert result['pan'] == vendor['pan']
    assert result['gst'] == vendor['gst']
    assert result['email'] == vendor['email']

# Cleanup
session.close()
engine.dispose()
print("Test completed successfully!!!!")