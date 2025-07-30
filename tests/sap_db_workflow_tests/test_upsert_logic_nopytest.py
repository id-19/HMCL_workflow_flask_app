"""
Script to test the upsert functionality for SAP workflow data without pytest.
This test is for small scale merges
"""
import os
from sqlalchemy import create_engine, Column, String, Integer, MetaData, Table, text
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv


# Import the function to test
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dags.sap_workflow_dag.sap_workflow_logic import upsert_data_to_db
from generate_test_data import generate_vendor_data
from test_utils import setup_test_database
# Load environment variables
load_dotenv()

table_name = os.getenv("TEST_TABLE_NAME", "user master")

# Test data
TEST_VENDORS = [
    {
        "vcode": "123000",
        "name": "John Doe",
        "pan": "AIHDS1234A",
        "gst": "22AIHDS1234A1Z5",
        "email": "john.doe@example.com"
    },
    {
        "vcode": "124000",
        "name": "Jane Smith",
        "pan": "BSMPS1234B",
        "gst": "27BSMPS1234B1Z6",
        "email": "jane.smith@example.com"
    }
]


def test_upsert_operations():
    """Test both insert and update operations."""
    # Setup
    engine, session = setup_test_database()
    
    try:
        print("=== Testing Insert Operation ===")
        # Test inserting new records
        result = upsert_data_to_db(TEST_VENDORS, session=session, 
                                 database_type=os.getenv("TEST_DB_TYPE", "postgres"))
        print(f"Insert operation result: {'Success' if result else 'Failed'}")
        
        # Verify records were inserted
        for vendor in TEST_VENDORS:
            # Use quotes around table_name to handle spaces
            result = session.execute(
                text(f'SELECT * FROM "{table_name}" WHERE vcode = :vcode'),
                {'vcode': vendor['vcode']}
            ).fetchone()
            print(f"Inserted record - vcode: {vendor['vcode']}, name: {vendor['name']}")
            assert result is not None, "Record not found after insert"
            assert result['name'] == vendor['name']
        
        print("\n=== Testing Update Operation ===")
        # Test updating existing record
        updated_vendor = {
            "vcode": "123000",  # Same vcode as first test vendor
            "name": "John Updated",
            "pan": "UPDATED1234A",
            "gst": "UPDATED1234A1Z5",
            "email": "john.updated@example.com"
        }
        
        result = upsert_data_to_db([updated_vendor], session=session, 
                                 database_type=os.getenv("TEST_DB_TYPE", "postgres"))
        print(f"Update operation result: {'Success' if result else 'Failed'}")
        
        # Verify record was updated
        # Use quotes around table_name to handle spaces
        result = session.execute(
            text(f'SELECT * FROM "{table_name}" WHERE vcode = :vcode'),
            {'vcode': '123000'}
        ).fetchone()
        
        if result:
            print(f"Updated record - vcode: {result['vcode']}, new name: {result['name']}, new email: {result['email']}")
            assert result['name'] == "John Updated"
            assert result['email'] == "john.updated@example.com"
        else:
            print("Error: Record not found after update")
        
        print("\n=== Test Completed Successfully ===")
        
    except Exception as e:
        print(f"Error during test: {str(e)}")
        raise
    finally:
        # Cleanup
        session.close()
        engine.dispose()

if __name__ == "__main__":
    test_upsert_operations()