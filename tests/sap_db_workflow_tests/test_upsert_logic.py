"""
Test the upsert functionality for SAP workflow data.
"""
import os
import pytest
from sqlalchemy import create_engine, Column, String, Integer, MetaData, Table
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from dotenv import load_dotenv

# Import the function to test
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dags.sap_workflow_dag.sap_workflow_logic import (
    upsert_data_to_db,
    bulk_upsert_query_postgres,
    bulk_upsert_query_mysql
)

# Load environment variables from .env.test if it exists
load_dotenv(dotenv_path='.env.test' if os.path.exists('.env.test') else ".env")

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

# Database setup
@pytest.fixture(scope="module")
def test_db_engine():
    """Create a test database and return a session factory."""
    # Use test-specific environment variables
    db_type = os.getenv("TEST_DB_TYPE", "postgres")
    
    if db_type == "postgres":
        db_url = f"postgresql+psycopg2://{os.getenv('TEST_DB_USER', 'postgres')}:{os.getenv('TEST_DB_PASSWORD', 'postgres')}@{os.getenv('TEST_DB_HOST', 'localhost')}/{os.getenv('TEST_DB_NAME', 'test_db')}"
    elif db_type == "mysql":
        db_url = f"mysql+pymysql://{os.getenv('TEST_DB_USER', 'root')}:{os.getenv('TEST_DB_PASSWORD', '')}@{os.getenv('TEST_DB_HOST', 'localhost')}/{os.getenv('TEST_DB_NAME', 'test_db')}"
    else:  # sqlite (default for testing)
        db_url = "sqlite:///:memory:"
    
    engine = create_engine(db_url)
    
    # Create test table
    metadata = MetaData()
    Table(
        'test_table',
        metadata,
        Column('vcode', String(50), primary_key=True),
        Column('name', String(100)),
        Column('pan', String(20)),
        Column('gst', String(20)),
        Column('email', String(100)),
    )
    metadata.create_all(engine)
    
    yield engine
    
    # Cleanup
    metadata.drop_all(engine)

@pytest.fixture
def db_session(test_db_engine):
    """Create a new database session for a test."""
    connection = test_db_engine.connect()
    transaction = connection.begin()
    Session = sessionmaker(bind=connection)
    
    yield Session()
    
    # Cleanup after test
    Session.close_all()
    transaction.rollback()
    connection.close()

def test_upsert_new_records(db_session):
    """Test inserting new records."""
    # Act
    result = upsert_data_to_db(TEST_VENDORS, session=db_session, database_type=os.getenv("TEST_DB_TYPE", "sqlite"))
    
    # Assert
    assert result is True
    
    # Verify records were inserted
    for vendor in TEST_VENDORS:
        result = db_session.execute(
            'SELECT * FROM "test_table" WHERE vcode = :vcode',
            {'vcode': vendor['vcode']}
        ).fetchone()
        assert result is not None
        assert result['name'] == vendor['name']
        assert result['email'] == vendor['email']

def test_upsert_update_records(db_session):
    """Test updating existing records."""
    # Arrange - insert initial data
    upsert_data_to_db(TEST_VENDORS, session=db_session, database_type=os.getenv("TEST_DB_TYPE", "postgres"))
    
    # Update a vendor
    updated_vendors = [{
        "vcode": "123000",  # Same vcode as first test vendor
        "name": "John Updated",
        "pan": "UPDATED1234A",
        "gst": "UPDATED1234A1Z5",
        "email": "john.updated@example.com"
    }]
    
    # Act
    result = upsert_data_to_db(updated_vendors, session=db_session, database_type=os.getenv("TEST_DB_TYPE", "postgres"))
    
    # Assert
    assert result is True
    
    # Verify record was updated
    result = db_session.execute(
        'SELECT * FROM "test_table" WHERE vcode = :vcode',
        {'vcode': '123000'}
    ).fetchone()
    
    assert result is not None
    assert result['name'] == "John Updated"
    assert result['email'] == "john.updated@example.com"

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
