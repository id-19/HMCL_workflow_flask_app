import os
from sqlalchemy import create_engine, Column, String, Integer, MetaData, Table
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

load_dotenv()

table_name = os.getenv("TEST_TABLE_NAME", "user master")

def setup_test_database():
    """Set up the test database and return a session factory."""
    # Use test-specific environment variables
    db_type = os.getenv("TEST_DB_TYPE", "postgres")
    user = os.getenv('TEST_DB_USER', 'postgres')
    password = os.getenv('TEST_DB_PASSWORD', 'postgres')
    host = os.getenv('TEST_DB_HOST', 'localhost')
    database = os.getenv('TEST_DB_NAME', 'test_db')
    if db_type == "postgres":
        db_url = f"postgresql+psycopg2://{user}:{password}@{host}/{database}"
    elif db_type == "mysql":
        db_url = f"mysql+pymysql://{user}:{password}@{host}/{database}"
    else:  # sqlite (default for testing)
        db_url = "sqlite:///:memory:"
    
    engine = create_engine(db_url)
    
    # Create test table
    metadata = MetaData()
    Table(
        table_name,
        metadata,
        Column('vcode', String(50), primary_key=True),
        Column('name', String(100)),
        Column('pan', String(20)),
        Column('gst', String(20)),
        Column('email', String(100)),
    )
    metadata.create_all(engine, checkfirst=True)
    
    # Create session
    Session = sessionmaker(bind=engine)
    session = Session()
    
    return engine, session

# def setup_test_database():
#     """Set up the test database and return a session factory."""
#     # Use test-specific environment variables
#     db_type = os.getenv("TEST_DB_TYPE", "postgres")
#     user = os.getenv('TEST_DB_USER', 'postgres')
#     password = os.getenv('TEST_DB_PASSWORD', 'postgres')
#     host = os.getenv('TEST_DB_HOST', 'localhost')
#     database = os.getenv('TEST_DB_NAME', 'test_db')
#     if db_type == "postgres":
#         db_url = f"postgresql+psycopg2://{user}:{password}@{host}/{database}"
#     elif db_type == "mysql":
#         db_url = f"mysql+pymysql://{user}:{password}@{host}/{database}"
#     else:  # sqlite (default for testing)
#         db_url = "sqlite:///:memory:"
    
#     engine = create_engine(db_url)
    
#     # Create test table
#     metadata = MetaData()
#     Table(
#         table_name,
#         metadata,
#         Column('vcode', String(50), primary_key=True),
#         Column('name', String(100)),
#         Column('pan', String(20)),
#         Column('gst', String(20)),
#         Column('email', String(100)),
#     )
#     metadata.create_all(engine, checkfirst=True)
    
#     # Create session
#     Session = sessionmaker(bind=engine)
#     session = Session()
    
#     return engine, session