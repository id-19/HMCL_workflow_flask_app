from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String

# For validation
from pydantic import BaseModel

Base = declarative_base()

class Order(Base):
    vendor = Column(String, primary_key=True)
    # Not sure of other fields rn

class OrderModel(BaseModel):
    vendor: str


bulk_upsert_query_postgres = """
    INSERT INTO orders (vendor)
    VALUES (%s)
    ON CONFLICT (vendor)
    DO UPDATE SET vendor = EXCLUDED.vendor;
"""

bulk_upsert_query_mysql = """
    INSERT INTO orders (vendor)
    VALUES (%s)
    ON DUPLICATE KEY UPDATE vendor = VALUES(vendor);
"""
    
    