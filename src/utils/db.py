# src/utils/db.py
from sqlalchemy import create_engine
from sqlalchemy.engine.url import make_url
import os
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("Please set DATABASE_URL in your .env file")

# Create SQLAlchemy engine. pool_pre_ping helps with dropped connections.
engine = create_engine(DATABASE_URL, pool_pre_ping=True)