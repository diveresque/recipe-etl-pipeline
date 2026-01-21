from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

# Load .env file
load_dotenv()

# Get connection string
db_url = os.getenv("DATABASE_URL")
print("Using DATABASE_URL:", db_url)

# Create SQLAlchemy engine
engine = create_engine(db_url)

# Try connecting and running a simple query
try:
    with engine.connect() as conn:
        result = conn.execute(text("SELECT NOW();"))
        print("✅ Connection successful! Server time:", list(result)[0][0])
except Exception as e:
    print("❌ Connection failed:")
    print(e)
