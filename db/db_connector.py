import psycopg2
import logging
import os
from dotenv import load_dotenv

# Load .env variables
load_dotenv()

POSTGRES_PW = os.getenv("POSTGRES_PW")
DB_NAME = os.getenv("DB_NAME")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PW = os.getenv("POSTGRES_PW")

# Ensure the logs directory exists
os.makedirs('logs', exist_ok=True)

# Set up logging to append to the file
logging.basicConfig(
    filename='logs/database.log',
    filemode='a',  # explicitly open in append mode
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def get_pg_connection():
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=POSTGRES_USER,
            password=POSTGRES_PW,
            host="localhost",
            port="5432"
        )
        logging.info("Connection successful.")
        return conn
    except Exception as e:
        logging.error(f"Database connection failed: {e}")
        raise
