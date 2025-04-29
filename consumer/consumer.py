from kafka import KafkaConsumer
import psycopg2
import json
import time
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("KafkaConsumer")

def create_consumer():
    return KafkaConsumer(
        'movie_ratings',
        bootstrap_servers='kafka:9092',
        auto_offset_reset='earliest',
        group_id='ratings_group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

def connect_postgres():
    while True:
        try:
            conn = psycopg2.connect(
                host="postgres",
                database="moviedb",
                user="user",
                password="password"
            )
            logger.info("Connected to PostgreSQL.")
            return conn
        except psycopg2.OperationalError:
            logger.warning("Waiting for PostgreSQL to be ready...")
            time.sleep(2)

def ensure_table_exists(cursor):
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS raw_ratings (
        user_id INT,
        movie_id INT,
        rating FLOAT,
        timestamp BIGINT
    )
    """)

def consume_and_store(consumer, cursor, conn):
    for msg in consumer:
        data = msg.value
        try:
            cursor.execute(
                "INSERT INTO raw_ratings (user_id, movie_id, rating, timestamp) VALUES (%s, %s, %s, %s)",
                (data['user_id'], data['movie_id'], data['rating'], data['timestamp'])
            )
            conn.commit()
            logger.info(f"Inserted: {data}")
        except Exception as e:
            logger.error(f"Failed to insert data: {data}, Error: {e}")
            conn.rollback()

if __name__ == "__main__":
    consumer = create_consumer()
    conn = connect_postgres()
    cur = conn.cursor()
    ensure_table_exists(cur)
    consume_and_store(consumer, cur, conn)
