from kafka import KafkaProducer
import json, random, time, requests, os
from dotenv import load_dotenv
import logging


# Load .env variables
load_dotenv()
TMDB_API_KEY = os.getenv("TMDB_API_KEY")
FILE_NAME = "logs/kafka_producer.log"

os.makedirs("logs", exist_ok=True) # creating log directory if it doesn't exist

producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

logging.info(f"Kafka Producer created successfully!")

# Configure logging
logging.basicConfig(
    filename=FILE_NAME,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    filemode="a"  # append mode
)

def get_movies_by_year(year, page=1):
    url = "https://api.themoviedb.org/3/discover/movie"
    params = {
        "api_key": TMDB_API_KEY,
        "primary_release_year": year,
        "page": page
    }
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as e:
        status = response.status_code
        if status == 400:
            logging.warning(f"[{year}] Bad request (400).")
        elif status == 401:
            logging.error(f"[{year}] Unauthorized (401). Check API key.")
        elif status == 429:
            logging.warning(f"[{year}] Rate limit (429). Waiting 10s.")
            time.sleep(10)
        else:
            logging.error(f"[{year}] HTTP error {status}: {e}")
        return None
    except requests.exceptions.RequestException as e:
        logging.error(f"[{year}] Request exception: {e}")
        return None

def stream_movies(start_year, end_year):
    for year in range(start_year, end_year + 1):
        page = 1
        while True:
            data = get_movies_by_year(year, page)
            if not data or not data.get("results"):
                logging.warning(f"[{year}] No data on page {page}")
                break
            for movie in data["results"]:
                try:
                    producer.send("movies_by_decade", movie)
                    logging.info(f"[{year}] Sent: {movie['title']}")
                except Exception as e:
                    logging.error(f"[{year}] Failed to send: {movie['title']}, Error: {e}")
            if page >= data.get("total_pages", 1):
                break
            page += 1
            time.sleep(0.2)

if __name__ == "__main__":
    stream_movies(1950, 1951)
