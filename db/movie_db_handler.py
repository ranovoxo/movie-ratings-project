import psycopg2, logging, os

os.makedirs('logs', exist_ok=True)

# Set up logging to append to the file
logging.basicConfig(
    filename='logs/database.log',
    filemode='a',  # explicitly open in append mode
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def insert_movie(movie, conn):

    try:
        with conn.cursor() as cur:
            insert_sql = """
                INSERT INTO movies_bronze (
                    id, title, original_title, overview, release_date,
                    popularity, vote_average, vote_count, adult,
                    original_language, genre_ids, poster_path,
                    backdrop_path, video
                ) VALUES (
                    %(id)s, %(title)s, %(original_title)s, %(overview)s, %(release_date)s,
                    %(popularity)s, %(vote_average)s, %(vote_count)s, %(adult)s,
                    %(original_language)s, %(genre_ids)s, %(poster_path)s,
                    %(backdrop_path)s, %(video)s
                )
                ON CONFLICT (id) DO NOTHING;
            """

            # Ensure data mapping aligns with expected structure
            movie_data = {
                "id": movie.get("id"),
                "title": movie.get("title"),
                "original_title": movie.get("original_title"),
                "overview": movie.get("overview"),
                "release_date": movie.get("release_date"),
                "popularity": movie.get("popularity"),
                "vote_average": movie.get("vote_average"),
                "vote_count": movie.get("vote_count"),
                "adult": movie.get("adult"),
                "original_language": movie.get("original_language"),
                "genre_ids": movie.get("genre_ids"),
                "poster_path": movie.get("poster_path"),
                "backdrop_path": movie.get("backdrop_path"),
                "video": movie.get("video")
            }

            cur.execute(insert_sql, movie_data)
            conn.commit()
            logging.info(f"Inserted movie: {movie_data['title']}")
    except Exception as e:
        logging.error(f"Error inserting movie {movie.get('title')}: {e}")
        conn.rollback()
