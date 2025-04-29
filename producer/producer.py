from kafka import KafkaProducer
import json, random, time

producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def fake_rating():
    return {
        "user_id": random.randint(1, 100),
        "movie_id": random.randint(1, 100),
        "rating": round(random.uniform(1, 5), 1),
        "timestamp": int(time.time())
    }

while True:
    rating = fake_rating()
    producer.send('movie_ratings', rating)
    print("Sent:", rating)
    time.sleep(1)
