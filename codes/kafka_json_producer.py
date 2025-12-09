from kafka import KafkaProducer
import json

KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "covid"
JSON_FILE_PATH = r"C:\Users\roway\Desktop\BD_project\data_files\covid_data.json"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

with open(JSON_FILE_PATH, "r", encoding="utf-8") as file:
    for line in file:
        json_data = json.loads(line.strip())
        producer.send(TOPIC_NAME, value=json_data)
        print("Sent:", json_data)

producer.flush()
producer.close()
print("All JSON data sent to Kafka successfully!")
