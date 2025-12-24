import json
from kafka import KafkaConsumer
from pymongo import MongoClient

# -----------------------
# Kafka Consumers
# -----------------------
consumer_normal = KafkaConsumer(
    "normal",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

consumer_fire = KafkaConsumer(
    "fire",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

# -----------------------
# MongoDB
# -----------------------
client = MongoClient("mongodb://localhost:27017/")
db = client["iot_health"]

measurements_col = db["measurements"]   # données normales
alerts_col = db["alerts"]               # alertes critiques

print("📦 Storage consumer démarré")
print("⏳ En attente de données (normal + fire)...\n")

# -----------------------
# Boucle de stockage
# -----------------------
while True:

    # NORMAL
    for records in consumer_normal.poll(timeout_ms=1000).values():
        for record in records:
            data = record.value
            data["status"] = "normal"
            measurements_col.insert_one(data)
            print(f"✔ NORMAL stocké | Patient {data['patient_id']} | {data['metric']}")

    # FIRE
    for records in consumer_fire.poll(timeout_ms=1000).values():
        for record in records:
            data = record.value
            data["status"] = "fire"
            alerts_col.insert_one(data)
            print(f"🚨 ALERT stockée | Patient {data['patient_id']} | {data['metric']}")
