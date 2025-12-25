import json
from kafka import KafkaConsumer
from pymongo import MongoClient



consumer_normal = KafkaConsumer(
    "normal",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="latest",
    enable_auto_commit=True,
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

consumer_fire = KafkaConsumer(
    "fire",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="latest",
    enable_auto_commit=True,
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)



client = MongoClient("mongodb://localhost:27017/")
db = client["iot_health"]

measurements_col = db["measurements"]  
alerts_col = db["alerts"]               

print("📦 Storage consumer démarré")
print("⏳ En attente de données (normal + fire)...\n")



while True:

    for records in consumer_normal.poll(timeout_ms=1000).values():
        for record in records:
            data = record.value
            data["status"] = "normal"

            try:
                measurements_col.insert_one(data)
                print(
                    f"✔ NORMAL stocké | "
                    f"Patient {data.get('patient_id')} | "
                    f"{data.get('metric')} | "
                    f"Value={data.get('value')}"
                )
            except Exception as e:
                print(" Erreur insertion Mongo (NORMAL):", e)

    for records in consumer_fire.poll(timeout_ms=1000).values():
        for record in records:
            data = record.value
            data["status"] = "fire"

            try:
                alerts_col.insert_one(data)
                print(
                    f"🚨 ALERT stockée | "
                    f"Patient {data.get('patient_id')} | "
                    f"{data.get('metric')} | "
                    f"Value={data.get('value')}"
                )
            except Exception as e:
                print(" Erreur insertion Mongo (ALERT):", e)
