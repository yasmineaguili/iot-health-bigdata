import json
from kafka import KafkaConsumer


TOPIC_NAME = "fire"
BOOTSTRAP_SERVERS = "localhost:9092"

consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

print("\n🚨 Alert Consumer démarré")
print("⏳ En attente d'événements critiques...\n")


for message in consumer:
    alert = message.value

    print(" ALERTE CRITIQUE DÉTECTÉE ")
    print(f"Patient ID  : {alert['patient_id']}")
    print(f"Device ID   : {alert['device_id']}")
    print(f"Métrique    : {alert['metric']}")
    print(f"Valeur      : {alert['value']} {alert['unit']}")
    print(f"Timestamp   : {alert['timestamp']}")
    print("-" * 60)
