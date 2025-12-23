import json
import time
import pandas as pd
from kafka import KafkaProducer

# -----------------------
# Configuration
# -----------------------
CSV_FILE = "iot_measurements.csv"   # adapte au nom exact si besoin
BOOTSTRAP_SERVERS = "localhost:9092"

# -----------------------
# Kafka Producer
# -----------------------
producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# -----------------------
# Charger le dataset
# -----------------------
df = pd.read_csv(iot_measurements.csv)

# -----------------------
# Règles simples d'urgence
# -----------------------
def is_emergency(metric, value):
    try:
        value = float(value)
    except:
        return False

    if metric == "HeartRate" and (value < 40 or value > 130):
        return True
    if metric == "OxygenLevel" and value < 92:
        return True
    if metric in ["SystolicBP", "DiastolicBP"] and value > 140:
        return True
    if metric == "BodyTemperature" and (value < 35 or value > 39):
        return True

    return False

for _, row in df.iterrows():

    event = {
        "timestamp": row["Timestamp"],
        "patient_id": int(row["PatientID"]),
        "device_id": row["DeviceID"],
        "metric": row["Metric"],
        "value": row["Value"],
        "unit": row["Unit"]
    }

    topic = "fire" if is_emergency(event["metric"], event["value"]) else "normal"

    producer.send(topic, event)
    print(f"[{topic.upper()}] {event}")

    time.sleep(0.2)   # simulation temps réel

producer.flush()
print("Streaming terminé.")
