import json
import time
import pandas as pd
from kafka import KafkaProducer


CSV_FILE = "iot_measurements.csv"   
BOOTSTRAP_SERVERS = "localhost:9092"


producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)


df = pd.read_csv(CSV_FILE)

def is_emergency(metric, value):
    try:
        value = float(value)
    except:
        return False

    if metric == "HeartRate" and (value < 50 or value > 110):
        return True
    if metric == "OxygenLevel" and value < 94:
        return True
    if metric in ["SystolicBP", "DiastolicBP"] and value > 135:
        return True
    if metric == "BodyTemperature" and (value < 36 or value > 38):
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

    time.sleep(0.2)  

producer.flush()
print("Streaming terminé.")
