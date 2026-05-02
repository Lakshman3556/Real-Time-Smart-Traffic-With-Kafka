"""
consumer.py — Debug tool only.
Use this to verify Kafka messages are flowing correctly.
This is NOT needed to run the dashboard (app.py does the consuming).
"""
from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    "traffic",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="latest",
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

print("✅ Listening to Kafka topic: traffic")
print("   (Start producer.py in another terminal if nothing appears)")
print("-" * 60)

for message in consumer:
    d = message.value
    print(
        f"  {d.get('location','?'):<16} | "
        f"{d.get('traffic_status','?'):<8} | "
        f"{d.get('count',0):>3} vehicles | "
        f"{d.get('speed',0):>3} km/h"
    )