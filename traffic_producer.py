from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime

# Connect to Kafka
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Hyderabad locations
locations = [
    "Madhapur", "Kondapur", "Hitech City", "Gachibowli",
    "Banjara Hills", "Jubilee Hills", "Begumpet",
    "Ameerpet", "Secunderabad", "Kukatpally",
    "Miyapur", "LB Nagar", "Dilsukhnagar",
    "Uppal", "Charminar"
]
def generate_data():
    location = random.choice(locations)

    hour = datetime.now().hour

    # Simulate realistic traffic based on time
    if 8 <= hour <= 11 or 17 <= hour <= 21:
        speed = random.randint(5, 30)   # rush hour
    else:
        speed = random.randint(30, 80)  # normal

    # 🔥 ADD THIS (core logic)
    if speed < 25:
        status = "HEAVY"
    elif speed < 50:
        status = "MODERATE"
    else:
        status = "SMOOTH"

    return {
        "vehicle_id": f"V{random.randint(100, 999)}",
        "location": location,
        "speed": speed,
        "traffic_status": status,   # ✅ IMPORTANT
        "count": random.randint(1, 20),  # ✅ IMPORTANT
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }

# Send data continuously
while True:
    data = generate_data()
    producer.send("traffic", value=data)
    print("Sent:", data)
    time.sleep(2)