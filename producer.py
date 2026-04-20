from kafka import KafkaProducer
import time

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: v.encode('utf-8')
)


print("Sending messages to Kafka...")

i = 0
while True:
    
    producer.send(topic, message)
    print(f"Sent: {message}")
    i += 1
    time.sleep(2)