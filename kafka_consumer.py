from kafka import KafkaConsumer
from kafka.errors import KafkaError
import time
import json

# Consumer configuration
consumer = KafkaConsumer('flight', bootstrap_servers='localhost:9092')

try:
    while True:
        # Fetch messages continuously
        for message in consumer:
            try:
                # Assuming messages are JSON, load and print
                message_value = json.loads(message.value)
                print("Received message:", message_value)
            except json.JSONDecodeError as json_error:
                print(f"Error decoding JSON: {json_error}")
                
except KafkaError as e:
    print(f"KafkaError: {e}")
    time.sleep(3)  # Wait for a few seconds before retrying

except KeyboardInterrupt:
    print("Consumer interrupted. Closing...")
    
finally:
    consumer.close()
