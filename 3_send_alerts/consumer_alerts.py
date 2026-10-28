from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField
import os
import sys
import time

from dotenv import load_dotenv
load_dotenv()
#from send_alert import send_alert_to_driver  

if __name__ == '__main__':  

    # Kafka consumer config
    config = {
        # User-specific properties that you must set
        'bootstrap.servers': os.getenv('BOOTSTRAP_SERVERS'),
        'sasl.username': os.getenv('API_KEY'),
        'sasl.password': os.getenv('API_SECRET'),

        # Fixed properties
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',

        # Consumer-specific properties
        'group.id': 'alerts-consumer-group-1',
        'auto.offset.reset': 'earliest'
    }

    # Schema Registry config
    sr_conf = {
        'url': os.getenv('SCHEMA_REGISTRY_URL'),
        'basic.auth.user.info': f"{os.getenv('SCHEMA_REGISTRY_API_KEY')}:{os.getenv('SCHEMA_REGISTRY_API_SECRET')}"
    }

    # Create Schema Registry client and Avro deserializer
    schema_registry_client = SchemaRegistryClient(sr_conf)
    avro_deserializer = AvroDeserializer(schema_registry_client)

    # Create Consumer instance
    consumer = Consumer(config)

    # Subscribe to topic
    topic = "vehicle.alerts.notifications"
    consumer.subscribe([topic])

    print(f"Subscribed to topic: {topic}")
    print("Waiting for messages... (Press Ctrl+C to exit)\n")
    print("=" * 60)

    try:
        while True:
            # Poll for messages
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                # No message received within timeout
                continue

            if msg.error():
                print("ERROR: %s".format(msg.error()))
            else:
                try:
                    # Deserialize Avro message
                    alert = avro_deserializer(msg.value(), SerializationContext(topic, MessageField.VALUE))
                except Exception as e:
                    print(f"Skipping message at offset {msg.offset()} - unable to decode: {e}")
                    continue

                print(f"Received message from topic: {msg.topic()}")
                print(f"  Partition: {msg.partition()}, Offset: {msg.offset()}")
                print(f"  Alert: {alert}")
                send_alert_to_driver(alert)
                time.sleep(1)
                print("-" * 60)

    except KeyboardInterrupt:
        print("\n\nShutting down consumer...")
    finally:
        # Close the consumer to commit final offsets
        consumer.close()
        print("Consumer closed successfully.")
