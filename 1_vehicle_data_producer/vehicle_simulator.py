from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField
import time
import random
from datetime import datetime, timezone
import os
import logging

from dotenv import load_dotenv
load_dotenv()


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Track metrics
delivery_success = 0
delivery_failed = 0

# --- 1. UTILITY TO LOAD SCHEMA ---
def load_schema(filepath):
    """Reads the avro schema from a local file"""
    with open(filepath, 'r') as f:
        # We return the raw string because JSONSerializer expects a string
        return f.read()

def generate_telemetry(vehicle_id):
    """Generate realistic vehicle telemetry data"""
    return {
        "vehicle_id": vehicle_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "speed_kmph": random.randint(30, 120),
        "fuel_percent": random.randint(10, 100),
        "temperature_c": random.randint(70, 95),
        "latitude": round(random.uniform(40.0, 41.0), 6),
        "longitude": round(random.uniform(-74.0, -73.0), 6)
    }

def delivery_callback(err, msg):
    """Called when message is delivered or fails"""
    global delivery_success, delivery_failed

    if err:
        delivery_failed += 1
        logger.error(f"Delivery failed for {msg.topic()}: {err}")
        print(f"❌ Delivery failed: {err}")
    else:
        delivery_success += 1

        print(f"✅ Message delivered to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}")


if __name__ == "__main__":
    # print(generate_telemetry('SKZ_35'))
    # --- 2. CONFIGURATION ---
    kafka_conf = {
        'bootstrap.servers': os.getenv('BOOTSTRAP_SERVERS'),
        'sasl.username': os.getenv('API_KEY'),
        'sasl.password': os.getenv('API_SECRET'),
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'acks': 'all',

        'linger.ms': 10,
        'compression.type': 'lz4'
    }

    sr_conf = {
        'url': os.getenv('SCHEMA_REGISTRY_URL'),
        'basic.auth.user.info': f"{os.getenv('SCHEMA_REGISTRY_API_KEY')}:{os.getenv('SCHEMA_REGISTRY_API_SECRET')}"
    }
     # --- 3. LOAD SCHEMA & INITIALIZE COMPONENTS ---

    # Load from the file we created
    script_dir = os.path.dirname(os.path.abspath(__file__))
    schema_path = os.path.join(script_dir, "schemas", "vehicle_telemetry.avsc")


    try:
        schema_str = load_schema(schema_path)
        print(f"✅ Loaded schema from {schema_path}")
    except FileNotFoundError:
        print(f"❌ Error: Could not find schema file at {schema_path}")
        exit(1)

    topic = 'vehicle.telemetry'

    schema_registry_client = SchemaRegistryClient(sr_conf)

    # 3. Configure the Avro Serializer
    # This automatically registers the schema if it doesn't exist
    avro_serializer = AvroSerializer(schema_registry_client,
                                     schema_str)

    producer = Producer(kafka_conf)

    # Vehicle IDs
    vehicles = [f"VEH_{str(i).zfill(3)}" for i in range(1, 11)]  # VEH_001 to VEH_010

    print("🚗 Starting vehicle telemetry simulator...")
    print(f"📡 Simulating {len(vehicles)} vehicles")
    print("📝 Using Avro serialization with Schema Registry")
    print("⏱️  Sending messages every 2 seconds")
    print("🛑 Press Ctrl+C to stop\n")

    try:
        message_count = 0
        while True:
            # Each vehicle sends telemetry
            for vehicle_id in vehicles:
                telemetry = generate_telemetry(vehicle_id)

                # Manually serialize key and value (recommended approach)
                try:
                    payload_bytes = avro_serializer(telemetry, SerializationContext(topic, MessageField.VALUE))

                    # Produce with pre-serialized bytes
                    producer.produce(
                        topic='vehicle.telemetry',
                        key=vehicle_id.encode('utf-8'),
                        value=payload_bytes,
                        on_delivery=delivery_callback
                    )
                    message_count += 1

                except Exception as e:
                    logger.error(f"Serialization error for {vehicle_id}: {e}")
                    print(f"❌ Serialization error: {e}")

            # Poll with small timeout to trigger delivery callbacks
            producer.poll(0.1)

            print(f"📊 Sent {message_count} messages total (✅ {delivery_success} delivered, ❌ {delivery_failed} failed)")
            time.sleep(2)  # Wait 2 seconds before next batch

    except KeyboardInterrupt:
        print(f"\n🛑 Stopping simulator... Sent {message_count} total messages")
        logger.info(
            f"Stopping simulator. Total: {message_count}, Success: {delivery_success}, Failed: {delivery_failed}")

        print("⏳ Flushing remaining messages...")
        producer.flush(timeout=10)  # Wait up to 10 seconds for delivery
        producer.poll(0.1)  # Process any remaining callbacks

        print("✅ All messages delivered")
        print(f"\n📈 Final Stats:")
        print(f"   Total messages: {message_count}")
        print(f"   Successfully delivered: {delivery_success}")
        print(f"   Failed: {delivery_failed}")
        print("\n📋 Next steps:")
        print("   1. Check Schema Registry in Confluent Cloud")
        print("   2. You should see Avro schema: vehicle.telemetry-value")
        print("   3. Proceed to Module 3 (Flink) - topic will auto-catalog!")
