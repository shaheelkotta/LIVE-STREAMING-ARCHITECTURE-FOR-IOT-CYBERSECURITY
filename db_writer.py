import json
import psycopg2
from kafka import KafkaConsumer
from datetime import datetime, timezone

# ==============================================================================
# 1. CONFIGURATION
# ==============================================================================
DB_HOST = "localhost"
DB_PORT = "5432"
DB_USER = "sec_admin"
DB_PASS = "admin123" 
DB_NAME = "iot_security"

KAFKA_BROKER = "localhost:9092"
TOPICS = ["iot-sensor-data", "iot-threat-alerts"]

# ==============================================================================
# 2. HELPER FUNCTIONS
# ==============================================================================
def parse_timestamp(ts):
    """
    Converts timestamps into a PostgreSQL-friendly format.
    IoT sensors often send time in milliseconds, but Python expects seconds.
    This function automatically detects and fixes millisecond timestamps.
    """
    if ts > 1e11:  # If the number is huge, it is in milliseconds
        ts = ts / 1000.0
    return datetime.fromtimestamp(ts, tz=timezone.utc)

# ==============================================================================
# 3. DATABASE INSERTION LOGIC
# ==============================================================================
def insert_raw_traffic(cursor, payload):
    """
    Processes everyday sensor data.
    Uses .get() to safely handle data even if the generator omits certain fields.
    """
    # 1. Verify we have the minimum required data to avoid database crashes
    if "device_id" not in payload or "timestamp" not in payload:
        return

    import json

    # 2. Extract ALL fields matching your database columns
    status = payload.get("status", "ONLINE")
    device_type = payload.get("device_type", "unknown")
    event_id = payload.get("event_id", "unknown")
    ip_address = payload.get("ip_address", "0.0.0.0")
    sequence_number = payload.get("sequence_number", 0)
    
    # Metrics needs to be formatted as JSON for your database's jsonb column
    metrics = json.dumps(payload.get("metrics", {}))

    # 3. Write to database using ALL 8 columns
    query = """
        INSERT INTO raw_traffic (time, event_id, device_id, device_type, ip_address, sequence_number, status, metrics)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
    """
    dt = parse_timestamp(payload["timestamp"])
    cursor.execute(query, (dt, event_id, payload["device_id"], device_type, ip_address, sequence_number, status, metrics))


def insert_threat_alert(cursor, payload):
    """
    Processes confirmed cyber attacks detected by Apache Flink.
    """
    query = """
        INSERT INTO threat_alerts (time, alert_type, severity, device_id, latency_ms)
        VALUES (%s, %s, %s, %s, %s);
    """
    dt = parse_timestamp(payload["timestamp"])
    cursor.execute(query, (dt, payload["alert_type"], payload["severity"], payload["device_id"], payload["latency_ms"]))

# ==============================================================================
# 4. MAIN ENGINE
# ==============================================================================
def main():
    print("=== STARTING LAYER 4 DATA ARCHIVER ===")
    
    # Step A: Connect to TimescaleDB
    try:
        conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASS, dbname=DB_NAME)
        conn.autocommit = True
        cursor = conn.cursor()
        print("[OK] Database connection successful.")
    except Exception as e:
        print(f"[ERROR] Database connection failed: {e}")
        return

    # Step B: Connect to Kafka
    try:
        consumer = KafkaConsumer(
            *TOPICS,
            bootstrap_servers=KAFKA_BROKER,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            auto_offset_reset='earliest', # Ensures we process all missed messages
            enable_auto_commit=True,
            group_id='timescaledb-archiver'
        )
        print(f"[OK] Kafka connection successful. Listening to: {TOPICS}")
    except Exception as e:
        print(f"[ERROR] Kafka connection failed: {e}")
        return

    print("\n--- SYSTEM ONLINE AND RECORDING ---\n")

    # Step C: The Infinite Listening Loop
    try:
        for message in consumer:
            payload = message.value
            topic = message.topic
            
            try:
                # Route the message to the correct table based on its topic
                if topic == "iot-sensor-data":
                    insert_raw_traffic(cursor, payload)
                elif topic == "iot-threat-alerts":
                    insert_threat_alert(cursor, payload)
            except Exception as e:
                # Catch row-level errors so the whole script doesn't crash
                print(f"[WARNING] Failed to insert record: {e} | Payload: {payload}")

    except KeyboardInterrupt:
        print("\n[INFO] Archiver shutting down safely...")
    finally:
        # Step D: Cleanup resources
        cursor.close()
        conn.close()
        consumer.close()
        print("[INFO] Connections closed.")

if __name__ == "__main__":
    main()