import psycopg2

# ==============================================================================
# CONFIGURATION (Ensure these match your docker-compose.yml)
# ==============================================================================
DB_HOST = "localhost"
DB_PORT = "5432"
DB_USER = "sec_admin"
DB_PASS = "admin123" 
DB_NAME = "iot_security"

def setup_database():
    conn = None   # <--- FIX: We declare the variables up here first
    cursor = None
    try:
        # Connect to the default postgres database first to create our custom DB
        conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASS, dbname="postgres")
        conn.autocommit = True
        cursor = conn.cursor()
        
        # Create the database if it doesn't exist
        cursor.execute(f"SELECT 1 FROM pg_catalog.pg_database WHERE datname = '{DB_NAME}'")
        exists = cursor.fetchone()
        if not exists:
            cursor.execute(f"CREATE DATABASE {DB_NAME}")
            print(f"Database '{DB_NAME}' created.")
        cursor.close()
        conn.close()

        # Reconnect directly to our new iot_security database
        conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASS, dbname=DB_NAME)
        conn.autocommit = True
        cursor = conn.cursor()

        # Enable the TimescaleDB extension
        cursor.execute("CREATE EXTENSION IF NOT EXISTS timescaledb;")
        print("TimescaleDB extension enabled.")

        # ---------------------------------------------------------
        # TABLE 1: THE GROUND TRUTH (From generator.py)
        # ---------------------------------------------------------
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS raw_traffic (
                time TIMESTAMPTZ NOT NULL,
                device_id TEXT NOT NULL,
                device_type TEXT NOT NULL,
                ground_truth TEXT NOT NULL
            );
        """)
        cursor.execute("SELECT create_hypertable('raw_traffic', 'time', if_not_exists => TRUE);")
        print("Hypertable 'raw_traffic' created.")

        # ---------------------------------------------------------
        # TABLE 2: THE ALERTS (From flink_processor.py)
        # ---------------------------------------------------------
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS threat_alerts (
                time TIMESTAMPTZ NOT NULL,
                alert_type TEXT NOT NULL,
                severity TEXT NOT NULL,
                device_id TEXT NOT NULL,
                latency_ms FLOAT NOT NULL
            );
        """)
        cursor.execute("SELECT create_hypertable('threat_alerts', 'time', if_not_exists => TRUE);")
        print("Hypertable 'threat_alerts' created.")

        print("=== LAYER 4 ONLINE: FORENSIC VAULT READY ===")

    except Exception as e:
        print(f"Database setup failed. Is your TimescaleDB Docker container running? Error: {e}")
    finally:
        if conn:
            cursor.close()
            conn.close()

if __name__ == "__main__":
    setup_database()