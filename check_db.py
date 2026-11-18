import psycopg2

conn = psycopg2.connect(host="localhost", port="5432", user="sec_admin", password="admin123", dbname="iot_security")
cursor = conn.cursor()

print("--- LATEST 5 RAW TRAFFIC LOGS (From Generator) ---")
# Querying the raw_traffic table correctly
cursor.execute("SELECT time, device_id, status FROM raw_traffic ORDER BY time DESC LIMIT 5;")
for row in cursor.fetchall():
    print(row)

print("\n--- LATEST 5 THREAT ALERTS (From Flink) ---")
# Querying the threat_alerts table correctly
cursor.execute("SELECT time, device_id, alert_type FROM threat_alerts ORDER BY time DESC LIMIT 5;")
for row in cursor.fetchall():
    print(row)

cursor.close()
conn.close()