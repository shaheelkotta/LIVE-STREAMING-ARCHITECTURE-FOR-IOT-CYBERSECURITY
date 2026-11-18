import time
import json
import random
import uuid
from datetime import datetime
from kafka import KafkaProducer

KAFKA_TOPIC = "iot-sensor-data"
KAFKA_BOOTSTRAP_SERVER = "localhost:9092"

# ==============================================================================
# LAYER 1: EXPLICIT VIRTUAL DEVICE LIST
# ==============================================================================
DEVICE_LIST = [
    # 5 Temperature Sensors
    {"id": "temp-sensor-01-server-room", "type": "temp_sensor", "ip": "192.168.1.10"},
    {"id": "temp-sensor-02-warehouse", "type": "temp_sensor", "ip": "192.168.1.11"},
    {"id": "temp-sensor-03-office", "type": "temp_sensor", "ip": "192.168.1.12"},
    {"id": "temp-sensor-04-datacenter", "type": "temp_sensor", "ip": "192.168.1.13"},
    {"id": "temp-sensor-05-lobby", "type": "temp_sensor", "ip": "192.168.1.14"},

    # 5 Smart Cameras
    {"id": "smart-cam-01-front-door", "type": "smart_camera", "ip": "192.168.1.15"},
    {"id": "smart-cam-02-back-door", "type": "smart_camera", "ip": "192.168.1.16"},
    {"id": "smart-cam-03-loading-dock", "type": "smart_camera", "ip": "192.168.1.17"},
    {"id": "smart-cam-04-hallway", "type": "smart_camera", "ip": "192.168.1.18"},
    {"id": "smart-cam-05-parking-lot", "type": "smart_camera", "ip": "192.168.1.19"},

    # 5 Smart Meters
    {"id": "smart-meter-01-main-grid", "type": "smart_meter", "ip": "192.168.1.20"},
    {"id": "smart-meter-02-hvac-system", "type": "smart_meter", "ip": "192.168.1.21"},
    {"id": "smart-meter-03-lighting", "type": "smart_meter", "ip": "192.168.1.22"},
    {"id": "smart-meter-04-server-rack-a", "type": "smart_meter", "ip": "192.168.1.23"},
    {"id": "smart-meter-05-server-rack-b", "type": "smart_meter", "ip": "192.168.1.24"}
]

# --- V2 STATE TRACKING ---
device_state = {
    device["id"]: {"sequence_number": 0, "last_heartbeat": time.time()} 
    for device in DEVICE_LIST
}

ATTACK_PROBABILITY = 0.05       
TRAFFIC_SPIKE_PROBABILITY = 0.02 

# ==============================================================================
# CORE LOGIC
# ==============================================================================

def generate_normal_payload(device):
    """Generates standard operating data and increments sequence number."""
    seq = device_state[device['id']]["sequence_number"]
    device_state[device['id']]["sequence_number"] += 1
    
    base_payload = {
        "event_id": str(uuid.uuid4()),
        "device_id": device['id'],
        "device_type": device['type'],
        "ip_address": device['ip'],
        "sequence_number": seq,
        "timestamp": round(time.time(), 3),
        "status": "ONLINE"
    }

    if device['type'] == "temp_sensor":
        base_payload["metrics"] = {"temperature": round(random.uniform(20.0, 25.0), 2)}
    elif device['type'] == "smart_camera":
        base_payload["metrics"] = {"bitrate_kbps": random.randint(1000, 2500), "auth_status": "SUCCESS"}
    elif device['type'] == "smart_meter":
        base_payload["metrics"] = {"power_usage_kwh": round(random.uniform(1.0, 5.0), 2)}

    return base_payload

def send_to_kafka(producer, payload):
    """Sends complex payload to Kafka partitions, but prints a simple summary to the terminal."""
    display_value = 0
    
    if payload.get("status") == "HEARTBEAT":
        display_value = 0
    elif payload["device_type"] == "temp_sensor":
        display_value = payload["metrics"]["temperature"]
    elif payload["device_type"] == "smart_camera":
        display_value = payload["metrics"]["bitrate_kbps"]
    elif payload["device_type"] == "smart_meter":
        display_value = payload["metrics"]["power_usage_kwh"]

    # Print the clean summary to your screen
    terminal_output = {
        "Device": payload["device_id"],
        "Value": display_value,
        "Time": datetime.now().strftime("%H:%M:%S")
    }
    print(json.dumps(terminal_output))
    
    # Send the full, complex payload to Kafka (Layer 2)
    producer.send(
        KAFKA_TOPIC, 
        key=payload['device_id'].encode('utf-8'), 
        value=payload
    )

if __name__ == "__main__":
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVER,
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
    except Exception as e:
        print(f"Kafka connection failed. Did you run 'docker compose up -d'? Error: {e}")
        exit()

    try:
        while True:
            current_time = time.time()

            # --- STEP A: QUICK HEARTBEAT CHECK ---
            # Even if a device isn't randomly selected, we must ensure it pings every 10s
            for check_device in DEVICE_LIST:
                if current_time - device_state[check_device['id']]["last_heartbeat"] > 10:
                    heartbeat_payload = generate_normal_payload(check_device)
                    heartbeat_payload["status"] = "HEARTBEAT"
                    if "metrics" in heartbeat_payload:
                        del heartbeat_payload["metrics"]
                    send_to_kafka(producer, heartbeat_payload)
                    device_state[check_device['id']]["last_heartbeat"] = current_time

            # --- STEP B: SELECT ONE RANDOM DEVICE ---
            # This makes the network traffic look perfectly organic and scattered
            device = random.choice(DEVICE_LIST)

            # --- STEP C: NORMAL DATA ---
            payload = generate_normal_payload(device)
            send_to_kafka(producer, payload)

           # --- STEP D: ATTACKS ---
            
            # 1. DOS ATTACK (Volumetric flood on any device)
            if random.random() < TRAFFIC_SPIKE_PROBABILITY:
                for _ in range(50): 
                    dos_payload = generate_normal_payload(device)
                    send_to_kafka(producer, dos_payload)

            # 2. DATA EXFILTRATION (NEW: Impossible Temperature Outlier)
            elif device['type'] == "temp_sensor" and random.random() < ATTACK_PROBABILITY:
                exfil_payload = generate_normal_payload(device)
                # Override the normal 20-25 range with a massive, impossible number
                exfil_payload["metrics"]["temperature"] = round(random.uniform(9000.0, 9500.0), 2)
                send_to_kafka(producer, exfil_payload)

            # 3. REPLAY ATTACK (Sequence spoofing on smart meters)
            elif device['type'] == "smart_meter" and random.random() < ATTACK_PROBABILITY:
                replay_payload = generate_normal_payload(device)
                stolen_sequence = max(0, device_state[device['id']]["sequence_number"] - 5)
                replay_payload["sequence_number"] = stolen_sequence
                send_to_kafka(producer, replay_payload)
            # Wait a tiny fraction of a second before picking the next random device
            time.sleep(0.2)

    except KeyboardInterrupt:
        producer.close()