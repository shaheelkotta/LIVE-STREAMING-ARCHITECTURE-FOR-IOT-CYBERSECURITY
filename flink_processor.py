import os
import json
import time
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaSource, 
    KafkaOffsetsInitializer, 
    KafkaSink, 
    KafkaRecordSerializationSchema, 
    DeliveryGuarantee
)
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream.functions import KeyedProcessFunction
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.common import Configuration

# ==============================================================================
# CONFIGURATION
# ==============================================================================
KAFKA_TOPIC = "iot-sensor-data"
KAFKA_ALERTS_TOPIC = "iot-threat-alerts"
KAFKA_BROKER = "localhost:9092"
JAR_PATH = f"file://{os.getcwd()}/flink-sql-connector-kafka-3.1.0-1.18.jar"

# ==============================================================================
# LAYER 3: THE MASTER THREAT DETECTOR 
# ==============================================================================
class MasterThreatDetector(KeyedProcessFunction):
    def open(self, runtime_context):
        """
        Initializes the 'Memory Boxes' in RAM for every single device.
        """
        # Memory Box 1: Replay Attack (Tracks highest sequence number)
        self.seq_state = runtime_context.get_state(ValueStateDescriptor("max_seq", Types.INT()))
        
        # Memory Box 2: DoS Attack (Tracks packet count and window start time)
        self.dos_state = runtime_context.get_state(ValueStateDescriptor("dos_tracker", Types.STRING()))

    def process_element(self, value, ctx: 'KeyedProcessFunction.Context'):
        """
        The core engine. Evaluates packets, calculates latency, and outputs strict JSON alerts.
        """
        try:
            payload = json.loads(value)
            device_id = payload.get("device_id", "Unknown")
            device_type = payload.get("device_type", "Unknown")
            seq_num = payload.get("sequence_number", 0)
            
            # 1. Grab the exact time the packet was born at Layer 1
            creation_time = payload.get("timestamp", time.time())
            
            # 2. Grab the exact time Flink is processing it right now at Layer 3
            current_time = time.time()
            
            # 3. Calculate the Latency in milliseconds
            latency_ms = round((current_time - creation_time) * 1000, 2)

            # ======================================================
            # ALGORITHM 1: DATA EXFILTRATION
            # ======================================================
            if device_type == "temp_sensor":
                temp = payload.get("metrics", {}).get("temperature", 0)
                if temp > 100.0:
                    yield json.dumps({
                        "alert_type": "DATA_EXFILTRATION",
                        "severity": "LOW",
                        "device_id": device_id,
                        "evidence": f"Impossible temperature: {temp}°C",
                        "latency_ms": latency_ms,
                        "timestamp": current_time
                    })

            # ======================================================
            # ALGORITHM 2: REPLAY ATTACK
            # ======================================================
            if device_type == "smart_meter":
                max_seq = self.seq_state.value()
                if max_seq is None:
                    self.seq_state.update(seq_num)
                elif seq_num < max_seq:
                    yield json.dumps({
                        "alert_type": "REPLAY_ATTACK",
                        "severity": "MEDIUM",
                        "device_id": device_id,
                        "evidence": f"Old Sequence: {seq_num} (Expected > {max_seq})",
                        "latency_ms": latency_ms,
                        "timestamp": current_time
                    })
                else:
                    self.seq_state.update(seq_num)

            # ======================================================
            # ALGORITHM 3: DOS ATTACK
            # ======================================================
            dos_data_str = self.dos_state.value()
            if dos_data_str is None:
                dos_data = {"count": 1, "start_time": current_time}
            else:
                dos_data = json.loads(dos_data_str)
                
                if current_time - dos_data["start_time"] < 10.0:
                    dos_data["count"] += 1
                    if dos_data["count"] > 20: 
                        yield json.dumps({
                            "alert_type": "DOS_ATTACK",
                            "severity": "HIGH",
                            "device_id": device_id,
                            "evidence": f"Network flood: {dos_data['count']} packets",
                            "latency_ms": latency_ms,
                            "timestamp": current_time
                        })
                        # Reset tracker after firing alert
                        dos_data = {"count": 1, "start_time": current_time} 
                else:
                    # Reset tracker if window expired
                    dos_data = {"count": 1, "start_time": current_time}
            
            self.dos_state.update(json.dumps(dos_data))

        except Exception as e:
            # If an error happens or packet is malformed, format that as JSON too
            yield json.dumps({"error": f"Failed to process packet: {e}"})
        # ======================================================
            # SYSTEM HEALTH: TRACK ACTIVE DEVICES
            # ======================================================
            if payload.get("status") == "HEARTBEAT":
                yield json.dumps({
                    "alert_type": "HEARTBEAT",
                    "severity": "INFO",
                    "device_id": device_id,
                    "evidence": "Device is online and transmitting",
                    "latency_ms": latency_ms,
                    "timestamp": current_time
                })

# ==============================================================================
# PIPELINE EXECUTION
# ==============================================================================
def main():
    # Force Flink to open the Web UI on port 8081
    config = Configuration()
    config.set_integer("rest.port", 8081)
    env = StreamExecutionEnvironment.get_execution_environment(config)
    
    env.add_jars(JAR_PATH)
    
    # --- UPGRADED TO FLINK 1.18 KAFKA SOURCE API ---
    kafka_source = KafkaSource.builder() \
        .set_bootstrap_servers(KAFKA_BROKER) \
        .set_topics(KAFKA_TOPIC) \
        .set_group_id("flink-soc-engine") \
        .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()
    
    # Read the stream using the new Watermark strategy
    stream = env.from_source(kafka_source, WatermarkStrategy.no_watermarks(), "Kafka_Source")
    
    # Route data into parallel lanes by device ID
    keyed_stream = stream.key_by(lambda x: json.loads(x).get("device_id", "Unknown"), key_type=Types.STRING())
    
    # Process through our math engine
    processed_stream = keyed_stream.process(MasterThreatDetector(), output_type=Types.STRING())
    
    # --- THE DUAL SINK (TERMINAL + DATABASE) ---
    kafka_sink = KafkaSink.builder() \
        .set_bootstrap_servers(KAFKA_BROKER) \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
                .set_topic(KAFKA_ALERTS_TOPIC)
                .set_value_serialization_schema(SimpleStringSchema())
                .build()
        ) \
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE) \
        .build()

    # 1. Print to Ubuntu terminal for your live presentation
    processed_stream.print()
    
    # 2. Sink to Kafka so the TimescaleDB vacuum cleaner can catch it
    processed_stream.sink_to(kafka_sink)
    
    print("=== STARTING LAYER 3: FLINK SOC ENGINE ===")
    print(f"Algorithms Loaded: Exfiltration, Replay, DoS")
    print(f"Routing Alerts to Terminal and Kafka Topic: {KAFKA_ALERTS_TOPIC}")
    env.execute("IoT_Threat_Detection_Job")

if __name__ == '__main__':
    main()