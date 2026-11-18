import psycopg2

# ==============================================================================
# CONFIGURATION
# ==============================================================================
DB_HOST = "localhost"
DB_PORT = "5432"
DB_USER = "sec_admin"
DB_PASS = "admin123" 
DB_NAME = "iot_security"

def calculate_deterministic_metrics():
    print("--- DETERMINISTIC SYSTEM EVALUATION ---")
    try:
        conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASS, dbname=DB_NAME)
        cursor = conn.cursor()

        # 1. Total Packets Generated (Layer 1)
        cursor.execute("SELECT COUNT(*) FROM raw_traffic;")
        total_packets = cursor.fetchone()[0]

        if total_packets == 0:
            print("Database is empty. Please let the generator and Flink engine run for a minute!")
            return

        # 2. Total Actual Attacks (Layer 1 Ground Truth)
        cursor.execute("SELECT COUNT(*) FROM raw_traffic WHERE ground_truth != 'NORMAL';")
        actual_attacks = cursor.fetchone()[0]

        # 3. Total Normal Traffic
        actual_normal = total_packets - actual_attacks

        # ====================================================================
        # NEW DETERMINISTIC MATH (Using the new ground_truth column)
        # ====================================================================
        
        # True Positives: Flink fired an alert AND the payload was actually an attack
        cursor.execute("SELECT COUNT(*) FROM threat_alerts WHERE ground_truth != 'NORMAL';")
        true_positives = cursor.fetchone()[0]

        # False Positives: Flink fired an alert BUT the payload was perfectly normal
        cursor.execute("SELECT COUNT(*) FROM threat_alerts WHERE ground_truth = 'NORMAL';")
        false_positives = cursor.fetchone()[0]

        # Calculate the rest of the Confusion Matrix
        false_negatives = actual_attacks - true_positives
        true_negatives = actual_normal - false_positives

        # ====================================================================
        # THE ACADEMIC RESEARCH SCORES
        # ====================================================================
        
        # Avoid division by zero if the system just started
        precision = true_positives / (true_positives + false_positives) if (true_positives + false_positives) > 0 else 0
        recall = true_positives / (true_positives + false_negatives) if (true_positives + false_negatives) > 0 else 0
        f1_score = 2 * (precision * recall) / (precision + recall) if (precision + recall) > 0 else 0
        accuracy = (true_positives + true_negatives) / total_packets if total_packets > 0 else 0

        print(f"Total Traffic Analyzed: {total_packets} packets")
        print("\n=== CONFUSION MATRIX ===")
        print(f"True Positives (Caught Hackers):      {true_positives}")
        print(f"False Positives (False Alarms):       {false_positives}")
        print(f"False Negatives (Missed Attacks):     {false_negatives}")
        print(f"True Negatives (Ignored Safe Data):   {true_negatives}")

        print("\n=== FINAL RESEARCH SCORES ===")
        print(f"Accuracy:  {accuracy * 100:.2f}%")
        print(f"Precision: {precision * 100:.2f}%")
        print(f"Recall:    {recall * 100:.2f}%")
        print(f"F1 Score:  {f1_score * 100:.2f}%")

    except Exception as e:
        print(f"Evaluation failed: {e}")
    finally:
        if 'conn' in locals() and conn:
            cursor.close()
            conn.close()

if __name__ == "__main__":
    calculate_deterministic_metrics()