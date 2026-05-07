import os
import json
import csv
import time
from kafka import KafkaProducer

def main():
    print("Starting producer...")
    time.sleep(15)
    
    producer = KafkaProducer(
        bootstrap_servers=['kafka:29092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    data_dir = '/app/data'
    topic_name = 'sales_topic'
    global_row_id = 0
    total_rows = 0

    for file_name in sorted(os.listdir(data_dir)):
        if file_name.endswith('.csv'):
            file_path = os.path.join(data_dir, file_name)
            with open(file_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    total_rows += 1

    print(f"Total rows to send: {total_rows}")

    for file_name in sorted(os.listdir(data_dir)):
        if file_name.endswith('.csv'):
            file_path = os.path.join(data_dir, file_name)
            print(f"Reading file: {file_path}")
            with open(file_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    global_row_id += 1
                    row['global_id'] = str(global_row_id)
                    producer.send(topic_name, row)
                    if global_row_id % 1000 == 0:
                        print(f"  Sent {global_row_id} rows...")
            print(f"Completed {file_name}")

    producer.flush()
    print(f"✓ Finished sending all {global_row_id} rows to Kafka!")
    print("Waiting before shutdown to ensure data is committed...")
    time.sleep(5)

if __name__ == "__main__":
    main()
