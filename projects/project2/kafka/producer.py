from kafka import KafkaProducer
import csv
import json
import time
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

producer = KafkaProducer(
    bootstrap_servers=['localhost:9093'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
)

topic = 'taxi_trips'
count = 0

with open('/tmp/sorted_data.csv', 'r') as file:
    csv_reader = csv.reader(file)
    for row in csv_reader:
        if len(row) == 17:  # Ensure all columns are present
            record = {
                "medallion": row[0],
                "hack_license": row[1],
                "pickup_datetime": row[2],
                "dropoff_datetime": row[3],
                "trip_time_in_secs": row[4],
                "trip_distance": row[5],
                "pickup_longitude": row[6],
                "pickup_latitude": row[7],
                "dropoff_longitude": row[8],
                "dropoff_latitude": row[9],
                "payment_type": row[10],
                "fare_amount": row[11],
                "surcharge": row[12],
                "mta_tax": row[13],
                "tip_amount": row[14],
                "tolls_amount": row[15],
                "total_amount": row[16]
            }
            producer.send(topic, value=record)
            count += 1
            logger.info(f"Produced record {count}: Trip from {record['pickup_datetime']} with medallion {record['medallion']}")
            time.sleep(0.1)  # Small delay to simulate streaming

producer.flush()
logger.info(f"{count} records sent to Kafka.")