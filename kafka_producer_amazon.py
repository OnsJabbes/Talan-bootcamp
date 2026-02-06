"""
Kafka Producer for Amazon Sales Data
Reads Amazon.csv and sends records to Kafka topic 'supermarket-sales'
"""
import csv
import json
import time
import os
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Configuration
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'supermarket-sales')
CSV_FILE = 'Amazon.csv'
DELAY_SECONDS = 0.01  # Delay between messages to simulate streaming


def create_producer():
    """Create and return a Kafka producer instance"""
    max_retries = 10
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                max_request_size=500000000,
                buffer_memory=500000000,
                acks='all',
                retries=3
            )
            print(f"✓ Successfully connected to Kafka broker: {KAFKA_BROKER}")
            return producer
        except KafkaError as e:
            retry_count += 1
            print(f"⚠ Attempt {retry_count}/{max_retries}: Failed to connect to Kafka - {e}")
            time.sleep(5)
    
    raise Exception("Failed to connect to Kafka after maximum retries")


def read_and_send_data(producer):
    """Read CSV file and send each row to Kafka"""
    print(f"\n📊 Starting to read data from {CSV_FILE}")
    
    if not os.path.exists(CSV_FILE):
        raise FileNotFoundError(f"CSV file not found: {CSV_FILE}")
    
    records_sent = 0
    
    with open(CSV_FILE, 'r', encoding='utf-8') as file:
        csv_reader = csv.DictReader(file)
        
        for row in csv_reader:
            try:
                # Convert row to JSON and send to Kafka
                message = {
                    'OrderID': row['OrderID'],
                    'OrderDate': row['OrderDate'],
                    'CustomerID': row['CustomerID'],
                    'CustomerName': row['CustomerName'],
                    'ProductID': row['ProductID'],
                    'ProductName': row['ProductName'],
                    'Category': row['Category'],
                    'Brand': row['Brand'],
                    'Quantity': int(row['Quantity']),
                    'UnitPrice': float(row['UnitPrice']),
                    'Discount': float(row['Discount']),
                    'Tax': float(row['Tax']),
                    'ShippingCost': float(row['ShippingCost']),
                    'TotalAmount': float(row['TotalAmount']),
                    'PaymentMethod': row['PaymentMethod'],
                    'OrderStatus': row['OrderStatus'],
                    'City': row['City'],
                    'State': row['State'],
                    'Country': row['Country'],
                    'SellerID': row['SellerID']
                }
                
                # Send to Kafka
                future = producer.send(KAFKA_TOPIC, value=message)
                record_metadata = future.get(timeout=10)
                
                records_sent += 1
                
                if records_sent % 100 == 0:
                    print(f"📤 Sent {records_sent} records to topic '{KAFKA_TOPIC}'")
                
                # Simulate streaming with a small delay
                time.sleep(DELAY_SECONDS)
                
            except Exception as e:
                print(f"❌ Error sending record {records_sent + 1}: {e}")
                continue
    
    print(f"\n✅ Successfully sent {records_sent} records to Kafka topic '{KAFKA_TOPIC}'")
    return records_sent


def main():
    """Main function to run the Kafka producer"""
    print("=" * 70)
    print("🚀 KAFKA PRODUCER - Amazon Sales Data Pipeline")
    print("=" * 70)
    print(f"Kafka Broker: {KAFKA_BROKER}")
    print(f"Kafka Topic: {KAFKA_TOPIC}")
    print(f"Source File: {CSV_FILE}")
    print("=" * 70)
    
    try:
        # Create Kafka producer
        producer = create_producer()
        
        # Read CSV and send data
        total_records = read_and_send_data(producer)
        
        # Flush and close
        producer.flush()
        producer.close()
        
        print(f"\n{'=' * 70}")
        print(f"✅ COMPLETED: {total_records} records successfully sent to Kafka")
        print(f"{'=' * 70}")
        
    except KeyboardInterrupt:
        print("\n⚠ Process interrupted by user")
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        raise


if __name__ == "__main__":
    main()
