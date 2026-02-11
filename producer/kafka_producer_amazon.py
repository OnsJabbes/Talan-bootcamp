import csv, json, time, os
from kafka import KafkaProducer
from kafka.errors import KafkaError

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC  = os.getenv("KAFKA_TOPIC", "amazon-sales")
CSV_FILE     = os.getenv("CSV_FILE", "/data/Amazon.csv")
RATE         = int(os.getenv("RATE", "1000"))
DELAY        = (60.0 / RATE) if RATE > 0 else 0

def create_producer():
    for i in range(15):
        try:
            return KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
                retries=5,
                batch_size=32768,
                linger_ms=10,
            )
        except KafkaError as e:
            print("Kafka not ready (%d/15): %s" % (i+1, e))
            time.sleep(3)
    raise RuntimeError("Kafka not reachable")

def safe_int(x, default=0):
    try: return int(x)
    except: return default

def safe_float(x, default=0.0):
    try: return float(x)
    except: return default

def main():
    print("Producer config:", KAFKA_BROKER, KAFKA_TOPIC, CSV_FILE)
    if not os.path.exists(CSV_FILE):
        raise FileNotFoundError(CSV_FILE)

    producer = create_producer()
    sent = 0

    with open(CSV_FILE, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                msg = {
                    "OrderID": row.get("OrderID"),
                    "OrderDate": row.get("OrderDate"),
                    "CustomerID": row.get("CustomerID"),
                    "CustomerName": row.get("CustomerName"),
                    "ProductID": row.get("ProductID"),
                    "ProductName": row.get("ProductName"),
                    "Category": row.get("Category"),
                    "Brand": row.get("Brand"),
                    "Quantity": safe_int(row.get("Quantity")),
                    "UnitPrice": safe_float(row.get("UnitPrice")),
                    "Discount": safe_float(row.get("Discount")),
                    "Tax": safe_float(row.get("Tax")),
                    "ShippingCost": safe_float(row.get("ShippingCost")),
                    "TotalAmount": safe_float(row.get("TotalAmount")),
                    "PaymentMethod": row.get("PaymentMethod"),
                    "OrderStatus": row.get("OrderStatus"),
                    "City": row.get("City"),
                    "State": row.get("State"),
                    "Country": row.get("Country"),
                    "SellerID": row.get("SellerID"),
                }
                producer.send(KAFKA_TOPIC, value=msg)
                sent += 1
                if sent % 5000 == 0:
                    producer.flush()
                    print("sent %d" % sent)
                if DELAY > 0:
                    time.sleep(DELAY)
            except Exception as e:
                print("skip row:", e)

    producer.flush()
    producer.close()
    print("done:", sent)

if __name__ == "__main__":
    main()
