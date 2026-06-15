import pandas as pd
import json
import time
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

datasets = {
    "customers": pd.read_csv("data/customers.csv"),
    "geography": pd.read_csv("data/geography.csv"),
    "inventory": pd.read_csv("data/inventory.csv"),
    "order_items": pd.read_csv("data/order_items.csv"),
    "orders": pd.read_csv("data/orders.csv"),
    "payments": pd.read_csv("data/payments.csv"),
    "products": pd.read_csv("data/products.csv"),
    "promotions": pd.read_csv("data/promotions.csv"),
    "returns": pd.read_csv("data/returns.csv"),
    "reviews": pd.read_csv("data/reviews.csv"),
    "sales": pd.read_csv("data/sales.csv"),
    "shipments": pd.read_csv("data/shipments.csv"),
    "web_traffic": pd.read_csv("data/web_traffic.csv")
}

iterators = {
    topic: df.iterrows()
    for topic, df in datasets.items()
}

print("START STREAMING...")

while True:
    for topic, iterator in iterators.items():
        try:
            _, row = next(iterator)

            message = {k.lower(): v for k, v in row.to_dict().items()}
            message["source_topic"] = topic

            producer.send(topic, value=message)

            print(f"[{topic}] sent")

        except StopIteration:
            continue

    time.sleep(1)