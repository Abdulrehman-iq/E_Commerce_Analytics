import csv
import random
import os
from datetime import datetime, timedelta

output_folder = "/app/batch_processing/data"
os.makedirs(output_folder, exist_ok=True)

# Generate sample users
users = [f"user_{i}" for i in range(1, 101)]
user_data = []
for user in users:
    user_data.append({
        "user_id": user,
        "name": f"User {user.split('_')[1]}",
        "location": random.choice(["NY", "CA", "TX", "FL", "IL"]),
        "signup_date": (datetime.now() - timedelta(days=random.randint(1, 365))).strftime("%Y-%m-%d")
    })

# Generate sample products
products = [f"prod_{i}" for i in range(1, 21)]
product_data = []
for product in products:
    product_data.append({
        "product_id": product,
        "category": random.choice(["Electronics", "Clothing", "Home", "Books", "Toys"]),
        "price": round(random.uniform(10, 500), 2),
        "brand": random.choice(["BrandA", "BrandB", "BrandC", "BrandD"])
    })

# Generate orders
order_data = []
for i in range(1, 1001):
    timestamp = (datetime.now() - timedelta(days=random.randint(0, 30))).strftime("%Y-%m-%d %H:%M:%S")
    order_data.append({
        "order_id": f"order_{i}",
        "user_id": random.choice(users),
        "product_id": random.choice(products),
        "timestamp": timestamp,
        "revenue": round(random.uniform(10, 500), 2),
        "quantity": random.randint(1, 5)
    })

# Write to CSV files in the Docker-mounted folder
def write_csv(filename, data, fieldnames):
    with open(filename, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)

write_csv(f'{output_folder}/users.csv', user_data, ["user_id", "name", "location", "signup_date"])
write_csv(f'{output_folder}/products.csv', product_data, ["product_id", "category", "price", "brand"])
write_csv(f'{output_folder}/orders.csv', order_data, ["order_id", "user_id", "product_id", "timestamp", "revenue", "quantity"])
