import os
import csv
import random
from faker import Faker
from datetime import datetime, timedelta

# Initialize Faker and paths
fake = Faker()
RAW_DATA_DIR = os.path.join("data", "raw")
os.makedirs(RAW_DATA_DIR, exist_ok=True)

# --- PARAMETERS ---
N_CUSTOMERS = 1000
N_PRODUCTS = 500
N_TRANSACTIONS = 5000

# --- GENERATE CUSTOMERS ---
def generate_customers(n=N_CUSTOMERS):
    customers = []
    for i in range(1, n + 1):
        customers.append({
            "id": i,
            "name": fake.name(),
            "email": fake.email(),
            "registration_date": fake.date_between(start_date="-3y", end_date="today").isoformat(),
            "country": fake.country()
        })
    return customers

# --- GENERATE PRODUCTS ---
def generate_products(n=N_PRODUCTS):
    categories = ["Electronics", "Clothing", "Books", "Home", "Beauty"]
    suppliers = ["Supplier A", "Supplier B", "Supplier C"]
    products = []
    for i in range(1, n + 1):
        products.append({
            "id": i,
            "name": fake.word().capitalize(),
            "category": random.choice(categories),
            "price": round(random.uniform(5, 500), 2),
            "supplier": random.choice(suppliers)
        })
    return products

# --- GENERATE TRANSACTIONS ---
def generate_transactions(customers, products, n=N_TRANSACTIONS):
    transactions = []
    payment_methods = ["Credit Card", "PayPal", "Bank Transfer"]
    for i in range(1, n + 1):
        product = random.choice(products)
        transactions.append({
            "id": i,
            "customer_id": random.choice(customers)["id"],
            "product_id": product["id"],
            "quantity": random.randint(1, 5),
            "timestamp": fake.date_time_between(start_date="-1y", end_date="now").isoformat(),
            "payment_method": random.choice(payment_methods)
        })
    return transactions

# --- SAVE TO CSV ---
def save_csv(data, filename):
    path = os.path.join(RAW_DATA_DIR, filename)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)
    print(f"Saved {filename} ({len(data)} records)")

if __name__ == "__main__":
    customers = generate_customers()
    products = generate_products()
    transactions = generate_transactions(customers, products)

    save_csv(customers, "customers.csv")
    save_csv(products, "products.csv")
    save_csv(transactions, "transactions.csv")
