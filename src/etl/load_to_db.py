# src/etl/load_to_db.py

import os
import pandas as pd
from sqlalchemy import Table, inspect, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from src.scripts.db_setup import engine, metadata

# ---------- CSV Paths ----------
CSV_PATHS = {
    "customers": "data/raw/customers.csv",
    "products": "data/raw/products.csv",
    "transactions": "data/raw/transactions.csv",
}

# ---------- Create load_audit table if not exists ----------
inspector = inspect(engine)
if not inspector.has_table("load_audit"):
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE load_audit (
                id SERIAL PRIMARY KEY,
                load_timestamp TIMESTAMP DEFAULT NOW(),
                table_name TEXT NOT NULL,
                row_count INT,
                status TEXT
            );
        """))
    print("✅ Created load_audit table.")


# ---------- Helper: Upsert Table ----------
def upsert_table(df, table_name, key_columns):
    """Perform bulk upsert into PostgreSQL using ON CONFLICT."""
    if df.empty:
        print(f"⚠️ Skipping {table_name} — DataFrame empty")
        return 0

    df = df.where(pd.notnull(df), None)
    table = Table(table_name, metadata, autoload_with=engine)

    insert_stmt = pg_insert(table)
    update_dict = {c.name: insert_stmt.excluded[c.name]
                   for c in table.columns if c.name not in key_columns}

    upsert_stmt = insert_stmt.on_conflict_do_update(
        index_elements=key_columns,
        set_=update_dict
    )

    with engine.begin() as conn:
        conn.execute(upsert_stmt, df.to_dict(orient="records"))
    return len(df)


# ---------- Helper: Audit Logging ----------
def log_audit(table_name, row_count, status="success"):
    with engine.begin() as conn:
        conn.execute(
            text("INSERT INTO load_audit (table_name, row_count, status) VALUES (:t, :r, :s)"),
            {"t": table_name, "r": row_count, "s": status},
        )


# ---------- ETL Pipeline ----------
def load_data():
    # ------------------ 1️⃣ CUSTOMERS ------------------
    if os.path.exists(CSV_PATHS["customers"]):
        customers_df = pd.read_csv(CSV_PATHS["customers"])
        customers_df.columns = [c.strip() for c in customers_df.columns]
        customers_df["id"] = customers_df["id"].astype(int)
        customers_df = customers_df.where(pd.notnull(customers_df), None)

        rows = upsert_table(customers_df, "customers", key_columns=["id"])
        log_audit("customers", rows)
        print(f"✅ Loaded {rows} customers")
    else:
        print("❌ customers.csv not found")

    # ------------------ 2️⃣ SUPPLIERS + PRODUCTS ------------------
    if os.path.exists(CSV_PATHS["products"]):
        df = pd.read_csv(CSV_PATHS["products"])
        df = df.where(pd.notnull(df), None)

        suppliers_df = pd.DataFrame(df["supplier"].dropna().unique(), columns=["name"])
        suppliers_df = suppliers_df.drop_duplicates(subset=["name"])
        rows = upsert_table(suppliers_df, "suppliers", key_columns=["name"])
        log_audit("suppliers", rows)
        print(f"✅ Loaded {rows} suppliers")

        with engine.connect() as conn:
            supplier_map = dict(conn.execute(text("SELECT name, id FROM suppliers")).fetchall())

        df["supplier_id"] = df["supplier"].map(supplier_map)
        df = df.drop(columns=["supplier"])

        rows = upsert_table(df, "products", key_columns=["id"])
        log_audit("products", rows)
        print(f"✅ Loaded {rows} products")
    else:
        print("❌ products.csv not found")

    # ------------------ 3️⃣ PAYMENT METHODS + TRANSACTIONS ------------------
    if os.path.exists(CSV_PATHS["transactions"]):
        df = pd.read_csv(CSV_PATHS["transactions"])
        df = df.where(pd.notnull(df), None)

        # 3a. Payment Methods
        payment_df = pd.DataFrame(df["payment_method"].dropna().unique(), columns=["method"])
        rows = upsert_table(payment_df, "payment_methods", key_columns=["method"])
        log_audit("payment_methods", rows)
        print(f"✅ Loaded {rows} payment methods")

        with engine.connect() as conn:
            payment_map = dict(conn.execute(text("SELECT method, id FROM payment_methods")).fetchall())
        df["payment_method_id"] = df["payment_method"].map(payment_map)

        # 3b. Transactions
        transaction_df = df[["id", "customer_id", "timestamp", "payment_method_id"]].drop_duplicates()

        with engine.connect() as conn:
            existing_customers = {r[0] for r in conn.execute(text("SELECT id FROM customers")).fetchall()}

        missing_ids = set(transaction_df["customer_id"]) - existing_customers
        if missing_ids:
            raise ValueError(f"Cannot insert transactions: missing customer IDs: {missing_ids}")

        rows = upsert_table(transaction_df, "transactions", key_columns=["id"])
        log_audit("transactions", rows)
        print(f"✅ Loaded {rows} transactions")

        # 3c. Transaction Items (no price)
        transaction_items_df = df[["id", "product_id", "quantity"]].rename(
            columns={"id": "transaction_id"}
        )

        rows = upsert_table(transaction_items_df, "transaction_items",
                            key_columns=["transaction_id", "product_id"])
        log_audit("transaction_items", rows)
        print(f"✅ Loaded {rows} transaction_items")
    else:
        print("❌ transactions.csv not found")


# ---------- MAIN ----------
if __name__ == "__main__":
    load_data()
