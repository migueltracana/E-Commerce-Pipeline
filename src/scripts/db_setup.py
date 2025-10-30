import os
from sqlalchemy import (
    create_engine, Column, Integer, UniqueConstraint, String, Float, DateTime,
    ForeignKey, Index, func
)
from sqlalchemy.orm import relationship, declarative_base
from config.config import load_config

# ---------- Load Configuration ----------
config = load_config(env=os.environ.get("ENV", "dev"))
db_conf = config["database"]

DB_URL = (
    f"postgresql+psycopg2://{db_conf['user']}:{db_conf['password']}@"
    f"{db_conf['host']}:{db_conf['port']}/{db_conf['name']}"
)
# ---------- Initialize Engine and Base ----------
engine = create_engine(DB_URL, echo=False)
Base = declarative_base()
metadata = Base.metadata

# ============================================================
#                        MODELS
# ============================================================

# ---------- Suppliers ----------
class Supplier(Base):
    __tablename__ = "suppliers"

    id = Column(Integer, primary_key=True)
    name = Column(String(100), unique=True, nullable=False)

    products = relationship("Product", back_populates="supplier")


# ---------- Payment Methods ----------
class PaymentMethod(Base):
    __tablename__ = "payment_methods"

    id = Column(Integer, primary_key=True)
    method = Column(String(100), unique=True, nullable=False)

    transactions = relationship("Transaction", back_populates="payment_method")


# ---------- Customers ----------
class Customer(Base):
    __tablename__ = "customers"

    id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False)
    email = Column(String(100), nullable=False)
    registration_date = Column(DateTime, default=func.now())
    country = Column(String(100))

    transactions = relationship("Transaction", back_populates="customer")


# ---------- Products ----------
class Product(Base):
    __tablename__ = "products"

    id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False)
    category = Column(String(100), index=True)
    price = Column(Float, nullable=False)
    supplier_id = Column(Integer, ForeignKey("suppliers.id"), nullable=False)

    supplier = relationship("Supplier", back_populates="products")
    transaction_items = relationship("TransactionItem", back_populates="product")


# ---------- Transactions ----------
class Transaction(Base):
    __tablename__ = "transactions"

    id = Column(Integer, primary_key=True)
    customer_id = Column(Integer, ForeignKey("customers.id"), nullable=False)
    timestamp = Column(DateTime, default=func.now(), index=True)
    payment_method_id = Column(Integer, ForeignKey("payment_methods.id"))

    customer = relationship("Customer", back_populates="transactions")
    payment_method = relationship("PaymentMethod", back_populates="transactions")
    items = relationship("TransactionItem", back_populates="transaction")


# ---------- Transaction Items ----------
class TransactionItem(Base):
    __tablename__ = "transaction_items"

    id = Column(Integer, primary_key=True)
    transaction_id = Column(Integer, ForeignKey("transactions.id"), nullable=False)
    product_id = Column(Integer, ForeignKey("products.id"), nullable=False)
    quantity = Column(Integer, nullable=False)

    # ⚠ Add UNIQUE constraint for upsert
    __table_args__ = (
        UniqueConstraint('transaction_id', 'product_id', name='uix_transaction_product'),
    )

    transaction = relationship("Transaction", back_populates="items")
    product = relationship("Product", back_populates="transaction_items")



# ---------- Indexes ----------
Index("idx_transactions_customer", Transaction.customer_id)
Index("idx_transactions_timestamp", Transaction.timestamp)
Index("idx_transaction_items_product", TransactionItem.product_id)

# ============================================================
#                      INITIALIZATION
# ============================================================

def init_db():
    """Create all tables and indexes in the target PostgreSQL database."""
    print("🚀 Connecting to database...")
    print(f"🔗 {DB_URL}")
    Base.metadata.create_all(engine)
    print("✅ Database schema created successfully!")

# ============================================================
#                        MAIN
# ============================================================

if __name__ == "__main__":
    init_db()
