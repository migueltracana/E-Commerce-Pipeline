# src/scripts/reset_db.py
from src.scripts.db_setup import Base, engine

if __name__ == "__main__":
    print("⚠️ Dropping all tables...")
    Base.metadata.drop_all(engine)
    print("🧱 Recreating all tables...")
    Base.metadata.create_all(engine)
    print("✅ Database reset complete.")
