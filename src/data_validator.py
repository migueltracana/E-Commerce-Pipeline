import os
import re
import pandas as pd
import logging
from datetime import datetime

# --- Setup Directories ---
DATA_DIR = "data/raw"
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

# --- Configure Logging ---
logging.basicConfig(
    filename=os.path.join(LOG_DIR, "validation.log"),
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# --- Helper Functions ---
def log(msg):
    print(msg)
    logging.info(msg)

def validate_email(email):
    """Check if an email address is valid."""
    pattern = r"^[\w\.-]+@[\w\.-]+\.\w+$"
    return re.match(pattern, str(email)) is not None

def validate_date(date_str):
    """Check if date is in valid ISO format (YYYY-MM-DD or with time)."""
    try:
        datetime.fromisoformat(str(date_str))
        return True
    except ValueError:
        return False

# --- Validation Functions ---
def check_nulls(df, name):
    null_counts = df.isnull().sum()
    if null_counts.any():
        log(f"[{name}] ⚠️ Null values found:\n{null_counts[null_counts > 0]}")
    else:
        log(f"[{name}] ✅ No null values detected.")

def check_email_format(df, name):
    if "email" in df.columns:
        invalid_emails = df[~df["email"].apply(validate_email)]
        if not invalid_emails.empty:
            log(f"[{name}] ⚠️ Invalid email formats found: {len(invalid_emails)}")
        else:
            log(f"[{name}] ✅ All email formats valid.")

def check_positive_prices(df, name):
    if "price" in df.columns:
        invalid_prices = df[df["price"] <= 0]
        if not invalid_prices.empty:
            log(f"[{name}] ⚠️ Non-positive prices found: {len(invalid_prices)}")
        else:
            log(f"[{name}] ✅ All prices are positive.")

def check_date_formats(df, name):
    date_columns = [col for col in df.columns if "date" in col or "timestamp" in col]
    for col in date_columns:
        invalid_dates = df[~df[col].apply(validate_date)]
        if not invalid_dates.empty:
            log(f"[{name}] ⚠️ Invalid date formats in column '{col}': {len(invalid_dates)}")
        else:
            log(f"[{name}] ✅ All dates valid in '{col}'.")

# --- Main Validation Process ---
def validate_file(file_path):
    name = os.path.basename(file_path)
    try:
        df = pd.read_csv(file_path)
        log(f"\n🔍 Validating {name} ({len(df)} records)")
        check_nulls(df, name)
        check_email_format(df, name)
        check_positive_prices(df, name)
        check_date_formats(df, name)
    except Exception as e:
        log(f"[{name}] ❌ Validation failed: {e}")

def main():
    log("=== DATA VALIDATION STARTED ===")

    for filename in os.listdir(DATA_DIR):
        if filename.endswith(".csv"):
            validate_file(os.path.join(DATA_DIR, filename))

    log("=== DATA VALIDATION COMPLETED ===\n")

if __name__ == "__main__":
    main()
