import boto3
import os
import time
from datetime import datetime
from botocore.exceptions import ClientError
from config.config import load_config

# ---------- Load Config ----------
config = load_config(env=os.environ.get("ENV", "dev"))
AWS_CONFIG = config["aws"]

BUCKET_NAME = AWS_CONFIG["bucket"]
REGION = AWS_CONFIG["region"]
PROFILE = AWS_CONFIG.get("profile", "default")

CSV_PATHS = {
    "customers": "data/raw/customers.csv",
    "products": "data/raw/products.csv",
    "transactions": "data/raw/transactions.csv"
}

MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds

# ---------- Initialize S3 Session ----------
session = boto3.Session(profile_name=PROFILE)
s3_client = session.client("s3", region_name=REGION)
s3_resource = session.resource("s3", region_name=REGION)

# ---------- Helper: Check/Create Bucket ----------
def ensure_bucket_exists(bucket_name):
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        print(f"✅ Bucket '{bucket_name}' already exists.")
    except ClientError as e:
        error_code = e.response["Error"]["Code"]
        if error_code == "404":
            print(f"🪣 Bucket '{bucket_name}' not found. Creating...")
            s3_client.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={"LocationConstraint": REGION},
            )
            print(f"✅ Bucket '{bucket_name}' created.")
        else:
            raise

# ---------- Helper: Enable Versioning ----------
def ensure_versioning_enabled(bucket_name):
    versioning = s3_client.get_bucket_versioning(Bucket=bucket_name)
    if versioning.get("Status") != "Enabled":
        print(f"🌀 Enabling versioning on bucket '{bucket_name}'...")
        s3_client.put_bucket_versioning(
            Bucket=bucket_name,
            VersioningConfiguration={"Status": "Enabled"}
        )
        print("✅ Versioning enabled.")
    else:
        print("✅ Versioning already enabled.")

# ---------- Helper: Build S3 Key ----------
def get_s3_key(file_type, filename):
    now = datetime.utcnow()
    date_path = f"raw/year={now.year}/month={now.month:02d}/day={now.day:02d}/{file_type}/"
    versioned_filename = f"{now.strftime('%H%M%S')}_{filename}"
    return date_path + versioned_filename

# ---------- Upload File with Retry ----------
def upload_file(local_path, file_type):
    filename = os.path.basename(local_path)
    s3_key = get_s3_key(file_type, filename)
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            s3_client.upload_file(local_path, BUCKET_NAME, s3_key)
            print(f"✅ Uploaded {filename} → s3://{BUCKET_NAME}/{s3_key}")
            return s3_key
        except ClientError as e:
            print(f"❌ Attempt {attempt} failed: {e}")
            if attempt < MAX_RETRIES:
                print(f"Retrying in {RETRY_DELAY} seconds...")
                time.sleep(RETRY_DELAY)
            else:
                raise RuntimeError(f"Failed to upload {filename} after {MAX_RETRIES} attempts")

# ---------- Upload All CSVs ----------
def upload_all():
    ensure_bucket_exists(BUCKET_NAME)
    ensure_versioning_enabled(BUCKET_NAME)

    for file_type, path in CSV_PATHS.items():
        if os.path.exists(path):
            upload_file(path, file_type)
        else:
            print(f"⚠️ File not found: {path}")

# ---------- Main ----------
if __name__ == "__main__":
    upload_all()
