import csv
import os
import time
import random
from datetime import datetime

from dotenv import load_dotenv
from google.cloud import storage


def require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required env var: {name}")
    return value


def main():
    load_dotenv()

    # Auth
    creds_path = require_env("GOOGLE_APPLICATION_CREDENTIALS")
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = creds_path

    bucket_name = require_env("GCS_BUCKET")
    gcs_prefix = os.getenv("GCS_PREFIX", "raw/pi")

    local_dir = os.getenv("LOCAL_DIR", "data")
    rows_per_minute = int(os.getenv("ROWS_PER_MINUTE", "1"))

    # Same file always
    local_file = os.path.join(local_dir, "orders.csv")
    gcs_object = f"{gcs_prefix}/orders.csv"

    os.makedirs(local_dir, exist_ok=True)

    client = storage.Client()
    bucket = client.bucket(bucket_name)

    # Create file with header only once (optional)
    if not os.path.exists(local_file):
        with open(local_file, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["order_id", "country", "amount", "event_time"])

    print("Appending to SAME file and uploading every minute...")
    print(f"Local file: {local_file}")
    print(f"GCS object: gs://{bucket_name}/{gcs_object}")
    print("Press Ctrl+C to stop.")

    while True:
        # Append new rows
        with open(local_file, "a", newline="") as f:
            writer = csv.writer(f)
            for _ in range(rows_per_minute):
                writer.writerow([
                    random.randint(100000, 999999),
                    random.choice(["IN", "US", "UK", "DE"]),
                    random.randint(100, 5000),
                    datetime.utcnow().isoformat()
                ])

        # Upload the same file again (overwrite object in GCS)
        blob = bucket.blob(gcs_object)
        blob.upload_from_filename(local_file)

        print(f"Updated & uploaded: {datetime.utcnow().isoformat()}")
        time.sleep(60)


if __name__ == "__main__":
    main()