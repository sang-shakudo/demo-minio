import io
import os
from datetime import datetime

from minio import Minio
from minio.error import S3Error

MINIO_ENDPOINT = os.getenv(
    "MINIO_ENDPOINT", "minio.hyperplane-minio.svc.cluster.local:9000"
)
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"
OUTPUT_BUCKET = os.getenv("OUTPUT_BUCKET", "reports")
LOCAL_REPORT_FILE = "bucket-objects-report.txt"

client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=MINIO_SECURE,
)


def build_report() -> str:
    lines = [
        "Minio Inventory Report",
        f"Generated: {datetime.utcnow().isoformat()}",
        "=" * 60,
        "",
    ]

    for bucket in client.list_buckets():
        lines.append(f"BUCKET: {bucket.name}")
        lines.append(
            f"  Created: {bucket.creation_date.isoformat() if bucket.creation_date else 'unknown'}"
        )
        lines.append("  Objects:")

        try:
            objects = list(client.list_objects(bucket.name, recursive=True))
            if objects:
                for obj in objects:
                    lines.append(f"    - {obj.object_name}  ({obj.size} bytes)")
            else:
                lines.append("    (empty)")
        except S3Error as e:
            lines.append(f"    Error: {e}")

        lines.append("")

    return "\n".join(lines)


def ensure_bucket(name: str):
    if not client.bucket_exists(name):
        client.make_bucket(name)
        print(f"Created bucket: {name}")


def upload_text(content: str, bucket: str, object_name: str):
    data = content.encode("utf-8")
    client.put_object(
        bucket,
        object_name,
        io.BytesIO(data),
        length=len(data),
        content_type="text/plain",
    )
    print(f"Uploaded → s3://{bucket}/{object_name}")


def main():
    print(f"Connecting to Minio at {MINIO_ENDPOINT} ...")
    report = build_report()
    print(report)

    with open(LOCAL_REPORT_FILE, "w") as f:
        f.write(report)
    print(f"Saved locally: {LOCAL_REPORT_FILE}")

    ensure_bucket(OUTPUT_BUCKET)
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    remote_name = f"reports/{timestamp}_{LOCAL_REPORT_FILE}"
    upload_text(report, OUTPUT_BUCKET, remote_name)
    print(f"\nDone. Report is at s3://{OUTPUT_BUCKET}/{remote_name}")


if __name__ == "__main__":
    main()
