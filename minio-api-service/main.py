import io
import os

import uvicorn
from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from minio import Minio
from minio.error import S3Error
from pydantic import BaseModel

app = FastAPI(
    title="Minio API",
    description="Demo FastAPI service for Minio on Shakudo",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

MINIO_ENDPOINT = os.getenv(
    "MINIO_ENDPOINT", "minio.hyperplane-minio.svc.cluster.local:9000"
)
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"

client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=MINIO_SECURE,
)


class BucketCreate(BaseModel):
    name: str


@app.get("/")
def root():
    return {"status": "ok", "service": "Minio API", "version": "1.0.0"}


@app.get("/buckets", summary="List all buckets")
def list_buckets():
    """Return a list of all buckets in Minio."""
    try:
        buckets = client.list_buckets()
        return {
            "buckets": [
                {
                    "name": b.name,
                    "created": b.creation_date.isoformat() if b.creation_date else None,
                }
                for b in buckets
            ]
        }
    except S3Error as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/buckets", status_code=201, summary="Create a new bucket")
def create_bucket(payload: BucketCreate):
    """Create a new bucket. Returns 409 if the bucket already exists."""
    try:
        if client.bucket_exists(payload.name):
            raise HTTPException(
                status_code=409,
                detail=f"Bucket '{payload.name}' already exists",
            )
        client.make_bucket(payload.name)
        return {"message": f"Bucket '{payload.name}' created successfully"}
    except HTTPException:
        raise
    except S3Error as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get(
    "/buckets/{bucket_name}/objects",
    summary="List objects in a bucket",
)
def list_objects(bucket_name: str, prefix: str = ""):
    """Return all objects inside *bucket_name*, optionally filtered by *prefix*."""
    try:
        if not client.bucket_exists(bucket_name):
            raise HTTPException(
                status_code=404, detail=f"Bucket '{bucket_name}' not found"
            )
        objects = client.list_objects(bucket_name, prefix=prefix, recursive=True)
        return {
            "bucket": bucket_name,
            "prefix": prefix,
            "objects": [
                {
                    "name": obj.object_name,
                    "size_bytes": obj.size,
                    "last_modified": (
                        obj.last_modified.isoformat() if obj.last_modified else None
                    ),
                    "etag": obj.etag,
                }
                for obj in objects
            ],
        }
    except HTTPException:
        raise
    except S3Error as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post(
    "/buckets/{bucket_name}/objects",
    status_code=201,
    summary="Upload an object to a bucket",
)
async def upload_object(bucket_name: str, file: UploadFile = File(...)):
    """Upload a file to *bucket_name*. Uses the original filename as the object key."""
    try:
        if not client.bucket_exists(bucket_name):
            raise HTTPException(
                status_code=404, detail=f"Bucket '{bucket_name}' not found"
            )

        content = await file.read()
        content_type = file.content_type or "application/octet-stream"

        client.put_object(
            bucket_name,
            file.filename,
            io.BytesIO(content),
            length=len(content),
            content_type=content_type,
        )

        return {
            "message": f"'{file.filename}' uploaded to bucket '{bucket_name}'",
            "object_name": file.filename,
            "size_bytes": len(content),
            "content_type": content_type,
        }
    except HTTPException:
        raise
    except S3Error as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8787)
