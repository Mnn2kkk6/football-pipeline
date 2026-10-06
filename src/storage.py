"""Tiện ích S3 / MinIO (boto3)."""
import json
import boto3
from botocore.exceptions import ClientError
from src import config


def get_client():
    return boto3.client(
        "s3",
        endpoint_url=config.S3_ENDPOINT_URL,
        aws_access_key_id=config.AWS_ACCESS_KEY_ID,
        aws_secret_access_key=config.AWS_SECRET_ACCESS_KEY,
    )


def ensure_bucket(client) -> None:
    """Tạo bucket nếu chưa có (thay cho bước minio-init)."""
    try:
        client.head_bucket(Bucket=config.S3_BUCKET)
    except ClientError:
        client.create_bucket(Bucket=config.S3_BUCKET)


def object_exists(client, key: str) -> bool:
    try:
        client.head_object(Bucket=config.S3_BUCKET, Key=key)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
            return False
        raise


def put_json(client, key: str, payload) -> None:
    client.put_object(
        Bucket=config.S3_BUCKET,
        Key=key,
        Body=json.dumps(payload).encode("utf-8"),
        ContentType="application/json",
    )