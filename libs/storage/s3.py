import os
import boto3

def s3_client():
    return boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_DEFAULT_REGION"),
    )

def s3_bucket() -> str:
    bucket = os.getenv("S3_BUCKET")
    if not bucket:
        raise RuntimeError("S3_BUCKET is not set")
    return bucket
