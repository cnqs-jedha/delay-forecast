
import boto3
from dotenv import load_dotenv
import os
import json

load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

def send_to_S3(data, file_name):
    session = boto3.Session(aws_access_key_id=AWS_ACCESS_KEY_ID, 
                        aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

    s3 = session.client("s3")

    json_data = json.dumps(data, ensure_ascii=False)

    s3.put_object(
        Bucket="jedha-lead-bus-delays",
        Key=f"transport/{file_name}.json",
        Body=json_data.encode("utf-8"),
        ContentType="application/json"
    )
