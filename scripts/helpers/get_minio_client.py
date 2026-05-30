import os
import boto3
from dotenv import load_dotenv
from airflow.models import Variable

load_dotenv()

def get_minio_client():
    return boto3.client(
        "s3",
        endpoint_url = 'http://minio:9000',
        aws_access_key_id = Variable.get("minio_access_key", default_var = os.getenv("AWS_ACCESS_KEY_ID")), # PUT TO .ENV
        aws_secret_access_key = Variable.get("minio_secret_key", default_var = os.getenv("AWS_SECRET_ACCESS_KEY")), # PUT TO .ENV
        region_name = "us-east-1",
        use_ssl = False
    )