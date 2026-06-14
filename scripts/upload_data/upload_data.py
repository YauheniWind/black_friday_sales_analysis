import os
import logging
import pandas as pd

from pymongo import MongoClient

from airflow.models import Variable

from scripts.helpers.get_minio_client import get_minio_client

logger = logging.getLogger(__name__)

def upload_file_to_s3():
    s3_client = get_minio_client()
    bucket_name = "sales-data"
    local_dir = "/opt/airflow/data/source_s3"
    file_name = os.listdir(local_dir)[0] # Take first file in list
    file_path = os.path.join(local_dir, file_name)
    logger.info(f"""
                File {file_path} in bucket {bucket_name}
                """)
    ############# Upload into bucket #############
    if os.path.isfile(file_path):
        s3_client.upload_file(
            Filename=file_path,
            Bucket=bucket_name,
            Key=file_name
        )
    logger.info(f"""
                Loaded file {file_name}
                """)
    ############# Removing Loaded file #############
    path = os.path.join(local_dir, file_name)
    os.remove(path)
    logger.info(f"""
                File {file_name} has been removed
                """)
    

def upload_mongo():
    cfg = Variable.get("lesson_38_mongo_uri")
    folder = "/opt/airflow/data/source_mongo"
    with MongoClient(cfg) as client:
        db = client["source_db"]
        col = db["black_friday_sales"]
        file = os.listdir(folder)[0] # Take first file in list
        if file.endswith(".csv"):
            file_path = os.path.join(folder, file)
            df = pd.read_csv(file_path)
            ############# Inserting records #############
            if not df.empty:
                inserted = col.insert_many(df.to_dict("records"))
            logger.info(f"Rows inserted: {len(inserted.inserted_ids)}")
            logger.info(f"Inserted: {file}")
            ############# Removing Loaded file #############
            path = os.path.join(folder, file)
            os.remove(path)
            logger.info(f"File {file} has been removed")