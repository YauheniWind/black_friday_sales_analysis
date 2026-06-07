import os
import logging
import pandas as pd
from datetime import datetime, timezone
from pymongo import MongoClient


from airflow.models import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

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
            print(f"Inserted: {file}")
            ############# Removing Loaded file #############
            path = os.path.join(folder, file)
            os.remove(path)
            print(f"File {file} has been removed")

with DAG(
    dag_id = 'upload_to_mongo',
    description='Upload the data to MongoDB',
    schedule_interval=None,
    start_date=datetime(2026, 5, 30),
    catchup=False,
    tags=['data_upload'],
) as dag:
    START = EmptyOperator(task_id = "START")

    UPLOAD_DATA_MONGO = PythonOperator(
        task_id = "UPLOAD_DATA_MONGO",
        python_callable = upload_mongo
    )

    END = EmptyOperator(task_id = "END")

    START >> UPLOAD_DATA_MONGO >> END