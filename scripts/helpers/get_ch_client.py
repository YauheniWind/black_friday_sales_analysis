import clickhouse_driver

from airflow.models import Variable

def get_ch_client():

    ch_сredentials = {
        "user": Variable.get("CLICKHOUSE_USER"),
        "password": Variable.get("CLICKHOUSE_PASSWORD"),
        "host": "clickhouse",
        "port": 9000
    }

    return clickhouse_driver.connect(
        host=ch_сredentials["host"],
        port=ch_сredentials["port"],
        user=ch_сredentials["user"],
        password=ch_сredentials["password"]
    )