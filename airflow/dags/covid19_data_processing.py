import csv
import logging

from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.email import EmailOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.utils import timezone

import requests


def _download_covid19_data():
    url = "https://api.covid19api.com/world?from=2021-03-01T00:00:00Z&to=2021-03-02T00:00:00Z"
    response = requests.get(url)
    data = response.json()
    with open("/Users/atb/covid19.csv", "w") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow([data[0]["NewConfirmed"]])
        logging.info("Save COVID-19 data to CSV file successfully")

    return data

def _load_data_to_s3():
    hook = S3Hook(aws_conn_id="aws_s3_conn")
    hook.load_file(
        filename="/Users/atb/covid19.csv",
        key="atb/covid19.csv",
        bucket_name="odds-dataops",
        replace=True,
    )

default_args = {
    "owner": "atb",
    "email": ["atb@odds.team",]
}
with DAG("convid19_data_processing",
         schedule_interval="@daily",
         default_args=default_args,
         start_date=timezone.datetime(2021, 3, 1),
         tags=["covid19", "odds"]) as dag:

    start = DummyOperator(task_id="start")

    download_covid19_data = PythonOperator(
        task_id="download_covid19_data",
        python_callable=_download_covid19_data,
    )

    create_table = SqliteOperator(
        task_id="create_db",
        sqlite_conn_id="sqlite_default",
        sql="""
            CREATE TABLE IF NOT EXISTS covid19 (
                NewConfirmed TEXT NOT NULL
            );
        """
    )

    load_data_to_db = BashOperator(
        task_id="load_data_to_db",
        bash_command="""
            sqlite3 -separator "," /Users/atb/covid19.db ".import /Users/atb/covid19.csv covid19"
        """
    )

    send_mail = EmailOperator(
        task_id="send_email",
        to=["atb@odds.team",],
        subject="Finished loading COVID-19 data to db!",
        html_content="Yeah!",
    )

    load_data_to_s3 = PythonOperator(
        task_id="load_data_to_s3",
        python_callable=_load_data_to_s3,
    )

    end = DummyOperator(task_id="end")

    start >> download_covid19_data >> create_table \
        >> load_data_to_db >> send_mail >> end
    download_covid19_data >> load_data_to_s3
