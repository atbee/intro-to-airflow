import csv
import logging

import requests
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.email import EmailOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.utils import timezone


def _download_covid19_data(**kwargs):
    ds = kwargs["ds"]
    yesterday_ds = kwargs["yesterday_ds"]
    # from airflow import macros
    # yesterday_ds = macros.ds_add(ds, -1)

    domain = "https://api.covid19api.com"
    url = f"{domain}/world?from={yesterday_ds}T00:00:00Z&to={ds}T00:00:00Z"
    response = requests.get(url)
    data = response.json()
    with open(f"/Users/atb/covid19-{ds}.csv", "w") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow([data[0]["NewConfirmed"]])
        logging.info("Save COVID-19 data to CSV file successfully")

    return data

def _load_data_to_s3(**kwargs):
    ds = kwargs["ds"]

    hook = S3Hook(aws_conn_id="aws_s3_conn")
    hook.load_file(
        filename=f"/Users/atb/covid19-{ds}.csv",
        key=f"atb/covid19-{ds}.csv",
        bucket_name="odds-dataops",
        replace=True,
    )

default_args = {
    "owner": "atb",
    "email": ["atb@odds.team",]
}
with DAG("covid19_data_processing",
         schedule_interval="@daily",
         default_args=default_args,
         start_date=timezone.datetime(2021, 3, 1),
         tags=["covid19", "odds"]) as dag:

    start = DummyOperator(task_id="start")

    print_prev_ds = BashOperator(
        task_id="print_prev_ds",
        bash_command="echo {{ prev_ds }}",
    )

    check_api = HttpSensor(
        task_id="check_api",
        endpoint="world",
        response_check=lambda response: True if len(response.json()) > 0 else False,
    )

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
            sqlite3 -separator "," /Users/atb/covid19-{{ ds }}.db ".import /Users/atb/covid19-{{ ds }}.csv covid19"
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

    start >> print_prev_ds >> check_api >> download_covid19_data \
        >> create_table >> load_data_to_db >> send_mail >> end
    download_covid19_data >> load_data_to_s3
