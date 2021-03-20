import json

import requests
from airflow.models import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.email import EmailOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils import timezone


def _download_exchange_rates_data():
    url = "https://api.exchangeratesapi.io/2021-03-19"
    response = requests.get(url)
    data = response.json()
    with open("/Users/atb/exchange_rates.json", "w") as jsonfile:
        json.dump(data, jsonfile)
    return data


def _load_data_to_s3():
    hook = S3Hook(aws_conn_id="aws_s3_conn")
    hook.load_file(
        filename="/Users/atb/exchange_rates.json",
        key="atb/exchange_rates.json",
        bucket_name="odds-dataops",
        replace=True,
    )


default_args = {
    "owner": "logi",
    "email": ["atb@odds.team", ],
}
with DAG("exhange_rates_data_processing",
         schedule_interval="@daily",
         default_args=default_args,
         start_date=timezone.datetime(2021, 3, 19),
         tags=["exchange_rates", "odds"]) as dag:

    start = DummyOperator(task_id='start')

    download_exchange_rates_data = PythonOperator(
        task_id="download_exchange_rates_data",
        python_callable=_download_exchange_rates_data,
    )

    load_data_to_s3 = PythonOperator(
        task_id="load_data_to_s3",
        python_callable=_load_data_to_s3,
    )

    send_email = EmailOperator(
         task_id="send_email",
         to=["atb@odds.team",],
         subject="Finished loading exchange rates data to s3",
         html_content="Logi Yeah!",
    )

    end = DummyOperator(task_id="end")

    start >> download_exchange_rates_data >> load_data_to_s3 \
        >> send_email >> end
