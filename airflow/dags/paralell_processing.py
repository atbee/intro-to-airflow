import random
import time

from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.utils import timezone


def _sleep():
    time.sleep(random.randint(1, 10))


default_args = {
    "owner": "atb",
    "email": ["atb@odds.team",]
}
with DAG("paralell_processing",
         schedule_interval="*/10 * * * *",
         default_args=default_args,
         start_date=timezone.datetime(2021, 3, 20),
         tags=["odds", "paralell"]) as dag:

    t1 = PythonOperator(
        task_id="t1",
        python_callable=_sleep,
    )

    t2 = PythonOperator(
        task_id="t2",
        python_callable=_sleep,
    )

    t3 = PythonOperator(
        task_id="t3",
        python_callable=_sleep,
    )

    t4 = PythonOperator(
        task_id="t4",
        python_callable=_sleep,
    )

    t5 = PythonOperator(
        task_id="t5",
        python_callable=_sleep,
    )

    t6 = PythonOperator(
        task_id="t6",
        python_callable=_sleep,
    )

    t7 = PythonOperator(
        task_id="t7",
        python_callable=_sleep,
    )

    t1 >> [t2, t3, t4, t5, t6] >> t7
