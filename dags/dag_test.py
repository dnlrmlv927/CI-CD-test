from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import time
#просто заглушка

# Функции для задач
def print_hello():
    print("Hello, Airflow!")

def sleep_task():
    time.sleep(5)

def print_done():
    print("DAG выполнен успешно!")

# Определение DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 2, 24),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="test_dag",
    default_args=default_args,
    schedule_interval="@daily",  # Запуск раз в день
    catchup=False,
) as dag:

    task1 = PythonOperator(
        task_id="say_hello",
        python_callable=print_hello
    )

    task2 = PythonOperator(
        task_id="sleep",
        python_callable=sleep_task
    )

    task3 = PythonOperator(
        task_id="done",
        python_callable=print_done
    )

    # Определяем порядок выполнения
    task1 >> task2 >> task3
