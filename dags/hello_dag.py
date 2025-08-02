from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator  # <= не забудь старый стиль импорта

def say_hello():
    print("Hello, Airflow!")

def say_goodbye():
    print("Goodbye, Airflow!")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 8, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='hello_and_goodbye',
    default_args=default_args,
    description='DAG с двумя задачами и зависимостью A -> B',
    schedule_interval='*/5 * * * *',
    catchup=False,
) as dag:
    
    task_hello = PythonOperator(
        task_id='say_hello_task',
        python_callable=say_hello,
    )

    task_goodbye = PythonOperator(
        task_id='say_goodbye_task',
        python_callable=say_goodbye,
    )

    task_hello >> task_goodbye  # Зависимость: A → B
