from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

def push_data(**kwargs):
    value = "Airflow is awesome!"
    kwargs['ti'].xcom_push(key='message', value=value)
    print("Положил значение в XCom")

def pull_data(**kwargs):
    value = kwargs['ti'].xcom_pull(key='message', task_ids='push_task')
    print(f"Получил из XCom: {value}")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 8, 1),
    'retries': 0,
}

with DAG(
    dag_id='xcom_example',
    default_args=default_args,
    description='Пример передачи данных между задачами через XCom',
    schedule_interval='*/5 * * * *',
    catchup=False,
) as dag:

    push_task = PythonOperator(
        task_id='push_task',
        python_callable=push_data,
        provide_context=True,
    )

    pull_task = PythonOperator(
        task_id='pull_task',
        python_callable=pull_data,
        provide_context=True,
    )

    push_task >> pull_task
