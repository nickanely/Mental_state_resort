import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from MSI_2.scripts.quote_loader import load_quote
from MSI_2.scripts.teams_notifier import send_to_teams
from MSI_2.scripts.image_loader import load_image

with DAG(
        dag_id="daily_cheer_up_dag",
        description="A simple DAG to send daily quote with image to Teams - MSI_2",
        start_date=pendulum.datetime(2023, 10, 17),
        schedule_interval="@daily",
        catchup=False,
) as dag:
    start = EmptyOperator(
        task_id="start",
    )

    load_quote_task = PythonOperator(
        task_id="load_quote",
        python_callable=load_quote,
    )

    load_image_task = PythonOperator(
        task_id="load_image",
        python_callable=load_image,
    )

    send_to_teams_task = PythonOperator(
        task_id="send_to_teams",
        python_callable=send_to_teams,
    )

    end = EmptyOperator(
        task_id="end",
    )

start >> [load_quote_task, load_image_task] >> send_to_teams_task >> end
