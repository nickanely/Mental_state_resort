import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from agile_project.scripts.MSI_4.image_loader import load_image
from agile_project.scripts.MSI_4.quote_loader import load_quote
from agile_project.scripts.MSI_4.teams_notifier import send_to_teams

with DAG(
        dag_id="MSI_4_send_toads_on_wednesday_dag",
        description="A simple DAG to send a toad image with a quote on Wednesdays",
        start_date=pendulum.datetime(2023, 10, 18),
        schedule_interval="0 10 * * 3",
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
