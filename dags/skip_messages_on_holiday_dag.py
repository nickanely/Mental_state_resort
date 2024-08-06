import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator

from agile_project.scripts.MSI_7.check_skip_date import check_skip_date
from agile_project.scripts.MSI_7.image_loader import load_image
from agile_project.scripts.MSI_7.quote_loader import load_quote
from agile_project.scripts.MSI_7.teams_notifier import send_to_teams

with DAG(
        dag_id="skip_messages_on_holiday_dag",
        description="A simple DAG to send daily quote with image to Teams - skip holidays",
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

    check_skip_date_task = BranchPythonOperator(
        task_id="check_skip_date",
        python_callable=check_skip_date,
    )

    send_to_teams_task = PythonOperator(
        task_id="send_to_teams",
        python_callable=send_to_teams,
    )
    skip_message = EmptyOperator(
        task_id="skip_message",
    )
    end = EmptyOperator(
        task_id="end",
    )

start >> [load_quote_task, load_image_task] >> check_skip_date_task
check_skip_date_task >> [send_to_teams_task, skip_message]
send_to_teams_task >> end
skip_message >> end
