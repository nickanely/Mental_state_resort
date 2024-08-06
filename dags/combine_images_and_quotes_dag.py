import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from agile_project.scripts.MSI_6.image_loader import load_image
from agile_project.scripts.MSI_6.quote_loader import load_quote
from agile_project.scripts.MSI_6.teams_notifier import send_to_teams
from agile_project.scripts.MSI_6.display_quote_on_image import display_quote_on_image


with DAG(
        dag_id="combine_images_and_quotes_dag",
        description="A simple DAG to send daily quote with image to Teams (Quotes are ON images)",
        start_date=pendulum.datetime(2023, 10, 17),
        schedule_interval="@daily",
        catchup=False,
) as dag:
    start = EmptyOperator(
        task_id="start"
    )

    load_quote_task = PythonOperator(
        task_id="load_quote",
        python_callable=load_quote,
    )

    load_image_task = PythonOperator(
        task_id="load_image",
        python_callable=load_image,
    )

    write_on_image_task = PythonOperator(
        task_id="write_on_image",
        python_callable=display_quote_on_image,
    )

    send_to_teams_task = PythonOperator(
        task_id="send_to_teams",
        python_callable=send_to_teams,
    )

    end = EmptyOperator(
        task_id="end"
    )

start >> [load_quote_task, load_image_task] >> write_on_image_task >> send_to_teams_task >> end
