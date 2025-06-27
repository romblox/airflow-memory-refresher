from airflow import DAG
from airflow.decorators import task
from datetime import datetime


with DAG(
    dag_id="dynamic_dag_get_price_google",
    start_date=datetime(2025, 6, 27),
    schedule="@weekly",
    catchup=False,
) as dag:
    @task
    def extract(stock):
        print("Extracting stock ...")
        return stock

    @task
    def process(stock):
        print("Processing stock ...")
        return stock


    @task
    def send_email(stock):
        print(f"Sending email with stock: {stock}")
        return stock

    send_email(process(extract("3422")))