import csv
import uuid
from datetime import datetime

import requests
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, task
from airflow.sdk.bases.sensor import PokeReturnValue

USER_API_URL = "https://raw.githubusercontent.com/marclamberti/datasets/refs/heads/main/fakeuser.json"
DB_TARGET_TABLE = "bwat_users"


sql = f"""
    CREATE TABLE IF NOT EXISTS {DB_TARGET_TABLE} (
        id SERIAL PRIMARY KEY,
        uid VARCHAR(255) UNIQUE NOT NULL,
        first_name VARCHAR(255),
        last_name VARCHAR(255),
        email VARCHAR(255),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    ); 
"""

@dag
def user_processing():
    create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id="postgres",
        sql=sql,
    )

    @task.sensor(poke_interval=30, timeout=300)
    def is_api_available() -> PokeReturnValue:
        response = requests.get(USER_API_URL)
        print(response.status_code)
        
        condition = False
        fake_user = None
        if response.status_code == 200:
            condition = True
            fake_user = response.json()
        return PokeReturnValue(is_done=condition, xcom_value=fake_user)


    @task
    def extract_user(fake_user):
        return {
            "uid": str(uuid.uuid4()),
            "firstname": fake_user["personalInfo"]["firstName"],
            "lastname": fake_user["personalInfo"]["lastName"],
            "email": fake_user["personalInfo"]["email"],
        }
    
    @task
    def process_user(user_info: dict):
        user_info["created_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        file_path = "/tmp/user_info.csv"
        with open(file_path, mode='w', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=user_info.keys())
            writer.writeheader()
            writer.writerow(user_info)
    
    @task
    def store_user():
        hook = PostgresHook(postgres_conn_id="postgres")
        hook.copy_expert(
            sql=f"COPY {DB_TARGET_TABLE} (uid, first_name, last_name, email, created_at) FROM STDIN WITH CSV HEADER;",
            filename="/tmp/user_info.csv",
        )

    # fake_user = is_api_available()
    # user_info = extract_user(fake_user)
    # process_user(user_info)
    # store_user()

    process_user(extract_user(create_table >> is_api_available())) >> store_user()


user_processing()
