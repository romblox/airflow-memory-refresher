from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.sdk import dag, task


# from airflow.operators.dummy import DummyOperator


@dag(
    dag_id='s3_access_check_on_default_aws_connection',
    description='Check access to S3 bucket on default AWS connection',
    tags=['s3', 'access', 'check'],
)
def s3_access_check_on_default_aws_connection():
    @task
    def start_check():
        print("Start checking S3 access")

    check_s3_access = S3KeySensor(
        task_id='check_s3_access',
        bucket_key='data_sources/',
        bucket_name='s3-shkiper-private',
        aws_conn_id='aws-free-tier',
        # dag=dag,
    )

    @task
    def end_check():
        print("Finished checking S3 access")

    start_check() >> check_s3_access >> end_check()


s3_access_check_on_default_aws_connection()
