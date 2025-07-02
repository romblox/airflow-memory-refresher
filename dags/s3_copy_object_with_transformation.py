from datetime import datetime
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.operators.s3 import S3CopyObjectOperator


@dag(
    dag_id='s3_copy_with_transformation_example',
    description='A DAG to copy an object from one S3 bucket to another',
    schedule='@daily',
    start_date=datetime(2025, 7, 1),
    catchup=False,
    tags=['example', 's3'],
    # default_args={
    #     'owner': 'airflow',
    #     'depends_on_past': False,
    #     'email_on_failure': False,
    #     'email_on_retry': False,
    #     'retries': 1,
    #     'retry_delay': timedelta(minutes=5),
    # },
)
def s3_copy_with_transformation_example():
    """DAG to copy files between S3 buckets with transformation"""

    # Example of a task before copy operation
    @task
    def prepare_for_copy():
        """Prepare for the copy operation"""
        print("Preparing to copy the S3 object")
        return "Ready to copy"

    # # Create the S3CopyObjectOperator task
    # # https://github.com/apache/airflow/blob/b808dd8d82d5407da31fd6085c403dbb3b0fa3c1/providers/amazon/tests/system/amazon/aws/example_s3.py#L235
    # copy_object = S3CopyObjectOperator(
    #     task_id='copy_s3_bucket_object_to_another_s3_bucket',
    #     source_bucket_name='s3-shkiper-private',
    #     dest_bucket_name='s3-shkiper-private-destination',
    #     source_bucket_key='images/YYachts-Y7n.jpg',
    #     dest_bucket_key='images/dump/YYachts-Y7n.jpg',
    #     aws_conn_id='aws-free-tier',
    # )

    # Example of a task after copy operation
    @task
    def confirm_copy():
        """Confirm copy operation completed"""
        print("S3 object copy completed successfully")
        return "Copy confirmed"

    # Set up the task dependencies
    preparation = prepare_for_copy()
    confirmation = confirm_copy()

    # Define the workflow
    preparation >> copy_object >> confirmation

# Instantiate the DAG
s3_copy_dag_instance = s3_copy_dag()
