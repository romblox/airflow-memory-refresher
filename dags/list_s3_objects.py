from airflow.sdk import dag, task
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator


@dag
def list_s3_objects():
    list_objects = S3ListOperator(
        task_id="list_objects",
        bucket="s3-shkiper-private",
        prefix="images/",
        delimiter="/",
        aws_conn_id="aws-free-tier",
    )

    @task
    def print_objects(objects):
        print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>")
        for obj in objects:
            print(obj)
        print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>")
        return objects

    print_objects(list_objects.output)


list_s3_objects()
