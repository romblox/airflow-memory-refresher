from datetime import datetime
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator, S3FileTransformOperator
from typing import List
import os

from constants import DAGS_DIR


@dag(
    dag_id='s3_to_s3_copy_with_transformation_example_close_to_task',
    description='DAG to download multiple files from S3, zip them, and upload to another bucket',
    schedule='@daily',
    start_date=datetime(2025, 7, 1),
    catchup=False,
    tags=['s3', 'zip', 'transform'],
)
def s3_to_s3_copy_with_transformation_example_close_to_task():
    """
    DAG to:
    1. List files in source S3 bucket
    2. Download each file, zip it, and upload to destination bucket
    """

    # Source and destination bucket configuration
    SOURCE_BUCKET = 's3-shkiper-private'
    DEST_BUCKET = 's3-shkiper-private-destination'
    PREFIX = 'images/'  # Folder in source bucket to process
    DEST_PREFIX = 'zipped/'  # Folder in destination bucket for zip files

    # List files in the source bucket
    list_files = S3ListOperator(
        task_id='list_s3_files',
        bucket=SOURCE_BUCKET,
        prefix=PREFIX,
        aws_conn_id='aws-free-tier',
    )

    @task
    def prepare_file_paths(s3_files: List[str]) -> List[dict]:
        """
        Prepare source and destination paths for each file
        """

        print("List files to process on source bucket:", s3_files)

        file_paths = []

        for s3_object in s3_files:
            if s3_object.endswith('/'):  # Skip folders
                continue

            # Extract filename from the path
            filename = os.path.basename(s3_object)

            file_info = {
                'source_key': f"s3://{SOURCE_BUCKET}/{s3_object}",
                'dest_key': f"s3://{DEST_BUCKET}/{DEST_PREFIX}{filename}.zip",
                'filename': filename
            }
            file_paths.append(file_info)

        print(f"Found {len(file_paths)} files to process:", file_paths)
        return file_paths

    # Create dynamic tasks for each file
    @task
    def process_files(file_paths: List[dict]) -> None:
        """
        Create a transformation task for each file
        """
        for i, file_info in enumerate(file_paths):
            # Create a transformation task for each file
            zip_transform = S3FileTransformOperator(
                task_id=f"zip_transform_{i}",
                source_s3_key=file_info['source_key'],
                dest_s3_key=file_info['dest_key'],
                transform_script=str(DAGS_DIR / 'zip_transform.py'),
                script_args=[file_info['filename']],
                replace=True,
                source_aws_conn_id='aws-free-tier',
                dest_aws_conn_id='aws-free-tier',
            )

            # Execute the task
            zip_transform.execute(context={})

    @task
    def confirm_completion(file_paths: List[dict]) -> str:
        """Confirm all files have been processed"""
        print(f"Successfully processed {len(file_paths)} files")
        return "All files processed and transferred successfully"

    # Set up the task dependencies
    file_paths = prepare_file_paths(list_files.output)
    process_result = process_files(file_paths)
    confirmation = confirm_completion(file_paths)

    # Define the workflow
    list_files >> file_paths >> process_result >> confirmation

# Instantiate the DAG
s3_to_s3_copy_with_transformation_example_close_to_task()