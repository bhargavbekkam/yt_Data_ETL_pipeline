from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from pyspark.sql import SparkSession
import sys
import os

aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")


sys.path.append('/home/ubuntu/airflow/scripts')
from data_pipeline_utils import (
    load_csv_json_from_s3,
    transform_csv_json_data,
    write_processed_data_to_s3,
    load_parquet_to_redshift
)

BUCKET_NAME = "youtube-trending-data-bucket"
PROCESSED_PARQUET_PATH = f"s3a://{BUCKET_NAME}/processed/parquet_output/"
REDSHIFT_TABLE = "public.youtube_trending_videos"
REDSHIFT_JDBC_URL = "jdbc:redshift://redshift-cluster-1.abcd1234.us-west-2.redshift.amazonaws.com:5439/dev"
REDSHIFT_USER = "admin_user"
REDSHIFT_PASSWORD = "bhargavBEkkam2"

TEMP_S3_DIR = f"s3a://{BUCKET_NAME}/redshift_temp/"

spark = SparkSession.builder \
    .appName("Airflow_Spark_ETL") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.access.key", "aws_access_key_id") \
    .config("spark.hadoop.fs.s3a.secret.key", "aws_secret_access_key") \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .getOrCreate()

csv_data_df = None
json_data_df = None
complete_df = None

def task_load():
    global csv_data_df, json_data_df
    csv_data_df, json_data_df = load_csv_json_from_s3(BUCKET_NAME, spark)

def task_transform():
    global complete_df
    complete_df = transform_csv_json_data(csv_data_df, json_data_df)

def task_write():
    write_processed_data_to_s3(complete_df, BUCKET_NAME)

def task_load_to_redshift():
    load_parquet_to_redshift(
        spark=spark,
        s3_parquet_path=PROCESSED_PARQUET_PATH,
        redshift_table=REDSHIFT_TABLE,
        jdbc_url=REDSHIFT_JDBC_URL,
        redshift_user=REDSHIFT_USER,
        redshift_password=REDSHIFT_PASSWORD,
        temp_s3_dir=TEMP_S3_DIR
    )

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1
}

dag = DAG(
    dag_id='spark_s3_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline for processing YouTube trending data from S3 using Spark and loading to Redshift',
    schedule_interval=None,
    catchup=False
)

load_task = PythonOperator(
    task_id='load_data_from_s3',
    python_callable=task_load,
    dag=dag
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=task_transform,
    dag=dag
)

write_task = PythonOperator(
    task_id='write_data_to_s3',
    python_callable=task_write,
    dag=dag
)

redshift_task = PythonOperator(
    task_id='load_data_to_redshift',
    python_callable=task_load_to_redshift,
    dag=dag
)

load_task >> transform_task >> write_task >> redshift_task
