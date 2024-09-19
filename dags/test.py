import requests
import pandas as pd
from io import StringIO
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType

# Define default_args for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 9, 18),  # Adjust as needed
}

# Create the DAG
dag = DAG(
    'google_colab_telecom_data_pipeline',
    default_args=default_args,
    description='A DAG to fetch, transform and load telecom data into Postgres DB',
    schedule_interval='@daily',  # Schedule it based on the requirement
    catchup=False,
)

# Step 1: Fetch Telecom Data from Google Colab (Assuming the data is publicly accessible via a URL)
def fetch_data(**kwargs):
    url = 'https://path-to-google-colab-cron-job-output/data.csv'  # Replace with the real URL
    response = requests.get(url)
    if response.status_code == 200:
        # Push the CSV data to XCom for further processing
        kwargs['ti'].xcom_push(key='csv_data', value=response.text)
    else:
        raise Exception("Failed to fetch data from Google Colab")

# Step 2: Transform the Telecom Data using PySpark
def transform_data(**kwargs):
    # Fetch the CSV data from XCom
    csv_data = kwargs['ti'].xcom_pull(key='csv_data')
    
    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName("Transform Telecom Data") \
        .getOrCreate()
    
    # Define schema for the data
    schema = StructType([
        StructField("call_id", StringType(), True),
        StructField("call_duration", FloatType(), True),
        StructField("data_usage", FloatType(), True),
        StructField("call_type", StringType(), True),
        StructField("location", StringType(), True),
        StructField("call_time", StringType(), True)
    ])
    
    # Convert the CSV data to PySpark DataFrame
    pandas_df = pd.read_csv(StringIO(csv_data))
    spark_df = spark.createDataFrame(pandas_df, schema=schema)
    
    # Transform the data (e.g., filter rows where call duration > 1 minute)
    transformed_df = spark_df.filter(spark_df['call_duration'] > 1)
    
    # Save the transformed data as CSV for loading into Postgres
    transformed_df.write.mode('overwrite').csv('/tmp/transformed_telecom_data.csv', header=True)
    
    # Push the transformed data path to XCom
    kwargs['ti'].xcom_push(key='transformed_data_path', value='/tmp/transformed_telecom_data.csv')

# Step 3: Load Transformed Data into Postgres DB
def load_data_to_postgres(**kwargs):
    # Retrieve the transformed data from XCom
    transformed_data_path = kwargs['ti'].xcom_pull(key='transformed_data_path')
    
    # Initialize Spark Session to read the transformed data
    spark = SparkSession.builder \
        .appName("Load Telecom Data to Postgres") \
        .getOrCreate()
    
    # Read the transformed data
    transformed_df = spark.read.option('header', 'true').csv(transformed_data_path)
    
    # Define the Postgres connection parameters
    jdbc_url = "jdbc:postgresql://<db_host>:<db_port>/<db_name>"  # Replace with actual values
    connection_properties = {
        "user": "<db_user>",  # Replace with your DB username
        "password": "<db_password>",  # Replace with your DB password
        "driver": "org.postgresql.Driver"
    }
    
    # Load the data into the Postgres table
    transformed_df.write.jdbc(url=jdbc_url, table="telecom_data", mode="append", properties=connection_properties)

# Define the tasks in the DAG
fetch_data_task = PythonOperator(
    task_id='fetch_data',
    python_callable=fetch_data,
    provide_context=True,
    dag=dag
)

transform_data_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    provide_context=True,
    dag=dag
)

load_data_task = PythonOperator(
    task_id='load_data_to_postgres',
    python_callable=load_data_to_postgres,
    provide_context=True,
    dag=dag
)

# Set task dependencies
fetch_data_task >> transform_data_task >> load_data_task
