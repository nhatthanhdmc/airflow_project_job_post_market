from __future__ import annotations
# [START tutorial]
# [START import_module]
from datetime import datetime, timedelta
from textwrap import dedent
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import pandas as pd 
import chardet
from psycopg2 import sql
import psycopg2
import job_post_crawling.cv.crawling_cv_jobpost_sitemap as cv

# [END import_module]
# Python script
def ex1():
    print("Hello world")   

def read_book_csv(file_name):
    # Detect the encoding of the file
    with open(file_name, 'rb') as f:
        result = chardet.detect(f.read())

    encoding = result['encoding']
    df = pd.read_csv(file_name, 
                    sep=',',
                    encoding=encoding)
    print(df.head(5))

# get postgres connection
def get_connection():
    connection = psycopg2.connect(user="ETL",
                                  password="thanh123",
                                  host="localhost",
                                  port="8000",
                                  database="postgres")
    return connection

# close postgres connection
def close_connection(connection):
    if connection:
        connection.close()

# save to postgres db
def save_to_postgres_db(df):
    connection = get_connection()
    cursor = connection.cursor()
    
    # Convert DataFrame to list of tuples
    data_tuples = [tuple(x) for x in df.to_numpy()]
    
    table_name = 'scraping_book_airflow'
    
    # Define the insert query
    insert_query = sql.SQL("""
        INSERT INTO {table} (book_url, image_url, rating, title, upc, product_type, price_excl_tax, tax, available, num_of_review) \
        VALUES (%s, %s, %s,%s, %s, %s, %s, %s, %s, %s)
    """).format(table=sql.Identifier(table_name))

    # Insert each row of the DataFrame
    cursor.executemany(insert_query, data_tuples)

    # Commit the transaction
    connection.commit()

    # Close the cursor and connection
    cursor.close()
    close_connection(connection)

def import_book(file_name):
    # Detect the encoding of the file
    with open(file_name, 'rb') as f:
        result = chardet.detect(f.read())

    encoding = result['encoding']
    df = pd.read_csv(file_name, 
                    sep=',',
                    encoding=encoding)
    save_to_postgres_db(df)
    
# [START instantiate_dag]
with DAG(
    "python_ex1",
    default_args={
        "depends_on_past": False,
        "email": ["airflow@example.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5)
    },
    # [END default_args]
    description="A simple tutorial DAG",
    schedule=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["python"],
) as dag:
    # [END instantiate_dag]
    # [START basic_task]
    t1 = BashOperator(
        task_id="print_date",
        bash_command="pwd",
    )
    
    t2 = PythonOperator(
        task_id="print_ex1",
        python_callable=ex1,
    ) 
    
    t3 = PythonOperator(
        task_id="print_df",
        python_callable=read_book_csv,
        op_kwargs={'file_name':'/home/thanhnn/airflow_project/dags/book.csv'}
    )
    
    t4 = PythonOperator(
        task_id="import_book",
        python_callable=import_book,
        op_kwargs={'file_name':'/home/thanhnn/airflow_project/dags/book.csv'}
    )
    
    t5 = PythonOperator(
        task_id="cv_job_sitemap",
        python_callable=cv.sitemap_process
    )
   
    # [END jinja_template]

    t1 >> [t2, t3] >> t4 >> t5
# [END tutorial]