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
import os
import sys 
# module_path = os.path.abspath(os.getcwd())
# if module_path not in sys.path:
#     sys.path.append(module_path)
# Add the directory containing your module to the Python path
# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), 'mymodule')))
   
# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), 'utils')))
import utils.careerviet.crawling_cv_job_post as cv_jp
import utils.careerviet.crawling_cv_employer as cv_emp   


# [END import_module]
# Python script

# CV
def cv_job_post_sitemap():
    cv_jp.job_post_sitemap_process()
    
def cv_job_post_detail(worker):
    cv_jp.job_url_generator_airflow(worker)
    
def etl_cv_jp_detail_postgres():
    print("ETL job post detail to postgres")
    
def cv_employer_sitemap():
    cv_emp.employer_sitemap_process()
    
def cv_employer_detail(worker):
    cv_emp.employer_url_generator_airflow(worker)
    
def daily_cv_employer_sitemap_to_postgres():
    cv_emp.daily_employer_sitemap_to_postgres()     
    
def daily_cv_employer_detail_to_postgres():
    cv_emp.daily_employer_detail_into_postgres()     
    
# [START instantiate_dag]
with DAG(
    "python_crawling_job_post",
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
    # CV
    t_cv_jp_sitemap = PythonOperator(
        task_id="cv_job_post_sitemap",
        python_callable=cv_job_post_sitemap
    )
    
    t_etl_cv_jp_detail_postgres = PythonOperator(
        task_id="etl_cv_jp_detail_postgres",
        python_callable=etl_cv_jp_detail_postgres
    )    
        
    t_cv_employer_sitemap = PythonOperator(
        task_id="cv_employer_sitemap",
        python_callable=cv_employer_sitemap
    )
    
    t_daily_cv_employer_detail_to_postgres = PythonOperator(
        task_id="daily_cv_employer_detail_to_postgres",
        python_callable=daily_cv_employer_detail_to_postgres
    )
             
    t_daily_cv_employer_detail_to_postgres = PythonOperator(
        task_id="daily_cv_employer_detail_to_postgres",
        python_callable=daily_cv_employer_detail_to_postgres
    )
    
    # [END jinja_template]

    for worker in [1,2]:
        call_employer_detail = PythonOperator(
            task_id= f"cv_employer_detail_{worker}",
            python_callable=cv_employer_detail,
            op_kwargs={'worker': worker}
        )
        t_cv_employer_sitemap >> call_employer_detail >> t_daily_cv_employer_detail_to_postgres 
    
    
    for worker in [1, 2]:
        call_jp_detail = PythonOperator(
            task_id=f"cv_job_post_detail_{worker}",
            python_callable=cv_job_post_detail,
            op_kwargs={'worker': worker}
        )
        t_cv_jp_sitemap >> call_jp_detail >> t_etl_cv_jp_detail_postgres
    
# [END tutorial]