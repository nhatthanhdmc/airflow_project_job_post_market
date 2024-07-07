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

# CV
def cv_job_post_sitemap():
    print('CV - job post sitemap')
    
def cv_job_post_detail():
    print('CV - job post detail')
    
def etl_cv_jp_detail_postgres(worker):
    print("ETL job post detail to postgres")
    
def cv_company_sitemap():
    print('CV - company sitemap')
    
def cv_company_detail():
    print('CV - company detail')    
    
def etl_cv_company_detail_postgres():
    print("ETL company detail to postgres")
    
    
# VNW
def vnw_job_post_sitemap():
    print('VNW - job post sitemap')
    
def vnw_job_post_detail():
    print('VNW - job post detail')
    
def vnw_company_sitemap():
    print('VNW - company sitemap')
    
def vnw_company_detail():
    print('VNW - company detail')
    
# TOPCV
def topcv_job_post_sitemap():
    print('TOPCV - job post sitemap')
    
def topcv_job_post_detail():
    print('TOPCV - job post detail')
    
def topcv_company_sitemap():
    print('TOPCV - company sitemap')
    
def topcv_company_detail():
    print('TOPCV - company detail')
    
# ITVIEC

def itviec_job_post_sitemap():
    print('IT VIEC - job post sitemap')
    
def itviec_job_post_detail():
    print('IT VIEC - job post detail')
    
def itviec_company_sitemap():
    print('IT VIEC - company sitemap')
    
def itviec_company_detail():
    print('IT VIEC - company detail')

    
    
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
    
    t_cv_jp_detail_1 = PythonOperator(
        task_id="cv_job_post_detail_1",
        python_callable=cv_job_post_detail
    )
    
    t_cv_jp_detail_2 = PythonOperator(
        task_id="cv_job_post_detail_2",
        python_callable=cv_job_post_detail
    )
    
    t_etl_cv_jp_detail_postgres = PythonOperator(
        task_id="etl_cv_jp_detail_postgres",
        python_callable=etl_cv_jp_detail_postgres
    )
    
    t_cv_company_sitemap = PythonOperator(
        task_id="cv_company_sitemap",
        python_callable=cv_company_sitemap
    )
    
    t_cv_company_detail_1 = PythonOperator(
        task_id="cv_company_detail_1",
        python_callable=cv_company_detail
    )
    
    t_cv_company_detail_2 = PythonOperator(
        task_id="cv_company_detail_2",
        python_callable=cv_company_detail
    )
    
    t_etl_cv_company_detail_postgres = PythonOperator(
        task_id="etl_cv_company_detail_postgres",
        python_callable=etl_cv_company_detail_postgres
    )
    # VNW
    # t_vnw_jp_sitemap = PythonOperator(
    #     task_id="vnw_job_post_sitemap",
    #     python_callable=vnw_job_post_sitemap
    # )
    
    # t_vnw_jp_detail = PythonOperator(
    #     task_id="vnw_job_post_detail",
    #     python_callable=vnw_job_post_detail
    # )
    
    # t_vnw_company_sitemap = PythonOperator(
    #     task_id="vnw_company_sitemap",
    #     python_callable=vnw_company_sitemap
    # )
    
    # t_vnw_company_detail = PythonOperator(
    #     task_id="vnw_company_detail",
    #     python_callable=vnw_company_detail
    # )
    
    # TOPCV
    # t_topcv_jp_sitemap = PythonOperator(
    #     task_id="topcv_job_post_sitemap",
    #     python_callable=topcv_job_post_sitemap
    # )
    
    # t_topcv_jp_detail = PythonOperator(
    #     task_id="topcv_job_post_detail",
    #     python_callable=topcv_job_post_detail
    # )
    
    # t_topcv_company_sitemap = PythonOperator(
    #     task_id="topcv_company_sitemap",
    #     python_callable=topcv_company_sitemap
    # )
    
    # t_topcv_company_detail = PythonOperator(
    #     task_id="topcv_company_detail",
    #     python_callable=topcv_company_detail
    # )
    
    # ITVIEC
    # t_itviec_jp_sitemap = PythonOperator(
    #     task_id="itviec_job_post_sitemap",
    #     python_callable=itviec_job_post_sitemap
    # )
    
    # t_itviec_jp_detail = PythonOperator(
    #     task_id="itviec_job_post_detail",
    #     python_callable=itviec_job_post_detail
    # )
    
    # t_itviec_company_sitemap = PythonOperator(
    #     task_id="itviec_company_sitemap",
    #     python_callable=itviec_company_sitemap
    # )
    
    # t_itviec_company_detail = PythonOperator(
    #     task_id="itviec_company_detail",
    #     python_callable=itviec_company_detail
    # )   
    
   
    # [END jinja_template]

    t_cv_company_sitemap >> [t_cv_company_detail_1, t_cv_company_detail_2] >> t_etl_cv_company_detail_postgres
    
    # t_vnw_company_sitemap >> t_vnw_company_detail
    # t_topcv_company_sitemap >> t_topcv_company_detail
    # t_itviec_company_sitemap >> t_itviec_company_detail
    
    t_cv_jp_sitemap >> [t_cv_jp_detail_1, t_cv_jp_detail_2] >> t_etl_cv_jp_detail_postgres
    # t_vnw_jp_sitemap >> t_vnw_jp_detail
    # t_topcv_jp_sitemap >> t_topcv_jp_detail
    # t_itviec_jp_sitemap >> t_itviec_jp_detail
    
# [END tutorial]