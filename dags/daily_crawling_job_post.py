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
from psycopg2 import sql
import os
import sys 
import utils.SlackNotification as slack
import utils.smtp as smtp

# module_path = os.path.abspath(os.getcwd())
# if module_path not in sys.path:
#     sys.path.append(module_path)
# Add the directory containing your module to the Python path
# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), 'mymodule')))
   
# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), 'utils')))
import utils.careerviet.crawling_cv_job_post as cv_jp
import utils.careerviet.crawling_cv_employer as cv_emp   
import utils.vietnamwork.crawling_vnw_employer as vnw_emp
import utils.vietnamwork.crawling_vnw_job_post as vnw_jp


# [END import_module]
# Python script

############ CV ############
# employer: crawl => mongodb  
def daily_cv_employer_sitemap():
    cv_emp.daily_employer_sitemap_process()
    
def daily_cv_employer_detail(worker):
    cv_emp.daily_employer_url_generator_airflow(worker)
# employer: mongodb => postgres    
def daily_cv_employer_sitemap_to_postgres():
    cv_emp.daily_employer_sitemap_to_postgres()     
    
def daily_cv_employer_detail_to_postgres():
    cv_emp.daily_load_employer_detail_to_postgres()  

# job post: crawl => mongodb
def daily_cv_job_post_sitemap():
    cv_jp.daily_job_post_sitemap_process()
    
def daily_cv_job_post_detail(worker):
    cv_jp.daily_job_url_generator_airflow(worker)   
    
# job post: mongodb => postgres  
def daily_cv_jp_sitemap_to_postgres():
    cv_jp.daily_job_post_sitemap_to_postgres()     
    
def daily_cv_jp_detail_to_postgres():
    cv_jp.daily_load_job_post_detail_to_postgres()     

############ VNW ############
# employer: crawl => mongodb    
def daily_vnw_employer_sitemap():
    vnw_emp.daily_employer_sitemap_process()
    
def daily_vnw_employer_detail(worker):
    vnw_emp.daily_employer_url_generator_airflow(worker)
    
# employer: mongodb => postgres    
def daily_vnw_employer_sitemap_to_postgres():
    vnw_emp.daily_employer_sitemap_to_postgres()     
    
def daily_vnw_employer_detail_to_postgres():
    vnw_emp.daily_load_employer_detail_to_postgres()   
    
# job post: crawl => mongodb
def daily_vnw_job_post_sitemap():
    vnw_jp.daily_job_post_sitemap_process()
    
def daily_vnw_job_post_detail(worker):
    vnw_jp.daily_job_url_generator_airflow(worker)   
    
# job post: mongodb => postgres  
def daily_vnw_jp_sitemap_to_postgres():
    vnw_jp.daily_job_post_sitemap_to_postgres()     
    
def daily_vnw_jp_detail_to_postgres():
    vnw_jp.daily_load_job_post_detail_to_postgres()  
 
############ VL24H ############
# employer: crawl => mongodb    
def daily_vl24h_employer_sitemap():
    vl24h_emp.daily_employer_sitemap_process()
    
def daily_vl24h_employer_detail(worker):
    vl24h_emp.daily_employer_url_generator_airflow(worker)
    
# employer: mongodb => postgres    
def daily_vl24h_employer_sitemap_to_postgres():
    vl24h_emp.daily_employer_sitemap_to_postgres()     
    
def daily_vl24h_employer_detail_to_postgres():
    vl24h_emp.daily_load_employer_detail_to_postgres()   
    
# job post: crawl => mongodb
def daily_vl24h_job_post_sitemap():
    vl24h_jp.daily_job_post_sitemap_process()
    
def daily_vl24h_job_post_detail(worker):
    vl24h_jp.daily_job_url_generator_airflow(worker)   
    
# job post: mongodb => postgres  
def daily_vl24h_jp_sitemap_to_postgres():
    vl24h_jp.daily_job_post_sitemap_to_postgres()     
    
def daily_vl24h_jp_detail_to_postgres():
    vl24h_jp.daily_load_job_post_detail_to_postgres()  
         
############ CALL BACK ############      
def on_success_callback(context):
    """
    # Callback function to send Slack notification and email when task/DAG succeeds
    """
    # 1. send an email
    email = smtp.EmailSender(sender_email=None, smtp_port=None, sender_password=None, smtp_server=None)
    email.send_email(subject=None, body=None, recipients=None, context = context, is_success=1)
    
    # 2. send noti in slack 
    slack.send_slack_success_message(context)
  
def on_failure_callback(context):
    """
    # Callback function to send Slack notification and email when task/DAG succeeds
    """
    # 1. send an email
    email = smtp.EmailSender(sender_email=None, smtp_port=None, sender_password=None, smtp_server=None)
    email.send_email(subject=None, body=None, recipients=None, context = context, is_success=0)
    
    # 2. send noti in slack 
    slack.send_slack_failure_message(context)
      
# [START instantiate_dag]
with DAG(
    "python_crawling_job_post",
    default_args={ # đưa context vào từng task
        "depends_on_past": False,
        "email": ["airflow@example.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),        
        "on_success_callback": on_success_callback,
        "on_failure_callback": on_failure_callback
    },
    # [END default_args]
    description="A DAG for crawling job post data and loading into DWH",
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["python"],
) as dag:
    # [END instantiate_dag]
    # [START basic_task]
    ######### CV #########
    t_daily_cv_employer_sitemap = PythonOperator(
        task_id="daily_cv_employer_sitemap",
        python_callable=daily_cv_employer_sitemap
    )
    
    t_daily_cv_employer_sitemap_to_postgres = PythonOperator(
        task_id="daily_cv_employer_sitemap_to_postgres",
        python_callable=daily_cv_employer_sitemap_to_postgres
    )
             
    t_daily_cv_employer_detail_to_postgres = PythonOperator(
        task_id="daily_cv_employer_detail_to_postgres",
        python_callable=daily_cv_employer_detail_to_postgres
    )    
    
    t_daily_cv_jp_sitemap = PythonOperator(
        task_id="daily_cv_job_post_sitemap",
        python_callable=daily_cv_job_post_sitemap
    )
        
    t_daily_cv_jp_sitemap_to_postgres = PythonOperator(
        task_id="daily_cv_jp_sitemap_to_postgres",
        python_callable=daily_cv_jp_sitemap_to_postgres
    )
             
    t_daily_cv_jp_detail_to_postgres = PythonOperator(
        task_id="daily_cv_jp_detail_to_postgres",
        python_callable=daily_cv_jp_detail_to_postgres
    )    
    ######### VNW #########     
    t_daily_vnw_employer_sitemap = PythonOperator(
        task_id="daily_vnw_employer_sitemap",
        python_callable=daily_vnw_employer_sitemap
    )
    
    t_daily_vnw_employer_sitemap_to_postgres = PythonOperator(
        task_id="daily_vnw_employer_sitemap_to_postgres",
        python_callable=daily_vnw_employer_sitemap_to_postgres
    )
             
    t_daily_vnw_employer_detail_to_postgres = PythonOperator(
        task_id="daily_vnw_employer_detail_to_postgres",
        python_callable=daily_vnw_employer_detail_to_postgres
    )    
    
    t_daily_vnw_jp_sitemap = PythonOperator(
        task_id="daily_vnw_job_post_sitemap",
        python_callable=daily_vnw_job_post_sitemap
    )
        
    t_daily_vnw_jp_sitemap_to_postgres = PythonOperator(
        task_id="daily_vnw_jp_sitemap_to_postgres",
        python_callable=daily_vnw_jp_sitemap_to_postgres
    )
    
    t_daily_vnw_jp_detail_to_postgres = PythonOperator(
        task_id="daily_vnw_jp_detail_to_postgres",
        python_callable=daily_vnw_jp_detail_to_postgres
    )      
    ######### VL24H #########
    t_daily_vl24h_employer_sitemap = PythonOperator(
        task_id="daily_vl24h_employer_sitemap",
        python_callable=daily_vl24h_employer_sitemap
    )
    
    t_daily_vl24h_employer_sitemap_to_postgres = PythonOperator(
        task_id="daily_vl24h_employer_sitemap_to_postgres",
        python_callable=daily_vl24h_employer_sitemap_to_postgres
    )
             
    t_daily_vl24h_employer_detail_to_postgres = PythonOperator(
        task_id="daily_vl24h_employer_detail_to_postgres",
        python_callable=daily_vl24h_employer_detail_to_postgres
    )    
    
    t_daily_vl24h_jp_sitemap = PythonOperator(
        task_id="daily_vl24h_job_post_sitemap",
        python_callable=daily_vl24h_job_post_sitemap
    )
        
    t_daily_vl24h_jp_sitemap_to_postgres = PythonOperator(
        task_id="daily_vl24h_jp_sitemap_to_postgres",
        python_callable=daily_vl24h_jp_sitemap_to_postgres
    )
    
    t_daily_vl24h_jp_detail_to_postgres = PythonOperator(
        task_id="daily_vl24h_jp_detail_to_postgres",
        python_callable=daily_vl24h_jp_detail_to_postgres
    )
    # [END jinja_template]
    
    #######################################################
    ######################## 1. CV ########################
    #######################################################
    # Create the call_cv_employer_detail tasks for each worker
    for worker in [1,2]:
        call_cv_employer_detail = PythonOperator(
            task_id= f"daily_cv_employer_detail_{worker}",
            python_callable=daily_cv_employer_detail,
            op_kwargs={'worker': worker}
        )
         # Set the task dependencies
        t_daily_cv_employer_sitemap >> call_cv_employer_detail >> t_daily_cv_employer_detail_to_postgres

    # t_daily_cv_employer_sitemap_to_postgres runs in parallel with call_cv_employer_detail tasks
    t_daily_cv_employer_sitemap >> t_daily_cv_employer_sitemap_to_postgres

    # Create the call_cv_jp_detail tasks for each worker
    for worker in [1, 2]:
        call_cv_jp_detail = PythonOperator(
            task_id=f"daily_cv_job_post_detail_{worker}",
            python_callable=daily_cv_job_post_detail,
            op_kwargs={'worker': worker}
        )
         # Set the task dependencies
        t_daily_cv_jp_sitemap >> call_cv_jp_detail >> t_daily_cv_jp_detail_to_postgres
        
    # Ensure t_daily_cv_jp_sitemap_to_postgres runs in parallel with call_cv_jp_detail tasks
    t_daily_cv_jp_sitemap >> t_daily_cv_jp_sitemap_to_postgres
    
    #######################################################
    ####################### 2. VNW ########################
    #######################################################
    # Create the call_vnw_employer_detail tasks for each worker
    for worker in [1,2]:
        call_vnw_employer_detail = PythonOperator(
            task_id= f"daily_vnw_employer_detail_{worker}",
            python_callable=daily_vnw_employer_detail,
            op_kwargs={'worker': worker}
        )
         # Set the task dependencies
        t_daily_vnw_employer_sitemap >> call_vnw_employer_detail >> t_daily_vnw_employer_detail_to_postgres

    # Ensure t_daily_vnw_employer_sitemap_to_postgres runs in parallel with call_vnw_employer_detail tasks
    t_daily_vnw_employer_sitemap >> t_daily_vnw_employer_sitemap_to_postgres
    
    # Create the call_vnw_jp_detail tasks for each worker
    for worker in [1, 2]:
        call_vnw_jp_detail = PythonOperator(
            task_id=f"daily_vnw_job_post_detail_{worker}",
            python_callable=daily_vnw_job_post_detail,
            op_kwargs={'worker': worker}
        )
         # Set the task dependencies
        t_daily_vnw_jp_sitemap >> call_vnw_jp_detail >> t_daily_vnw_jp_detail_to_postgres
        
    # Ensure t_daily_vnw_jp_sitemap_to_postgres runs in parallel with call_vnw_jp_detail tasks
    t_daily_vnw_jp_sitemap >> t_daily_vnw_jp_sitemap_to_postgres
        
    #######################################################
    ####################### 3. VL24H ######################
    #######################################################
    # Create the call_vl24h_employer_detail tasks for each worker
    for worker in [1,2]:
        call_vl24h_employer_detail = PythonOperator(
            task_id= f"daily_vl24h_employer_detail_{worker}",
            python_callable=daily_vl24h_employer_detail,
            op_kwargs={'worker': worker}
        )
         # Set the task dependencies
        t_daily_vl24h_employer_sitemap >> call_vl24h_employer_detail >> t_daily_vl24h_employer_detail_to_postgres

    # Ensure t_daily_vl24h_employer_sitemap_to_postgres runs in parallel with call_vl24h_employer_detail tasks
    t_daily_vl24h_employer_sitemap >> t_daily_vl24h_employer_sitemap_to_postgres
    
    # Create the call_vl24h_jp_detail tasks for each worker
    for worker in [1, 2]:
        call_vl24h_jp_detail = PythonOperator(
            task_id=f"daily_vl24h_job_post_detail_{worker}",
            python_callable=daily_vl24h_job_post_detail,
            op_kwargs={'worker': worker}
        )
         # Set the task dependencies
        t_daily_vl24h_jp_sitemap >> call_vl24h_jp_detail >> t_daily_vl24h_jp_detail_to_postgres
        
    # Ensure t_daily_vl24h_jp_sitemap_to_postgres runs in parallel with call_vl24h_jp_detail tasks
    t_daily_vl24h_jp_sitemap >> t_daily_vl24h_jp_sitemap_to_postgres
    
# [END tutorial]
#noti