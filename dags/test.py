import multiprocessing.pool
import os
import sys 
import json
# module_path = os.path.abspath(os.getcwd())
# if module_path not in sys.path:
#     sys.path.append(module_path)
import requests
from bs4 import BeautifulSoup
from utils.mongodb_connection import MongoDB
from utils import config as cfg
from datetime import date
import re
import time
import multiprocessing

"""
Global variable
"""
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Accept-Encoding": "*",
    "Connection": "keep-alive"
}
mongodb = None

# Get current date in YYYY-MM-DD format
today = date.today().strftime("%Y-%m-%d")  
conn = cfg.mongodb['CRAWLING']
def connect_mongodb():   
    """
    Return a connection to mongodb
    Args: None
    Returns: mongodb
    """      
    mongodb = MongoDB(  dbname = conn['dbname'], 
                        collection_name = conn['cv_job_post_sitemap'],
                        host = conn['host'], 
                        port = conn['port'], 
                        username = conn['username'], 
                        password = conn['password']
                    )
    mongodb.connect()    
    return mongodb
# employer = {
#                 "employer_id": 1,
#                 "employer_name": "nnt"
#             }    

# print(type(employer))

# print(employer["employer_id"])



mongodb = connect_mongodb()    
mongodb.set_collection(conn['cv_job_post_detail'])

filter = {'job_id': '35C10E0B'}

if len(mongodb.select(filter)) > 0:
    # Remove the 'created_date' key from the dictionary
    print('found')
    print(mongodb.select(filter))
else:
    print('new')
    
sample = mongodb.select(filter)