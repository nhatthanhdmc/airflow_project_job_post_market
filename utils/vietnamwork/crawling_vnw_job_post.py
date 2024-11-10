"""sitemap job post của vnw chuẩn XML nên có thể dùng thư viện xml.etree.ElementTree
"""
import multiprocessing.pool
import os
import sys 
module_path = os.path.abspath(os.getcwd())
if module_path not in sys.path:
    sys.path.append(module_path)
import requests
import pandas as pd
from bs4 import BeautifulSoup
import xml.etree.ElementTree as ET
from utils.mongodb_connection import MongoDB
from utils.postgres_connection import PostgresDB
from utils import config as cfg
from datetime import date
import re
import time
import multiprocessing
import xml.etree.ElementTree as ET
###########################################################################
#### 1. Global variable
###########################################################################
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Accept-Encoding": "*",
    "Connection": "keep-alive"
}
mongodb = postgresdb = None

# Get current date in YYYY-MM-DD format
today = date.today().strftime("%Y-%m-%d")  
mongo_conn = cfg.mongodb['CRAWLING']
postgres_conn = cfg.postgres['DWH']

###########################################################################
#### 2. Connection
###########################################################################

def connect_mongodb():   
    """
    Return a connection to mongodb
    Args: None
    Returns: mongodb
    """      
    mongodb = MongoDB(  dbname = mongo_conn['dbname'], 
                        collection_name = mongo_conn['vnw_job_post_sitemap'],
                        host = mongo_conn['host'], 
                        port = mongo_conn['port'], 
                        username = mongo_conn['username'], 
                        password = mongo_conn['password']
                    )
    mongodb.connect()
    
    return mongodb

def connect_postgresdb():   
    """
    Return a connection to postgresdb
    Args: None
    Returns: postgresdb
    """      
    postgresdb = PostgresDB(    dbname = postgres_conn['dbname'], 
                                host = postgres_conn['host'], 
                                port = postgres_conn['port'], 
                                user = postgres_conn['username'], 
                                password = postgres_conn['password']
                    )
    postgresdb.initialize_pool()
    
    return postgresdb   

###########################################################################
#### 3. Sitemap process: crawl => mongodb => postgres
###########################################################################
def check_url_worker(job_url):
    url_name = job_url[len('https://www.vietnamworks.com/') : len('https://www.vietnamworks.com/') +1]
    # print(url_name)
    if url_name in 'abcdefghigkl':
        return 1
    return 2

def crawl_job_post_sitemap(url):
    """
    Reads an XML URL containing URLs and saves them to a JSON file.
    Args:
        url (str): The URL of the XML file containing URLs.
    Raises:
        Exception: If the request fails or the XML parsing fails.           
    Return:
        List
    """    
    list_url = []
    # Regular expression to match the number before "-jv"
    pattern = r'-(\d+)-jv$'
    try:
        # Step 1: Fetch the sitemap
        response = requests.get(url = url, 
                                headers = headers)
        
        if response.status_code == 410:
            print(f"Warning: XML resource might be unavailable (410 Gone).")
            return  # Exit the function if it's a 410 error
        elif response.status_code != 200:
            raise Exception(f"Failed to fetch XML: {response.status_code}")
        elif response.status_code == 200:
            sitemap_content = response.content
            print("Sitemap fetched successfully")
            # Step 2: Parse the sitemap using ElementTree
            root = ET.fromstring(sitemap_content)
            namespaces = {'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9'}  # Namespace for the sitemap

            # Step 3: Extract the loc, changefreq, lastmod, and priority tags from the sitemap
            for url in root.findall('ns:url', namespaces):
                job_url = url.find('ns:loc', namespaces).text.strip()                
                job_id = re.search(pattern, job_url).group(1) if job_url else None
                changefreq = url.find('ns:changefreq', namespaces)
                lastmod = url.find('ns:lastmod', namespaces)
                priority = url.find('ns:priority', namespaces)
                
                list_url.append({
                    'job_url': job_url,
                    'job_id':job_id,
                    'changefreq': changefreq.text.strip() if changefreq is not None else None,
                    'lastmod': lastmod.text.strip() if lastmod is not None else None,
                    'priority': priority.text.strip() if priority is not None else None,
                    'created_date': today,
                    'worker': check_url_worker(job_url)
                })
                                
        return list_url    
    except requests.exceptions.RequestException as e:
        print( f"Error occurred: {str(e)}")         

def daily_job_post_sitemap_process():
    """
    Process the pipeline to crawl and store data of sitemap url into mongodb
    Args: 
        mongodb: connection to mongodb
    Returns: 
    """ 
    mongodb = connect_mongodb()
    # Crawling sitemap
    sitemap_url = "https://www.vietnamworks.com/sitemap/jobs.xml" 
    list_url = crawl_job_post_sitemap(sitemap_url)
    # print('length: ', list_url)
    
     # Delete current data
    delete_filter = {"created_date": today}
    mongodb.delete_many(delete_filter)
    
    # Load current data
    mongodb.insert_many(list_url)
    
    # Close the connection    
    mongodb.close()
    
def daily_job_post_sitemap_to_postgres():     
    mongodb = postgresdb = None
    try:
        mongodb = connect_mongodb()
        mongodb.set_collection(mongo_conn['vnw_job_post_sitemap']) 
        filter = {"created_date": today}
        employer_docs = mongodb.select(filter)
        
        postgresdb = connect_postgresdb()
        # delete current data
        condition_to_delete = {"created_date": today}
        deleted_rows = postgresdb.delete(postgres_conn['vnw_job_post_sitemap'], condition_to_delete)
        # load current data
        print(f'Delete {deleted_rows} employer sitemap urls')
        # load new data
        for doc in employer_docs:
            doc_id = doc.pop('_id', None)  # Remove MongoDB specific ID
            inserted_id = postgresdb.insert(postgres_conn["vnw_job_post_sitemap"], doc, "job_id")
            print("Inserting job_id: ", inserted_id)
       
        # close connection
        mongodb.close()
        postgresdb.close_pool()
        print("Data transferred successfully")
    except Exception as e:
        print(f"Error transferring data: {e}")     
   
###########################################################################
#### 4. Job post detail process:crawl => mongodb => postgres
###########################################################################
def crawl_job_post_template(soup, job_url):
    """
    Crawl a job with template 1
    Args: 
        job_url (string): job url
    Returns: job (json)
    """ 
    # Attribute
    job = {}
    job_id = job_title = company_url  = updated_date = industry =  \
    job_type = salary = experience = job_level = deadline = benefit = \
    job_description = job_requirement = more_information = updated_date_on_web = None
    
    pattern = r'\.([A-Z0-9]+)\.html'
    match = re.search(pattern, job_url)
    if match:
        job_id = match.group(1)    
        
    # PART 1: TOP 
    
    # PART 2: BODY
    
    # PART 1: BOTTOM
    
    job = {
        "job_id":job_id,
        "job_url": job_url,
        "job_title": job_title,
        "company_url": company_url,
        "updated_date_on_web": updated_date_on_web,
        "industry": industry,
        "job_type": job_type,
        "salary": salary,
        "experience": experience,
        "job_level": job_level,
        "deadline": deadline,
        "benefit": benefit,
        "job_description": job_description,
        "job_requirement": job_requirement,
        "more_information": more_information,
        "created_date": today,
        "updated_date": today,
        "worker" : check_url_worker(job_url)
    }   
    return job

def crawl_job_post_worker(job_url):
    """
    Crawl a job
    Args: 
        url (string): job url
    Returns: 
    """ 
    time.sleep(1) 
    try:
        response = requests.get(    url = job_url, 
                                    headers=headers)
        parser = 'html.parser'
        if response.status_code == 410:
            print(f"Warning: XML resource might be unavailable (410 Gone).")
            return  # Exit the function if it's a 410 error
        elif response.status_code != 200:
            
            raise Exception(f"Failed to fetch XML: {response.status_code}, url is {job_url}")
        elif response.status_code == 200:
            # Crawl job
            soup = BeautifulSoup(response.content, parser) 
            job = {}  
            job = crawl_job_post_template(soup, job_url)
            
            mongodb = connect_mongodb()    
            mongodb.set_collection(mongo_conn['vnw_job_post_detail'])
            
            if job:
                filter = {"job_id": job["job_id"]}
                
                if len(mongodb.select(filter)) > 0:
                    print("Update ", filter)
                    # Remove the 'created_date' key from the dictionary
                    if "created_date" in job:
                        del job["created_date"]
                    mongodb.update_one(filter, job)
                else:
                    print("Insert ", filter)
                    mongodb.insert_one(job)
                
                # Close the connection    
                mongodb.close()            
                # time.sleep(1) 
    except requests.exceptions.RequestException as e:
        print( f"Error occurred: {str(e)}")
 
def daily_job_url_generator_airflow(worker):    
    """
    Crawl all jobs in sitemap data and store into mongodb using Airflow
    Args: 
        worker
    Returns: job url
    """  
    mongodb = connect_mongodb()
    mongodb.set_collection(mongo_conn['vnw_job_post_sitemap'])
    # Filter
    filter = {"created_date": today, "worker": worker}
    # Projecttion: select only the "job_url" field
    projection = {"_id": False, "job_url": True}
    cursor = mongodb.select(filter, projection)
    count = 0
    # Extract job_url
    for document in cursor:
        print(document["job_url"])
        crawl_job_post_worker(document["job_url"]) 
        count += 1
        if  count > 4:
            break
        #     break   
    # Close the connection    
    mongodb.close()      
 
def daily_load_job_post_detail_to_postgres():       
    """
    Process the pipeline to transfer job post detail from mongodb to postgres using Airflow
    Args: 
        mongodb: connection to mongodb
    Returns: 
    """   
    mongodb = postgresdb = None
    try:
        mongodb = connect_mongodb()
        mongodb.set_collection(mongo_conn['vnw_job_post_detail']) 
        # load full
        employer_docs = mongodb.select()
        
        postgresdb = connect_postgresdb()
        # truncate
        postgresdb.truncate_table(postgres_conn["vnw_job_post_detail"])
        # load full
        for doc in employer_docs:
            doc_id = doc.pop('_id', None)  # Remove MongoDB specific ID
            inserted_id = postgresdb.insert(postgres_conn["vnw_job_post_detail"], doc, "job_id")
            print("Inserting job_id: ", inserted_id)
       
        # close connection
        mongodb.close()
        postgresdb.close_pool()
        print("Data transferred successfully")
    except Exception as e:
        print(f"Error transferring data: {e}")   
        
          
if __name__ == "__main__":  
    # Process sitemap
    daily_job_post_sitemap_process()
    print('main')