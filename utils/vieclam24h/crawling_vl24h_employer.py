"""sitemap employer của cv chuẩn XML nên có thể dùng thư viện xml.etree.ElementTree
"""
import multiprocessing.pool
import os
import sys 
import json
import requests
from bs4 import BeautifulSoup
import xml.etree.ElementTree as ET
from utils.mongodb_connection import MongoDB
from utils.postgres_connection import PostgresDB
from utils import config as cfg
from datetime import date
import re
import time
import multiprocessing

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
                        collection_name = mongo_conn['vl24h_employer_sitemap'],
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

def crawl_employer_sitemap(url):
    """
    Reads an XML URL containing URLs and saves them to a JSON file.
    Args:
        url (str): The URL of the XML file containing URLs.
    Raises:
        Exception: If the request fails or the XML parsing fails.           
    Return:
        List of sitemap url
    """    
    list_url = []
    pattern = r'(ntd\d+p\d+)\.html'
    try:
        response = requests.get(url = url, 
                                headers = headers)
        
        if response.status_code == 410:
            print(f"Warning: XML resource might be unavailable (410 Gone).")
            return  # Exit the function if it's a 410 error
        elif response.status_code != 200:
            raise Exception(f"Failed to fetch XML: {response.status_code}")
        elif response.status_code == 200:
            """ Solution 1 - Using BeautifulSoup
            """
            # # Crawl sitemap
            # soup = BeautifulSoup(response.content, "xml")
            # list_item = soup.find_all('url')
        
            # for item in list_item:
            #     employer_url = item.find('loc').get_text() if item.find('loc') else None
            #     employer_id = re.search(pattern, employer_url).group(1) if employer_url else None
            #     changefreq = item.find('changefreq').get_text() if item.find('changefreq') else None
            #     lastmod = item.find('lastmod').get_text() if item.find('lastmod') else None
                
            #     list_url.append(
            #         {
            #             "employer_id": employer_id,
            #             "employer_url": employer_url,
            #             "changefreq": changefreq,
            #             "lastmod": lastmod,
            #             "created_date": today
            #         }
            #     ) 
            """
            Solution 2: Using ElementTree + BeautifulSoup
            """
            root = ET.fromstring(response.content)
            namespaces = {'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9'}  # Namespace for the sitemap
            for url in root.findall('ns:url', namespaces):
                employer_url = url.find('ns:loc', namespaces).text.strip()
                print(employer_url)
                employer_id = re.search(pattern, employer_url).group(1) if employer_url else None
                changefreq = url.find('ns:changefreq', namespaces)
                lastmod = url.find('ns:lastmod', namespaces)
                
                list_url.append({
                    "employer_id": employer_id,
                    'employer_url': employer_url,
                    'changefreq': changefreq.text.strip() if changefreq is not None else None,
                    'lastmod': lastmod.text.strip() if lastmod is not None else None,
                    "created_date": today,
                    "worker": check_url_worker(employer_url)
                })
            
        return list_url    
    except requests.exceptions.RequestException as e:
        print( f"Error occurred: {str(e)}")         
    
def daily_employer_sitemap_process():
    """
    Process the pipeline to crawl and store data of sitemap url into mongodb
    Args: 
        mongodb: connection to mongodb
    Returns: 
    """ 
    mongodb = connect_mongodb()
    # Delete current data
    delete_filter = {"created_date": today}
    mongodb.delete_many(delete_filter)
    
    # Crawling sitemap
    sitemap_urls = ["https://cdn1.vieclam24h.vn/file/sitemap/employer/congty-0.xml", "https://cdn1.vieclam24h.vn/file/sitemap/employer/congty-1.xml"]
    for sitemap_url in sitemap_urls:
        list_url = crawl_employer_sitemap(sitemap_url)        
        # Load current data
        mongodb.insert_many(list_url)
    
    # Close the connection    
    mongodb.close()
 
def daily_employer_sitemap_to_postgres():  
    """
    Process the pipeline to transfer employer sitemap from mongodb to postgres
    Args: 
        mongodb: connection to mongodb
    Returns: 
    """    
    mongodb = postgresdb = None
    try:
        mongodb = connect_mongodb()
        mongodb.set_collection(mongo_conn['vl24h_employer_sitemap']) 
        filter = {"created_date": today}
        employer_docs = mongodb.select(filter)
        
        postgresdb = connect_postgresdb()
        # delete current data
        condition_to_delete = {"created_date": today}
        deleted_rows = postgresdb.delete(postgres_conn['vl24h_employer_sitemap'], condition_to_delete)
        print(f'Delete {deleted_rows} employer sitemap urls')
        # load new data
        for doc in employer_docs:
            doc_id = doc.pop('_id', None)  # Remove MongoDB specific ID
            inserted_id = postgresdb.insert(postgres_conn["vl24h_employer_sitemap"], doc, "employer_id")
            print("Inserting employer_id: ", inserted_id)
       
        # close connection
        mongodb.close()
        
        print("Data transferred successfully")
    except Exception as e:
        print(f"Error transferring data: {e}")
    
###########################################################################
#### 4. Employer detail process: crawl => mongodb => postgres
###########################################################################
 
def check_url_worker(employer_url):
    return 1 if 'https://vieclam24h.vn/danh-sach-tin-tuyen-dung-cong-ty-tnhh' in employer_url else 2
    
def crawl_employer_worker(employer_url):
    """
    Crawl a employer and save to mongodb
    Args: 
        url (string): employer url
    Returns: 
    """ 
    time.sleep(1) 
    employer_id = employer_name = location = company_size = industry = website = about_us = None
    pattern = r'(ntd\d+p\d+)\.html'
    match = re.search(pattern, employer_url)
    if match:
        employer_id = match.group(1)
        
    try:
        response = requests.get(    url = employer_url, 
                                    headers=headers)
        parser = 'html.parser'
        if response.status_code == 410:
            print(f"Warning: XML resource might be unavailable (410 Gone).")
            return  # Exit the function if it's a 410 error
        elif response.status_code != 200:
            raise Exception(f"Failed to fetch XML: {response.status_code}, url is {employer_url}")
        elif response.status_code == 200:
            # Crawl an employer
            soup = BeautifulSoup(response.content, parser) 
            company_info = soup.find('div', class_='company-info')
            employer = {} 
            
            employer_name = soup.find('h1', id='qc-name-company').text.strip() if soup.find('h1', id='qc-name-company') else None
            
            if soup.find('span', id='qc-website-company'):
                website = soup.find('span', id='qc-website-company').find_parent()['title']
             
            h3_elements = soup.select('#qc-box-communications > div > div:nth-child(2) > h3') 
                          
            specific_h3= next((h3 for h3 in h3_elements if "Địa chỉ:" in h3.text), None)
            if specific_h3:
                location = specific_h3.text.replace('Địa chỉ:', '').strip()
                
            specific_h3= next((h3 for h3 in h3_elements if "Quy mô:" in h3.text), None)
            if specific_h3:
                company_size = specific_h3.text.replace('Quy mô:', '').strip()
            
            specific_h3= next((h3 for h3 in h3_elements if "Lĩnh vực:" in h3.text), None)
            if specific_h3:
                industry = specific_h3.text.replace('Lĩnh vực:', '').strip()
                  
            employer = {
                    "employer_id": employer_id,
                    "employer_name": employer_name,
                    "location" : location,
                    "company_size" : company_size,
                    "industry" : industry,
                    "website" : website,
                    "about_us" : about_us,                
                    "employer_url": employer_url,
                    "created_date": today,
                    "updated_date": today,
                    "worker": check_url_worker(employer_url)
                }      
            print(employer)                   
            mongodb = connect_mongodb()    
            mongodb.set_collection(mongo_conn['vl24h_employer_detail'])
            
            # check employ_id exist or not
            filter = {"employer_id": employer_id}
            if len(mongodb.select(filter)) > 0:
                print("Update ", filter)
                # Remove the 'created_date' key from the dictionary
                if "created_date" in employer:
                    del employer["created_date"]
                mongodb.update_one(filter, employer)
            else:
                print("Insert ", filter)                    
                mongodb.insert_one(employer)
            # Close the connection    
            mongodb.close()  
    except requests.exceptions.RequestException as e:
        print( f"Error occurred: {str(e)}")
           
def employer_url_generator():    
    """
    Crawl all jobs in sitemap data and store into mongodb - not use Airflow, use multiprocessing, yield
    Args: 
        mongodb
    Returns: employer url
    """  
    mongodb = connect_mongodb()
    mongodb.set_collection(mongo_conn['vl24h_employer_sitemap'])
    # Filter
    filter = {"created_date": today}
    # Projecttion: select only the "job_url" field
    projection = {"_id": False, "employer_url": True}
    cursor = mongodb.select(filter, projection)
    
    # Extract job_url
    for document in cursor: 
        print(document["employer_url"])
        yield document["employer_url"]
    
    # Close the connection    
    mongodb.close()
    
def daily_employer_url_generator_airflow(worker):    
    """
    Crawl all jobs in sitemap data and store into mongodb using Airflow
    Args: 
        worker
    Returns: employer url
    """  
    mongodb = connect_mongodb()
    mongodb.set_collection(mongo_conn['vl24h_employer_sitemap'])
    # Filter
    filter = {"lastmod": today, "worker": worker}
    # Projecttion: select only the "job_url" field
    projection = {"_id": False, "employer_url": True}
    cursor = mongodb.select(filter, projection)
    count = 0
    # Extract job_url
    for document in cursor: 
        print(document["employer_url"])
        crawl_employer_worker(document["employer_url"])  
        count += 1
        if  count > 4:
            break
        # break
    # Close the connection    
    mongodb.close()
 
def current_employer_process():
    """
    Process the pipeline to crawl and store data of employer url into mongodb - not use Airflow, use multiprocessing, yield
    Args: 
        mongodb: connection to mongodb
    Returns: 
    """ 
    mongodb = connect_mongodb()
    mongodb.set_collection(mongo_conn['vl24h_employer_detail'])    
    # Delete current data
    delete_filter = {
                    "$or": [
                        { "created_date": { "$eq": today } },
                        { "updated_date": { "$eq": today } }
                    ]
                    }
    mongodb.delete_many(delete_filter)
    # Close the connection    
    mongodb.close()
    
    # print('Start to crawl')
    with multiprocessing.Pool(2) as pool:
        # parallel the scapring process
        pool.map(crawl_employer_worker, employer_url_generator())
        
def daily_load_employer_detail_to_postgres():    
    """
    Process the pipeline to transfer employer detail from mongodb to postgres using Airflow
    Args: 
        mongodb: connection to mongodb
    Returns: 
    """  
    mongodb = postgresdb = None
    try:
        mongodb = connect_mongodb()
        mongodb.set_collection(mongo_conn['vl24h_employer_detail']) 
        # load full
        employer_docs = mongodb.select()
        
        postgresdb = connect_postgresdb()
        # truncate 
        postgresdb.truncate_table(postgres_conn["vl24h_employer_detail"])
        # load full
        for doc in employer_docs:
            doc_id = doc.pop('_id', None)  # Remove MongoDB specific ID
            inserted_id = postgresdb.insert(postgres_conn["vl24h_employer_detail"], doc, "employer_id")
            print("Inserting employer_id: ", inserted_id)
       
        # close connection
        mongodb.close()
        postgresdb.close_pool()
        
        print("Data transferred successfully")
    except Exception as e:
        print(f"Error transferring data: {e}")

def delete_duplicate_employer_detail():
    mongodb = connect_mongodb()
    mongodb.set_collection(mongo_conn['vl24h_employer_detail'])
        # Delete duplicates based on specified key fields
    key_fields = ["employer_id"]  # Fields to identify duplicates
    condition = {"created_date": {"$gte": "2024-06-01"}}  # Condition to filter documents
    mongodb.delete_duplicates_with_condition(key_fields, condition)
    mongodb.close()   
 
if __name__ == "__main__":  
    # daily_employer_sitemap_process()
    # daily_employer_sitemap_to_postgres()
    employer_url = 'https://vieclam24h.vn/danh-sach-tin-tuyen-dung-sieu-viet-group-ntd2411779p122.html'
    crawl_employer_worker(employer_url)
     

    


