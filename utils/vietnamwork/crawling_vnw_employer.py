"""sitemap employer của vnw chuẩn XML nên có thể dùng thư viện xml.etree.ElementTree
"""
import multiprocessing.pool
import os
import sys 
module_path = os.path.abspath(os.getcwd())
if module_path not in sys.path:
    sys.path.append(module_path)
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
import hashlib
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
                        collection_name = mongo_conn['vnw_employer_sitemap'],
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
def check_url_worker(url):    
    if 'www.vietnamworks.com/nha-tuyen-dung' in url:
        return 1
    return 2

def generate_employer_id(employer_url):
    """
    Hash the extracted string using SHA256 (you can use MD5 if preferred)
    Args:
        employer_url (str): The URL of employer.
    Raises:     
    Return:
        employer id
    """    
    # 
    hash_object = hashlib.sha256(employer_url.encode())
    return hash_object.hexdigest()

def crawl_employer_sitemap(sitemap_url):
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
    
    try:
        response = requests.get(url = sitemap_url, 
                                headers = headers)
        
        if response.status_code == 410:
            print(f"Warning: XML resource might be unavailable (410 Gone).")
            return  # Exit the function if it's a 410 error
        elif response.status_code != 200:
            raise Exception(f"Failed to fetch XML: {response.status_code}")
        elif response.status_code == 200:
            """
            Solution 2: Using ElementTree + BeautifulSoup
            """
            root = ET.fromstring(response.content)
            namespaces = {'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9'}  # Namespace for the sitemap
            for url in root.findall('ns:url', namespaces):
                employer_url = url.find('ns:loc', namespaces).text.strip()
                changefreq = url.find('ns:changefreq', namespaces)
                lastmod = url.find('ns:lastmod', namespaces)                   
                employer_id = generate_employer_id(employer_url)
                
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
    # Crawling sitemap
    sitemap_url = "https://www.vietnamworks.com/sitemap/companies.xml"
    list_url = crawl_employer_sitemap(sitemap_url)
    
     # Delete current data
    delete_filter = {"created_date": today}
    mongodb.delete_many(delete_filter)
    
    # Load current data
    mongodb.insert_many(list_url)
    
    # Close the connection    
    mongodb.close()
    
def daily_employer_sitemap_to_postgres():
    mongodb = postgresdb = None
    try:
        mongodb = connect_mongodb()
        mongodb.set_collection(mongo_conn['vnw_employer_sitemap'])
        filter = {"created_date": today}
        employer_docs = mongodb.select(filter)
        
        postgresdb = connect_postgresdb()        
        # delete current data
        condition_to_delete = {"created_date": today}
        deleted_rows = postgresdb.delete(postgres_conn['vnw_employer_sitemap'], condition_to_delete)
        print(f'Delete {deleted_rows} employer sitemap urls')
        # load new data
        for doc in employer_docs:
            doc_id = doc.pop('_id', None) # Remove MongoDB specific ID
            inserted_id = postgresdb.insert(postgres_conn['vnw_employer_sitemap'], doc, "employer_id")
            print("Inserting employer_id: ", inserted_id)
        
        print("Data transferred successfully")
    except Exception as e:
        print(f"Error transferring data: {e}") 
        
###########################################################################
#### 4. Employer detail process: crawl => mongodb => postgres
###########################################################################

def crawl_employer_template1(employer_url):
    """
    Crawl employer url with pattern: https://www.vietnamworks.com/company/
    Ex: https://www.vietnamworks.com/company/misa
    Args: 
        employer_url (string): employer url
    Returns:
        employer (dict): containt all employer information
    """
    employer = {}
    employer_id = employer_name = location = company_size = industry = website = about_us = None
    
    employer_id = generate_employer_id(employer_url)
    
    try:
        response = requests.get(url=employer_url,
                                headers= headers)
        parser = 'html.parser'
        if response.status_code == 410:
            print(f"Warning: XML resource might be unavailable (410 Gone).")
            return  # Exit the function if it's a 410 error
        elif response.status_code != 200:
            raise Exception(f"Failed to fetch XML: {response.status_code}, url is {employer_url}")
        elif response.status_code == 200:
            # craw an employer
            soup = BeautifulSoup(response.content, parser) 
            basic_info = soup.find('div', class_='cp_basic_info_details')
            if basic_info:
                if basic_info.find('h1', id='cp_company_name'):
                    employer_name = soup.find('h1', id='cp_company_name').text.strip()
                if len(basic_info.find_all('span', class_='li-items-limit')) >= 1:
                    location = basic_info.find_all('span', class_='li-items-limit')[0].text.strip()
                if basic_info.find('a', class_='website-company'):
                    website = basic_info.find('a', class_='website-company').get('href')
                if len(basic_info.find_all('span', class_='li-items-limit')) >= 2:
                    industry = basic_info.find_all('span', class_='li-items-limit')[1].text.strip()
             
            if soup.find('div', class_='custom-story-item-content'):
                about_us = soup.find('div', class_='custom-story-item-content').text.strip()   
                
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
            
    except requests.exceptions.RequestException as e:
        print( f"Error occurred: {str(e)}")
    return employer

def crawl_employer_template2(employer_url):
    """
    Crawl employer url with pattern: https://www.vietnamworks.com/nha-tuyen-dung
    Ex: https://www.vietnamworks.com/nha-tuyen-dung/aia-exchange-c383788
    Args: 
        employer_url (string): employer url
    Returns:
        employer (dict): contain all employer information
    """
    employer = {}
    employer_id = employer_name = location = company_size = industry = website = about_us = None
    
    employer_id = generate_employer_id(employer_url)
    
    try:
        response = requests.get(url=employer_url,
                                headers= headers)
        parser = 'html.parser'
        if response.status_code == 410:
            print(f"Warning: XML resource might be unavailable (410 Gone).")
            return  # Exit the function if it's a 410 error
        elif response.status_code != 200:
            raise Exception(f"Failed to fetch XML: {response.status_code}, url is {employer_url}")
        elif response.status_code == 200:
            # craw an employer
            soup = BeautifulSoup(response.content, parser) 
            
            if soup.find('div', id='bannerSection').find('h1'):
                employer_name = soup.find('div', id='bannerSection').find('h1').text.strip()
                    
            basic_info = soup.find('div', id ='AboutUs')
            print(basic_info.find_all('li'))
            if basic_info.find_all('li'):   
                print('xxx')             
                if len(basic_info.find_all('li')) >= 1:
                    company_size = basic_info.find_all('li')[0].find_all('p')[1].text.strip()
                if len(basic_info.find_all('li')) >= 2:
                    industry = basic_info.find_all('li')[1].find_all('p')[1].text.strip()
                if len(basic_info.find_all('li')) >= 3:
                    location = basic_info.find_all('li')[2].text.strip()
                    
            if basic_info.select('#vnwLayout__col > p'):
                about_us = basic_info.select('#vnwLayout__col > p')[0].get_text() 
                
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
            
    except requests.exceptions.RequestException as e:
        print( f"Error occurred: {str(e)}")
    return employer

def crawl_employer_worker(employer_url):
    """
    Crawl a employer
    Args: 
        employer_url (string): employer url
    Returns: 
    """   
        
    try:
        response = requests.get(    url = employer_url, 
                                    headers=headers)
        parser = 'html.parser'
        if response.status_code == 410:
            print(f"Warning: XML resource might be unavailable (410 Gone).")
            return  # Exit the function if it's a 410 error
        elif response.status_code != 200:
            raise Exception(f"Failed to fetch XML: {response.status_code}")
        elif response.status_code == 200:
            # Crawl job
            soup = BeautifulSoup(response.content, parser) 
            company_info = soup.find('div', class_='company-info')
            employer = {} 
            
            pattern_recruiter = r"https://www\.vietnamworks\.com/nha-tuyen-dung/"
            pattern_company = r"https://www\.vietnamworks\.com/company/"

            # Check templete of url
            if re.match(pattern_company, employer_url):
                employer = crawl_employer_template1(employer_url)
            elif re.match(pattern_recruiter, employer_url):
                employer = crawl_employer_template2(employer_url)
            else:
                print("Employer url is undefined" )          
            print(employer)
            
            mongodb = connect_mongodb() 
            mongodb.set_collection(mongo_conn['vnw_employer_detail'])    
            
            # check employ_id exist or not
            filter = {"employer_id": generate_employer_id(employer_url)}
            
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
            # time.sleep(1) 
    except requests.exceptions.RequestException as e:
        print( f"Error occurred: {str(e)}")
           
def employer_url_generator():    
    """
    Crawl all jobs in sitemap data and store into mongodb
    Args: 
        mongodb
    Returns: employer url
    """  
    mongodb = connect_mongodb()
    mongodb.set_collection(mongo_conn['cv_employer_sitemap'])
    # Filter
    filter = {"created_date": today}
    # Projecttion: select only the "employer_url" field
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
    mongodb.set_collection(mongo_conn['vnw_employer_sitemap'])
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
    
def current_employer_detail_process():
    """
    Process the pipeline to crawl and store data of employer url into mongodb
    Args: 
        mongodb: connection to mongodb
    Returns: 
    """ 
    mongodb = connect_mongodb()
    mongodb.set_collection(mongo_conn['cv_employer_detail'])    
     # Delete current data
    delete_filter = {"created_date": today}
    mongodb.delete_many(delete_filter)
    # Close the connection    
    mongodb.close()
    
    print('Start to crawl')
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
        mongodb.set_collection(mongo_conn['vnw_employer_detail']) 
        # load full
        employer_docs = mongodb.select()
        
        postgresdb = connect_postgresdb()
        # truncate 
        postgresdb.truncate_table(postgres_conn["vnw_employer_detail"])
        # load full
        for doc in employer_docs:
            doc_id = doc.pop('_id', None)  # Remove MongoDB specific ID
            inserted_id = postgresdb.insert(postgres_conn["vnw_employer_detail"], doc, "employer_id")
            print("Inserting employer_id: ", inserted_id)
       
        # close connection
        mongodb.close()
        postgresdb.close_pool()
        
        print("Data transferred successfully")
    except Exception as e:
        print(f"Error transferring data: {e}")
               
if __name__ == "__main__":  
    # Process site map process
    # daily_employer_sitemap_process()
    # daily_employer_sitemap_to_postgres()
    # process employer detail
    url = "https://www.vietnamworks.com/company/misa"
    url = "https://www.vietnamworks.com/nha-tuyen-dung/aia-exchange-c383788"
    crawl_employer_worker(url)
    daily_load_employer_detail_to_postgres()

    

    


