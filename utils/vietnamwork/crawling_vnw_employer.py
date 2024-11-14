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
import utils.common as cm
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
    """
    Determines the worker ID based on the given URL.

    This function checks if the URL belongs to a specific employer path on vietnamworks.com.
    If the URL contains 'www.vietnamworks.com/nha-tuyen-dung', it returns worker ID 1.
    Otherwise, it returns worker ID 2.

    Args:
        url (str): The URL to be checked.

    Returns:
        int: The worker ID, either 1 or 2.
            - 1: If the URL contains 'www.vietnamworks.com/nha-tuyen-dung'.
            - 2: Otherwise.
    """
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
    Reads an XML URL containing employer URLs and returns a list of employer details.

    Args:
        sitemap_url (str): The URL of the XML sitemap file containing employer URLs.

    Raises:
        Exception: If the request fails or the XML parsing fails.

    Returns:
        list: A list of dictionaries containing employer details.
    """
    list_url = []

    try:
        # Fetch the XML sitemap
        response = requests.get(url=sitemap_url, headers=headers)
        response.raise_for_status()  # Raise an error for bad status codes automatically

        # Parse the XML content
        root = ET.fromstring(response.content)
        namespaces = {'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9'}  # Namespace for the sitemap

        # Iterate through each URL entry in the sitemap
        for url_element in root.findall('ns:url', namespaces):
            employer_url = cm.extract_text(url_element, 'ns:loc', namespaces)
            changefreq = cm.extract_text(url_element, 'ns:changefreq', namespaces)
            lastmod = cm.extract_text(url_element, 'ns:lastmod', namespaces)
            employer_id = generate_employer_id(employer_url)

            list_url.append({
                "employer_id": employer_id,
                'employer_url': employer_url,
                'changefreq': changefreq,
                'lastmod': lastmod,
                "created_date": today,
                "worker": check_url_worker(employer_url)
            })

        return list_url

    except requests.exceptions.RequestException as e:
        print(f"Error occurred: {str(e)}")       
    
def daily_employer_sitemap_process():
    """
    Process the pipeline to crawl and store employer sitemap data into MongoDB.

    This function fetches the latest employer sitemap data from a given URL, deletes any records 
    with the current date, and inserts the newly fetched data into MongoDB.

    Args:
        None

    Returns:
        None
    """
    mongodb = None
    try:
        # Connect to MongoDB
        mongodb = connect_mongodb()

        # Crawl the sitemap and get the list of URLs
        sitemap_url = "https://www.vietnamworks.com/sitemap/companies.xml"
        list_url = crawl_employer_sitemap(sitemap_url)

        # Delete existing records with the current date in MongoDB
        delete_filter = {"created_date": today}
        mongodb.delete_many(delete_filter)

        # Insert new data into MongoDB
        mongodb.insert_many(list_url)

        print("Employer sitemap data processed and stored successfully.")

    except Exception as e:
        print(f"An error occurred while processing employer sitemap: {e}")

    finally:
        # Ensure the MongoDB connection is closed properly if it was created
        if 'mongodb' in locals() and mongodb:
            mongodb.close()

def daily_employer_sitemap_to_postgres():
    """
    Process the pipeline to transfer employer sitemap data from MongoDB to PostgreSQL.

    Args:
        None

    Returns:
        None
    """
    mongodb = postgresdb = None
    try:
        # Connect to MongoDB and select the relevant collection
        mongodb = connect_mongodb()
        mongodb.set_collection(mongo_conn['vnw_employer_sitemap'])

        # Load the filtered data from MongoDB
        filter = {"created_date": today}
        employer_docs = mongodb.select(filter)

        # Connect to PostgreSQL and delete current data for today
        postgresdb = connect_postgresdb()
        condition_to_delete = {"created_date": today}
        deleted_rows = postgresdb.delete(postgres_conn['vnw_employer_sitemap'], condition_to_delete)
        print(f'Deleted {deleted_rows} employer sitemap URLs')

        # Insert new data into PostgreSQL
        for doc in employer_docs:
            doc.pop('_id', None)  # Remove MongoDB specific ID
            inserted_id = postgresdb.insert(postgres_conn['vnw_employer_sitemap'], doc, "employer_id")
            print("Inserting employer_id:", inserted_id)

        # Print success message
        print("Data transferred successfully")
    
    except Exception as e:
        print(f"Error transferring data: {e}")

    finally:
        # Ensure connections are closed
        if mongodb:
            mongodb.close()
        if postgresdb:
            postgresdb.close_pool()
        
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
    employer_id = employer_name = location = company_size = industry = website = about_us = total_current_jobs = None
    
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
            if soup.find('div', id='ajax_cp_our_jobs_listing'):
               total_current_jobs = re.findall(r'\d+', soup.find('div', id='ajax_cp_our_jobs_listing').find('h3').text)[0]
               
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
                    "total_current_jobs": total_current_jobs,
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
    employer_id = employer_name = location = company_size = industry = website = about_us = total_current_jobs = None
    
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
             
            if soup.find('div', id='OpeningJobHeader'):
                total_current_jobs = re.findall(r'\d+', soup.find('div', id='OpeningJobHeader').find('h3').text)[0] 
                 
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
                    "total_current_jobs": total_current_jobs,
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
            
    pattern_recruiter = r"https://www\.vietnamworks\.com/nha-tuyen-dung/"
    pattern_company = r"https://www\.vietnamworks\.com/company/"

    # Check templete of url
    if re.match(pattern_company, employer_url):
        employer = crawl_employer_template1(employer_url)
    elif re.match(pattern_recruiter, employer_url):
        employer = crawl_employer_template2(employer_url)
    else:
        print("Employer url is undefined" )          
        
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

def daily_employer_url_generator_airflow(worker):
    """
    Crawl all employer URLs from the sitemap data and store the resulting information in MongoDB using Airflow.

    Args: 
        worker (str): Identifier for the worker responsible for crawling.
    
    Returns:
        None
    """
    try:
        # Connect to MongoDB and set the collection to work with
        mongodb = connect_mongodb()
        mongodb.set_collection(mongo_conn['vnw_employer_sitemap'])

        # Define the filter and projection for MongoDB selection
        filter_query = {"lastmod": today, "worker": worker}
        projection_fields = {"_id": False, "employer_url": True}

        # Retrieve the relevant employer URLs
        employer_urls = mongodb.select(filter_query, projection_fields)

        # Crawl employer data and limit to a maximum of 5 URLs (count starts from 0)
        for count, document in enumerate(employer_urls):
            print(f"Crawling employer URL: {document['employer_url']}")
            crawl_employer_worker(document["employer_url"])
            
            # Stop after processing 5 URLs
            if count >= 4:
                break

    except Exception as e:
        print(f"Error occurred while generating employer URLs: {str(e)}")

    finally:        
        # Ensure the MongoDB connection is closed properly if it was created
        if 'mongodb' in locals() and mongodb:
            mongodb.close()

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

    

    


