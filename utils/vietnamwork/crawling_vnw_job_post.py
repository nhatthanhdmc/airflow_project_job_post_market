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
import utils.common as cm
from utils import config as cfg
from datetime import date, datetime
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
pattern = r'-(\d+)-jv$'
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
    """
    Determines the worker ID based on the URL structure.

    This function extracts a specific character from the job URL to determine which worker
    should handle the processing. If the character is within the specified range, it returns worker ID 1;
    otherwise, it returns worker ID 2.

    Args:
        job_url (str): The URL of the job posting.

    Returns:
        int: Worker ID (1 or 2) based on the extracted character from the URL.
    """
    # Extract a character from the job URL after the base URL length
    url_name = job_url[len('https://www.vietnamworks.com/'): len('https://www.vietnamworks.com/') + 1]

    # Determine the worker ID based on the extracted character
    if url_name in 'abcdefghigkl':
        return 1
    return 2

def crawl_job_post_sitemap(url):
    """
    Reads an XML URL containing job URLs and returns a list of extracted data.
    
    Args:
        url (str): The URL of the XML file containing job URLs.
    
    Raises:
        Exception: If the request fails or XML parsing fails.
    
    Returns:
        list: A list of dictionaries with job URL details.
    """    
    list_url = []
    
    try:
        # Step 1: Fetch the sitemap
        response = requests.get(url=url, headers=headers)
        
        if response.status_code == 410:
            print(f"Warning: XML resource might be unavailable (410 Gone).")
            return  # Exit the function if it's a 410 error
        elif response.status_code != 200:
            raise Exception(f"Failed to fetch XML: {response.status_code}")
        
        sitemap_content = response.content
        print("Sitemap fetched successfully")
        
        # Step 2: Parse the sitemap using ElementTree
        root = ET.fromstring(sitemap_content)
        namespaces = {'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9'}  # Namespace for the sitemap

        # Step 3: Extract relevant data from each <url> tag
        for url in root.findall('ns:url', namespaces):
            job_url = cm.extract_text(url, 'ns:loc', namespaces)
            job_id = cm.extract_object_id(job_url, pattern)
            changefreq = cm.extract_text(url, 'ns:changefreq', namespaces)
            lastmod = cm.extract_text(url, 'ns:lastmod', namespaces)
            priority = cm.extract_text(url, 'ns:priority', namespaces)
            
            list_url.append({
                'job_url': job_url,
                'job_id': job_id,
                'changefreq': changefreq,
                'lastmod': lastmod,
                'priority': priority,
                'created_date': today,
                'worker': check_url_worker(job_url)
            })
        
        return list_url

    except requests.exceptions.RequestException as e:
        print(f"Error occurred: {str(e)}")

def daily_job_post_sitemap_process():
    """
    Process the pipeline to crawl and store job sitemap data into MongoDB.

    This function retrieves job sitemap data from a specified URL and inserts it into MongoDB after 
    deleting any existing records for the current date.

    Args: 
        None

    Returns: 
        None
    """ 
    try:
        # Connect to MongoDB
        mongodb = connect_mongodb()

        # Crawl sitemap data
        sitemap_url = "https://www.vietnamworks.com/sitemap/jobs.xml" 
        list_url = crawl_job_post_sitemap(sitemap_url)

        if list_url:
            # Delete existing records for today in MongoDB
            delete_filter = {"created_date": today}
            deleted_count = mongodb.delete_many(delete_filter)
            print(f"Deleted {deleted_count} records from MongoDB.")

            # Insert new records into MongoDB
            insert_result = mongodb.insert_many(list_url)
            print(f"Inserted {len(insert_result)} records into MongoDB.")

    except Exception as e:
        print(f"Error processing job sitemap data: {e}")

    finally:        
        # Ensure the MongoDB connection is closed properly if it was created
        if 'mongodb' in locals() and mongodb:
            mongodb.close()

def daily_job_post_sitemap_to_postgres():     
    """
    Transfers job post sitemap data from MongoDB to PostgreSQL.

    This function retrieves job posts from MongoDB and transfers the data to PostgreSQL.
    It deletes existing records for the current date in PostgreSQL before inserting new data.

    Args: 
        None

    Returns: 
        None
    """
    mongodb = postgresdb = None
    try:
        # Connect to MongoDB
        mongodb = connect_mongodb()
        mongodb.set_collection(mongo_conn['vnw_job_post_sitemap'])
        
        # Retrieve data created today from MongoDB
        filter = {"created_date": today}
        employer_docs = mongodb.select(filter)
        
        # Connect to PostgreSQL
        postgresdb = connect_postgresdb()

        # Delete current data in PostgreSQL
        condition_to_delete = {"created_date": today}
        deleted_rows = postgresdb.delete(postgres_conn['vnw_job_post_sitemap'], condition_to_delete)
        print(f'Deleted {deleted_rows} job post sitemap URLs')

        # Insert new data into PostgreSQL
        for doc in employer_docs:
            doc.pop('_id', None)  # Remove MongoDB specific ID
            inserted_id = postgresdb.insert(postgres_conn["vnw_job_post_sitemap"], doc, "job_id")
            print(f"Inserting job_id: {inserted_id}")

        print("Data transferred successfully")

    except Exception as e:
        print(f"Error transferring data: {e}")

    finally:
        # Ensure connections are properly closed
        if mongodb:
            mongodb.close()
        if postgresdb:
            postgresdb.close_pool()    
   
###########################################################################
#### 4. Job post detail process:crawl => mongodb => postgres
###########################################################################
def crawl_job_post_template(soup, job_url):
    """
    Crawl a job post and extract relevant data from the page.

    Args:
        soup (BeautifulSoup): BeautifulSoup object representing the HTML of the job post page.
        job_url (str): The URL of the job post.

    Returns:
        dict: A dictionary containing job details.
    """
    # Initialize attributes in a dictionary
    job = {
        "job_id": cm.extract_object_id(job_url, pattern),
        "job_url": job_url,
        "job_title": None,
        "location": None,
        "company_url": None,
        "updated_date_on_web": None,
        "industry": None,
        "field": None,
        "job_type": None,
        "salary": None,
        "experience": None,
        "job_level": None,
        "deadline": None,
        "benefit": None,
        "job_description": None,
        "job_requirement": None,
        "more_information": None,
        "created_date": today,
        "total_views": None,
        "posted_date": None,
        "worker": check_url_worker(job_url)
    }

    # Extract information using reusable function extract_text and directly update the job dictionary
    job["job_title"] = cm.extract_text(soup, 'h1', {'name': 'title'})
    job["salary"] = cm.extract_text(soup, '#vnwLayout__col > span', index=0)
    job["deadline"] = cm.extract_text(soup, '#vnwLayout__col > div > span', index=0)

    # Extract total views using regex if the required element exists
    total_views_text = cm.extract_text(soup, '#vnwLayout__col > div > span', index=1)
    job["total_views"] = re.findall(r'\d+', total_views_text)[0] if total_views_text else None

    job["company_url"] = cm.extract_text(soup, '#vnwLayout__col > div > div.sc-37577279-0.joYsyf > div.sc-37577279-3.drWnZq > a', attr='href', index=0)

    # Extract job description by joining paragraph texts
    job["job_description"] = ''.join(p.text.strip() for p in soup.select('#vnwLayout__col > div > div.sc-4913d170-0.gtgeCm > div > div > div:nth-child(1) > div > div > p')) if soup.select('#vnwLayout__col > div > div.sc-4913d170-0.gtgeCm > div > div > div:nth-child(1) > div > div > p') else None

    # Extract benefit information
    job["benefit"] = '\n '.join([div.text.strip() for div in soup.find_all('div', attrs={'data-benefit-name': True})]) if soup.find_all('div', attrs={'data-benefit-name': True}) else None

    # Extract additional job information from div elements
    div_elements = soup.select('#vnwLayout__col > div > div.sc-7bf5461f-2.JtIju')

    def extract_from_divs(div_elements, text_to_search):
        specific_div = next((div for div in div_elements if text_to_search in div.text), None)
        return specific_div.find('p').text.strip() if specific_div else None

    # Extract data from div elements and update job dictionary
    job["posted_date"] = datetime.strptime(extract_from_divs(div_elements, "NGÀY ĐĂNG"), r"%d/%m/%Y") if extract_from_divs(div_elements, "NGÀY ĐĂNG") else None
    job["job_level"] = extract_from_divs(div_elements, "CẤP BẬC")
    job["field"] = extract_from_divs(div_elements, "NGÀNH NGHỀ")
    job["job_requirement"] = extract_from_divs(div_elements, "KỸ NĂNG")
    job["industry"] = extract_from_divs(div_elements, "LĨNH VỰC")
    job["experience"] = extract_from_divs(div_elements, "SỐ NĂM KINH NGHIỆM TỐI THIỂU")

    # Extract location information from the bottom section
    div_elements = soup.select('#vnwLayout__col > div > div.sc-a137b890-0.bAqPjv')
    job["location"] = extract_from_divs(div_elements, "Địa điểm làm việc")

    return job

def crawl_job_post_worker(job_url):
    """
    Crawl a job post from the given URL and save the extracted data to MongoDB.
    
    Args: 
        job_url (str): URL of the job post.
        
    Returns: 
        None
    """ 
    time.sleep(1)
    try:
        # Fetch job post page
        response = requests.get(url=job_url, headers=headers)
        response.raise_for_status()

        if response.status_code == 410:
            print(f"Warning: XML resource might be unavailable (410 Gone).")
            return

        soup = BeautifulSoup(response.content, 'html.parser')
        job = crawl_job_post_template(soup, job_url)

        # Connect to MongoDB
        mongodb = connect_mongodb()
        mongodb.set_collection(mongo_conn['vnw_job_post_detail'])

        if job:
            filter = {"job_id": job["job_id"]}

            if mongodb.select(filter):
                print("Update ", filter)
                job.pop("created_date", None)  # Remove 'created_date' if present
                mongodb.update_one(filter, job)
            else:
                print("Insert ", filter)
                mongodb.insert_one(job)

        # Close the MongoDB connection
        mongodb.close()

    except requests.exceptions.RequestException as e:
        print(f"Error occurred: {str(e)}")

def daily_job_url_generator_airflow(worker):
    """
    Generate and crawl job URLs from the job sitemap using Airflow.

    Args: 
        worker (str): Identifier for the worker to handle specific job URLs.
    
    Returns:
        None
    """
    try:
        # Connect to MongoDB and select job sitemap collection
        mongodb = connect_mongodb()
        mongodb.set_collection(mongo_conn['vnw_job_post_sitemap'])

        # Filter and projection
        filter = {"created_date": today, "worker": worker}
        projection = {"_id": False, "job_url": True}
        cursor = mongodb.select(filter, projection)

        # Extract and crawl job URLs
        count = 0
        for document in cursor:
            print(document["job_url"])
            crawl_job_post_worker(document["job_url"])
            count += 1
            if count >= 5:  # Limit crawling to 5 jobs for each run
                break

    finally:        
        # Ensure the MongoDB connection is closed properly if it was created
        if 'mongodb' in locals() and mongodb:
            mongodb.close()

def daily_load_job_post_detail_to_postgres():
    """
    Transfers job post details from MongoDB to PostgreSQL.

    This function loads all job post details from MongoDB and transfers the data to PostgreSQL.
    It first truncates the existing data in the target PostgreSQL table before inserting new records.

    Returns: 
        None
    """
    mongodb = postgresdb = None
    try:
        # Connect to MongoDB and select the job post detail collection
        mongodb = connect_mongodb()
        mongodb.set_collection(mongo_conn['vnw_job_post_detail'])
        employer_docs = mongodb.select()

        # Connect to PostgreSQL and truncate existing data in the table
        postgresdb = connect_postgresdb()
        postgresdb.truncate_table(postgres_conn["vnw_job_post_detail"])

        # Insert job details from MongoDB into PostgreSQL
        for doc in employer_docs:
            doc.pop('_id', None)  # Remove MongoDB-specific ID
            inserted_id = postgresdb.insert(postgres_conn["vnw_job_post_detail"], doc, "job_id")
            print(f"Inserting job_id: {inserted_id}")

        print("Data transferred successfully")

    except Exception as e:
        print(f"Error transferring data: {e}")

    finally:
        # Close connections
        if mongodb:
            mongodb.close()
        if postgresdb:
            postgresdb.close_pool()
 
        
          
if __name__ == "__main__":  
    # Process sitemap
    crawl_job_post_worker("https://www.vietnamworks.com/customer-service-executive-1836550-jv")
    daily_load_job_post_detail_to_postgres()