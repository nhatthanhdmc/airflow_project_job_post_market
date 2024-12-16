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
import utils.common as cm
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
pattern = r'\.([A-Z0-9]+)\.html'

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
                        collection_name = mongo_conn['cv_employer_sitemap'],
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
    Reads an XML sitemap URL and extracts employer URLs and related metadata.

    Args:
        url (str): The URL of the XML file containing employer data.

    Returns:
        list: A list of dictionaries, each containing metadata about the employer URLs.
    """
    namespaces = {'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
    list_url = []

    try:
        # Step 1: Fetch the sitemap
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raise HTTPError for bad responses (e.g., 4xx, 5xx)

        # Step 2: Parse the sitemap using ElementTree
        root = ET.fromstring(response.content)
        print("Sitemap fetched and parsed successfully.")

        # Step 3: Extract relevant fields from each <url> tag
        for url_tag in root.findall('ns:url', namespaces):
            employer_url = cm.extract_text(url_tag, 'ns:loc', namespaces)

            # Skip entries without employer URLs
            if not employer_url:
                continue

            employer_id = cm.extract_object_id(employer_url, pattern)
            changefreq = cm.extract_text(url_tag, 'ns:changefreq', namespaces)
            lastmod = cm.extract_text(url_tag, 'ns:lastmod', namespaces)

            list_url.append({
                "employer_id": employer_id,
                'employer_url': employer_url,
                'changefreq': changefreq,
                'lastmod': lastmod,
                "created_date": today,
                "worker": check_url_worker(employer_url)
            })

    except requests.exceptions.RequestException as e:
        print(f"Error occurred during request to URL {url}: {e}")
    except ET.ParseError as e:
        print(f"Error parsing XML from URL {url}: {e}")

    return list_url
       
def daily_employer_sitemap_process():
    """
    Crawls the employer sitemap and stores the data in MongoDB.
    - Fetches employer data from the sitemap URL.
    - Deletes existing records for today.
    - Inserts the new data into MongoDB.
    
    Args:
        None
        
    Returns:
        None
    """
    mongodb = None
    try:
        # Connect to MongoDB
        mongodb = connect_mongodb()

        # Crawling the sitemap
        sitemap_url = "https://careerviet.vn/sitemap/employer.xml"
        list_url = crawl_employer_sitemap(sitemap_url)
        if not list_url:
            print("No data retrieved from sitemap. Exiting process.")
            return

        # Delete current data for today
        delete_filter = {"created_date": today}
        mongodb.delete_many(delete_filter)

        # Insert the newly fetched data into MongoDB
        mongodb.insert_many(list_url)

        print("Employer sitemap data successfully processed and inserted into MongoDB.")

    except Exception as e:
        print(f"Error during daily employer sitemap process: {e}")

    finally:
        # Close the MongoDB connection if it was opened
        if mongodb:
            mongodb.close()
 
def daily_employer_sitemap_to_postgres():  
    """
    Transfers employer sitemap data from MongoDB to PostgreSQL.
    - Retrieves today's records from MongoDB.
    - Deletes existing data in PostgreSQL for today.
    - Inserts new records from MongoDB into PostgreSQL.
    
    Args:
        None
        
    Returns:
        None
    """
    mongodb = postgresdb = None
    try:
        # Connect to MongoDB
        mongodb = connect_mongodb()
        mongodb.set_collection(mongo_conn['cv_employer_sitemap'])
        filter = {"created_date": today}
        employer_docs = mongodb.select(filter)
        
        # Connect to PostgreSQL
        postgresdb = connect_postgresdb()

        # Delete current data from PostgreSQL
        condition_to_delete = {"created_date": today}
        deleted_rows = postgresdb.delete(postgres_conn['vnw_employer_sitemap'], condition_to_delete)
        print(f'Delete {deleted_rows} employer sitemap urls')

        # Load new data into PostgreSQL
        for doc in employer_docs:
            doc_id = doc.pop('_id', None)  # Remove MongoDB-specific ID
            inserted_id = postgresdb.insert(postgres_conn["cv_employer_sitemap"], doc, "employer_id")
            print("Inserting employer_id: ", inserted_id)
       
        # Close MongoDB connection
        mongodb.close()
        
        print("Data transferred successfully")
    except Exception as e:
        print(f"Error transferring data: {e}")
    
###########################################################################
#### 4. Employer detail process: crawl => mongodb => postgres
###########################################################################
 
def check_url_worker(employer_url):
    """
    Determines a worker ID based on the first character of the employer URL.

    This function extracts the first character following the base employer URL to assign a worker ID.
    If the character is 'c', worker ID 1 is assigned; otherwise, worker ID 2 is assigned.

    Args:
        employer_url (str): The URL of the employer page.

    Returns:
        int: The worker ID assigned based on the extracted character (1 or 2).
    """
    # Define base URL and extract the character after it
    base_url = 'https://careerviet.vn/vi/nha-tuyen-dung/'
    url_name = employer_url[len(base_url): len(base_url) + 1]  # Extracts the character following the base URL
    
    # Determine worker ID based on the character
    if url_name == 'c':
        return 1
    return 2

def crawl_employer_template(employer_url):
    """
    Crawl employer details from the given employer URL.

    Args:
        employer_url (str): URL of the employer page.

    Returns:
        dict: Dictionary containing the extracted employer data.
    """
    # Optional sleep for rate-limiting
    time.sleep(1)

    # Extract employer_id using regex
    match = re.search(pattern, employer_url)
    employer_id = match.group(1) if match else None
    if not employer_id:
        return None

    try:
        response = requests.get(employer_url, headers=headers)
        response.raise_for_status()  # Automatically raises an error for bad HTTP status codes

        # Parse content using BeautifulSoup
        soup = BeautifulSoup(response.content, 'html.parser')

        # Extract details
        company_info = soup.find('div', class_='company-info')
        employer_name = company_info.find('h1', class_='name').text.strip() if company_info and company_info.find('h1', class_='name') else None
        location = soup.find('div', class_='content').find('p').text.strip() if soup.find('div', class_='content') and soup.find('div', class_='content').find('p') else None

        company_size = industry = website = about_us = None
        li_tags = soup.find_all('li')
        for li in li_tags:
            if li.find('span', class_='mdi-account-supervisor'):
                company_size = li.find('span', class_='mdi-account-supervisor').text.strip()
            elif li.find('span', class_='mdi-gavel'):
                industry = li.find('span', class_='mdi-gavel').text.strip()
            elif li.find('span', class_='mdi-link'):
                website = li.find('span', class_='mdi-link').text.strip()

        intro_section = soup.find('div', class_='intro-section')
        about_us = intro_section.find('div', class_='box-text').text.strip() if intro_section and intro_section.find('div', class_='box-text') else None

        # Count current jobs listed
        list_job_section = soup.find('div', class_='list-job')
        total_current_jobs = len(list_job_section.find_all('div', class_='job-item')) if list_job_section else 0

        # Compile the employer data into a dictionary
        employer_data = {
            "employer_id": employer_id,
            "employer_name": employer_name,
            "location": location,
            "company_size": company_size,
            "industry": industry,
            "website": website,
            "about_us": about_us,
            "employer_url": employer_url,
            "created_date": today,
            "updated_date": today,
            "total_current_jobs": total_current_jobs,
            "worker": check_url_worker(employer_url),
        }

        return employer_data

    except requests.exceptions.RequestException as e:
        print(f"Error occurred while crawling URL {employer_url}: {e}")
        return None

def crawl_employer_worker(employer_url):
    """
    Crawl employer data from the provided URL and update or insert into MongoDB.

    Args:
        employer_url (str): URL of the employer page.
    """
    employer_id = cm.extract_object_id(employer_url, pattern)

    mongodb = connect_mongodb()    
    mongodb.set_collection(mongo_conn['cv_employer_detail'])

    employer = crawl_employer_template(employer_url)

    # Check if employer_id exists
    filter = {"employer_id": employer_id}
    existing_record = mongodb.select(filter)

    if existing_record:
        print("Update ", filter)
        # Remove the 'created_date' key from the dictionary for an update
        employer.pop("created_date", None)
        mongodb.update_one(filter, employer)
    else:
        print("Insert ", filter)
        mongodb.insert_one(employer)

    # Close the connection    
    mongodb.close()

def daily_employer_url_generator_airflow(worker):
    """
    Generate employer URLs, crawl them, and store results into MongoDB using Airflow.

    Args:
        worker (str): Worker identifier.

    Returns:
        None
    """
    mongodb = None
    try:
        # Connect to MongoDB and select the appropriate collection
        mongodb = connect_mongodb()
        mongodb.set_collection(mongo_conn['cv_employer_sitemap'])

        # Filter for URLs last modified today and assigned to the given worker
        filter = {
            "$or": [
                {"lastmod": today},
                {"created_date": today}
            ],
            "worker": worker
        }
        projection = {"_id": False, "employer_url": True}
        cursor = mongodb.select(filter, projection)

        # Limit to 5 URLs
        for count, document in enumerate(cursor, start=1):
            employer_url = document.get("employer_url")
            if employer_url:
                print(f"Crawling employer URL: {employer_url}")
                crawl_employer_worker(employer_url)

            # Break after 5 records
            if count >= cm.limited_item:
                break

    except Exception as e:
        print(f"Error during daily employer URL generation process: {e}")

    finally:
        # Close the MongoDB connection if it was opened
        if mongodb:
            mongodb.close()

def daily_load_employer_detail_to_postgres():    
    """
    Transfer employer details from MongoDB to PostgreSQL.

    - Fetches all employer details from MongoDB.
    - Truncates the PostgreSQL employer detail table.
    - Inserts all records from MongoDB into PostgreSQL.

    Args:
        None

    Returns:
        None
    """
    mongodb = postgresdb = None
    try:
        mongodb = connect_mongodb()
        mongodb.set_collection(mongo_conn['cv_employer_detail'])
        
        # Load all employer details from MongoDB
        employer_docs = mongodb.select()

        # Connect to PostgreSQL
        postgresdb = connect_postgresdb()

        # Truncate the target PostgreSQL table
        postgresdb.truncate_table(postgres_conn["cv_employer_detail"])

        # Insert documents into PostgreSQL
        for doc in employer_docs:
            doc.pop('_id', None)  # Remove MongoDB-specific ID
            inserted_id = postgresdb.insert(postgres_conn["cv_employer_detail"], doc, "employer_id")
            print(f"Inserting employer_id: {inserted_id}")

        # Close connections
        mongodb.close()
        postgresdb.close_pool()

        print("Data transferred successfully")

    except Exception as e:
        print(f"Error transferring data: {e}")
 
if __name__ == "__main__":  
    # daily_employer_url_generator_airflow(1)
    daily_employer_sitemap_process()
    daily_employer_sitemap_to_postgres()


    


