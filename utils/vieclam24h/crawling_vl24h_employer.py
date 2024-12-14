"""sitemap employer của cv chuẩn XML nên có thể dùng thư viện xml.etree.ElementTree
"""
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
pattern = r'(ntd\d+p\d+)\.html'

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
    Reads an XML URL containing URLs and extracts information from the sitemap.

    Args:
        url (str): The URL of the XML file containing URLs.

    Returns:
        list: A list of dictionaries containing information from the sitemap.
    """
    list_url = []

    try:
        # Fetch the XML sitemap
        response = requests.get(url=url, headers=headers)
        response.raise_for_status()  # Raise an exception for HTTP errors

        # Check for specific status codes
        if response.status_code == 410:
            print(f"Warning: XML resource might be unavailable (410 Gone).")
            return list_url

        # Parse the sitemap using ElementTree
        root = ET.fromstring(response.content)
        namespaces = {'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9'}  # Define XML namespaces

        # Extract relevant information from each <url> element
        for url_element in root.findall('ns:url', namespaces):
            employer_url = cm.extract_text(url_element, 'ns:loc', namespaces)
            if employer_url:
                employer_id = cm.extract_object_id(employer_url, pattern)
                # Extract other fields
                changefreq = cm.extract_text(url_element, 'ns:changefreq', namespaces)
                lastmod = cm.extract_text(url_element, 'ns:lastmod', namespaces)

                # Append to the list of URLs
                list_url.append({
                    "employer_id": employer_id,
                    "employer_url": employer_url,
                    "changefreq": changefreq,
                    "lastmod": lastmod,
                    "created_date": today,
                    "worker": check_url_worker(employer_url)
                })

        return list_url

    except requests.exceptions.RequestException as e:
        print(f"Error occurred: {str(e)}")
        return list_url     
    
def daily_employer_sitemap_process():
    """
    Process the pipeline to crawl and store employer sitemap URLs into MongoDB.

    This function deletes the current day's data from MongoDB and then crawls new sitemap URLs,
    saving the retrieved information back into MongoDB.

    Returns:
        None
    """
    mongodb = None
    try:
        # Connect to MongoDB
        mongodb = connect_mongodb()

        # Delete current data
        delete_filter = {"created_date": today}
        mongodb.delete_many(delete_filter)

        # Sitemap URLs to crawl
        sitemap_urls = [
            "https://cdn1.vieclam24h.vn/file/sitemap/employer/congty-0.xml",
            "https://cdn1.vieclam24h.vn/file/sitemap/employer/congty-1.xml"
        ]

        # Crawl and insert data for each sitemap URL
        for sitemap_url in sitemap_urls:
            list_url = crawl_employer_sitemap(sitemap_url)
            if list_url:
                mongodb.insert_many(list_url)

        print("Sitemap data processed and stored successfully.")

    except Exception as e:
        print(f"Error processing employer sitemap: {e}")

    finally:
        # Ensure the MongoDB connection is closed properly
        if 'mongodb' in locals() and mongodb:
            mongodb.close()

def daily_employer_sitemap_to_postgres():
    """
    Process the pipeline to transfer employer sitemap data from MongoDB to PostgreSQL.

    Returns:
        None
    """
    mongodb = postgresdb = None
    try:
        # Connect to MongoDB and select collection
        mongodb = connect_mongodb()
        mongodb.set_collection(mongo_conn['vl24h_employer_sitemap'])

        # Filter data to transfer
        filter = {"created_date": today}
        employer_docs = mongodb.select(filter)

        # Connect to PostgreSQL
        postgresdb = connect_postgresdb()

        # Delete current data in PostgreSQL
        condition_to_delete = {"created_date": today}
        deleted_rows = postgresdb.delete(postgres_conn['vl24h_employer_sitemap'], condition_to_delete)
        print(f"Deleted {deleted_rows} employer sitemap URLs")

        # Load new data from MongoDB to PostgreSQL
        for doc in employer_docs:
            doc.pop('_id', None)  # Remove MongoDB specific ID
            inserted_id = postgresdb.insert(postgres_conn["vl24h_employer_sitemap"], doc, "employer_id")
            print(f"Inserting employer_id: {inserted_id}")

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
 
def check_url_worker(employer_url):
    """
    Determines the worker ID based on the employer URL.

    This function checks if a specific substring is present in the given employer URL.
    If the substring is found, it returns worker ID 1, otherwise it returns worker ID 2.

    Args:
        employer_url (str): The URL of the employer.

    Returns:
        int: The worker ID, either 1 or 2.
            - 1: If the URL contains 'https://vieclam24h.vn/danh-sach-tin-tuyen-dung-cong-ty-tnhh'.
            - 2: Otherwise.
    """
    return 1 if 'https://vieclam24h.vn/danh-sach-tin-tuyen-dung-cong-ty-tnhh' in employer_url else 2
    
def crawl_employer_worker(employer_url):
    """
    Crawls an employer's details from the provided URL and saves to MongoDB.

    Args:
        employer_url (str): The employer's URL.

    Returns:
        None
    """
    time.sleep(1)  # Adding delay to prevent overwhelming the server
    employer_id = cm.extract_object_id(employer_url, pattern)

    try:
        # Fetch the employer page content
        response = requests.get(url=employer_url, headers=headers)
        response.raise_for_status()  # Automatically handle HTTP errors

        # Parse the page content with BeautifulSoup
        parser = 'html.parser'
        soup = BeautifulSoup(response.content, parser)

        # Extract employer information
        employer_name = soup.find('h1', id='qc-name-company').get_text(strip=True) if soup.find('h1', id='qc-name-company') else None

        website = None
        website_span = soup.find('span', id='qc-website-company')
        if website_span:
            website_parent = website_span.find_parent()
            if website_parent and 'title' in website_parent.attrs:
                website = website_parent['title'].strip()

        about_us = soup.find('div', id='qc-content-introduction').get_text(strip=True) if soup.find('div', id='qc-content-introduction') else None
        total_current_jobs = len(soup.select('div#qc-box-recruiting a')) if soup.find('div', id='qc-box-recruiting') else None

        # Extract location, company size, and industry from the h3 elements
        location = company_size = industry = None
        h3_elements = soup.select('#qc-box-communications > div > div:nth-child(2) > h3')
        for h3 in h3_elements:
            h3_text = h3.get_text(strip=True)
            if "Địa chỉ:" in h3_text:
                location = h3_text.replace('Địa chỉ:', '').strip()
            elif "Quy mô:" in h3_text:
                company_size = h3_text.replace('Quy mô:', '').strip()
            elif "Lĩnh vực:" in h3_text:
                industry = h3_text.replace('Lĩnh vực:', '').strip()

        # Create the employer dictionary
        employer = {
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
            "worker": check_url_worker(employer_url)
        }

        # MongoDB Operations
        mongodb = connect_mongodb()
        mongodb.set_collection(mongo_conn['vl24h_employer_detail'])

        # Check if the employer document already exists and either update or insert
        filter = {"employer_id": employer_id}
        existing_doc = mongodb.select(filter)

        if existing_doc:
            print("Update", filter)
            employer.pop("created_date", None)  # Remove 'created_date' for update
            mongodb.update_one(filter, employer)
        else:
            print("Insert", filter)
            mongodb.insert_one(employer)

        # Close the MongoDB connection
        mongodb.close()

    except requests.exceptions.RequestException as e:
        print(f"Error occurred: {str(e)}")
    
def daily_employer_url_generator_airflow(worker):
    """
    Crawl all employer data from the sitemap and store it into MongoDB using Airflow.

    Args:
        worker (str): Identifier for the worker handling the task.

    Returns:
        None
    """
    mongodb = None
    try:
        # Connect to MongoDB and set the collection
        mongodb = connect_mongodb()
        mongodb.set_collection(mongo_conn['vl24h_employer_sitemap'])

        # Define filter and projection for the query
        filter = {"lastmod": today, "worker": worker}
        projection = {"_id": False, "employer_url": True}

        # Fetch employer URLs to crawl data
        cursor = mongodb.select(filter, projection)

        # Process up to 5 employer URLs
        for count, document in enumerate(cursor):
            if count >= cm.limited_item:
                break

            employer_url = document.get("employer_url")
            if employer_url:
                print(f"Crawling employer URL: {employer_url}")
                crawl_employer_worker(employer_url)

    except Exception as e:
        print(f"Error occurred during employer URL generation: {e}")

    finally:
        # Close MongoDB connection if it was established
        if 'mongodb' in locals() and mongodb:
            mongodb.close()

def daily_load_employer_detail_to_postgres():
    """
    Process the pipeline to transfer employer details from MongoDB to PostgreSQL using Airflow.

    Returns:
        None
    """
    mongodb = postgresdb = None
    try:
        # Connect to MongoDB and select the appropriate collection
        mongodb = connect_mongodb()
        mongodb.set_collection(mongo_conn['vl24h_employer_detail'])

        # Load full data from MongoDB
        employer_docs = mongodb.select()

        # Connect to PostgreSQL and truncate existing table data
        postgresdb = connect_postgresdb()
        postgresdb.truncate_table(postgres_conn["vl24h_employer_detail"])

        # Insert each document into PostgreSQL
        for doc in employer_docs:
            doc.pop('_id', None)  # Remove MongoDB specific ID
            inserted_id = postgresdb.insert(postgres_conn["vl24h_employer_detail"], doc, "employer_id")
            print(f"Inserting employer_id: {inserted_id}")

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


if __name__ == "__main__":  
    # daily_employer_sitemap_process()
    # daily_employer_sitemap_to_postgres()
    employer_url = 'https://vieclam24h.vn/danh-sach-tin-tuyen-dung-sieu-viet-group-ntd2411779p122.html'
    # crawl_employer_worker(employer_url)
    daily_employer_sitemap_process()
    daily_employer_sitemap_to_postgres()
    # daily_load_employer_detail_to_postgres()
     

    


