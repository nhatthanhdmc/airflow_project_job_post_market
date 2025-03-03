"""
Cấu trúc sitemap của careerviet không chuẩn XML nên có lỗi khi dùng ElementTree để parse sang cấu trúc XML.
Sẽ có lỗi xml.etree.ElementTree.ParseError: mismatched tag
Chi tiết:
The xml.etree.ElementTree.ParseError: mismatched tag error indicates that the XML content is not well-formed
, meaning that there is a syntax issue in the XML document (such as an unclosed tag or a tag mismatch).
To handle this, we can first ensure the XML content is fetched correctly and then use BeautifulSoup with 
the xml parser to handle potential issues with malformed XML.
"""
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
mongodb = None

# Get current date in YYYY-MM-DD format
today = date.today().strftime("%Y-%m-%d")  
mongo_conn = cfg.mongodb['CRAWLING']
postgres_conn = cfg.postgres['DWH']
pattern_job_id = r'\.([A-Z0-9]+)\.html'
pattern_employer_id = r'\.([A-Z0-9]+)\.html'
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
                        collection_name = mongo_conn['cv_job_post_sitemap'],
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
    Determines a worker ID based on the first character of the job URL.

    This function extracts a character from the job URL to assign a worker ID.
    If the character falls between 'a' to 'l', worker ID 1 is assigned;
    otherwise, worker ID 2 is assigned.

    Args:
        job_url (str): The URL of the job posting.

    Returns:
        int: The worker ID assigned based on the extracted character (1 or 2).
    """
    # Extract a character after the base job URL prefix
    base_url = 'https://careerviet.vn/vi/tim-viec-lam/'
    url_name = job_url[len(base_url): len(base_url) + 1]  # Extracts the character at the expected position
    
    # Determine worker ID based on the character
    if url_name in 'abcdefghigkl':
        return 1
    return 2

def crawl_job_post_sitemap(sitemap_url):
    """
    Reads an XML URL containing job URLs and extracts the necessary information.

    Args:
        sitemap_url (str): The URL of the XML sitemap.

    Returns:
        list: A list of dictionaries, each containing data about a job post.
    """
    list_url = []
    try:
        response = requests.get(sitemap_url, headers=headers)
        response.raise_for_status()  # Automatically raise an error for bad HTTP status codes

        # Parse the sitemap content with BeautifulSoup
        soup = BeautifulSoup(response.content, "xml")
        list_item = soup.find_all('url')

        for item in list_item:
            job_url = cm.extract_text(item, 'loc')
            job_id = cm.extract_object_id(job_url, pattern_job_id)
            image = cm.extract_text(item, 'image:loc')
            changefreq = cm.extract_text(item, 'changefreq')
            lastmod = cm.extract_text(item, 'lastmod')
            priority = cm.extract_text(item, 'priority')

            list_url.append({
                "job_id": job_id,
                "job_url": job_url,
                "image": image,
                "changefreq": changefreq,
                "lastmod": lastmod,
                "priority": priority,
                "created_date": today,
                "worker": check_url_worker(job_url)
            })

    except requests.exceptions.RequestException as e:
        print(f"Error occurred while fetching the sitemap from {sitemap_url}: {e}")
    
    return list_url      

def daily_job_post_sitemap_process():
    """
    Crawls sitemap URL and stores the data into MongoDB.

    This function retrieves job posts from the sitemap, deletes any existing records 
    for the current date, and then inserts the newly fetched data into MongoDB.

    Args:
        None

    Returns:
        None
    """
    mongodb = None
    try:
        # Connect to MongoDB
        mongodb = connect_mongodb()

        # Crawling sitemap
        sitemap_url = "https://careerviet.vn/sitemap/job_vi.xml"
        list_url = crawl_job_post_sitemap(sitemap_url)

        # Delete current data for today
        delete_filter = {"created_date": today}
        mongodb.delete_many(delete_filter)

        # Insert the newly fetched data into MongoDB
        mongodb.insert_many(list_url)

        print("Sitemap data successfully processed and inserted into MongoDB.")

    except Exception as e:
        print(f"Error during daily job post sitemap process: {e}")

    finally:
        # Close MongoDB connection if it was opened
        if mongodb:
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
        mongodb.set_collection(mongo_conn['cv_job_post_sitemap'])
        
        # Retrieve job post documents created today from MongoDB
        filter = {"created_date": today}
        employer_docs = mongodb.select(filter)

        # Connect to PostgreSQL
        postgresdb = connect_postgresdb()

        # Delete existing records for the current date from PostgreSQL
        condition_to_delete = {"created_date": today}
        deleted_rows = postgresdb.delete(postgres_conn['cv_job_post_sitemap'], condition_to_delete)
        print(f"Deleted {deleted_rows} job post sitemap URLs")

        # Insert new records into PostgreSQL
        for doc in employer_docs:
            doc.pop('_id', None)  # Remove MongoDB-specific ID
            inserted_id = postgresdb.insert(postgres_conn["cv_job_post_sitemap"], doc, "job_id")
            print(f"Inserting job_id: {inserted_id}")

        # Print success message
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
    
def crawl_job_post_template1(soup, job_url):
    """
    Crawl a job post from a given URL with template 1.

    Args:
        soup (BeautifulSoup object): Parsed HTML content of the job post page using BeautifulSoup.
        job_url (str): The URL of the job post.

    Returns:
        dict: A dictionary containing job details.
    """
    # Initialize job dictionary with default None values
    job = {
        "job_id": None,
        "job_url": job_url,
        "job_title": None,
        "company_url": None,
        "updated_date_on_web": None,
        "industry": None,
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
        "updated_date": today,
        "worker": check_url_worker(job_url),
        "employer_id": None
    }

    # Extract job ID using the URL
    job["job_id"] = cm.extract_object_id(job_url, pattern_job_id)

    # PART 1: Extract TOP section (Job title, Company URL)
    head_left = soup.find('div', class_='head-left')
    if head_left:
        job["job_title"] = cm.extract_text(head_left, 'h2', 'title')
        company_link = head_left.find('a')
        job["company_url"] = company_link.get('href') if company_link else None
        job["employer_id"] = cm.extract_object_id(job["company_url"], pattern_employer_id) if job["company_url"] else None
    # PART 2: Extract BODY section (Industry, Salary, Job Type, etc.)
    body = soup.find('div', class_='body-template')
    if body:
        content = body.find('div', class_='content')
        if content:
            tr_tags = content.find_all('tr')
            for tr in tr_tags:
                icon_class = tr.find('em')['class'][0] if tr.find('em') else ''
                td_content = tr.find('td', class_='content')
                if td_content:
                    if 'fa-id-badge' in icon_class:
                        job["industry"] = ' '.join(a.text.strip() for a in td_content.find_all('a'))
                    elif 'fa-usd' in icon_class:
                        job["salary"] = cm.extract_text(td_content, 'strong')
                    elif 'mdi-briefcase-edit' in icon_class:
                        job["job_type"] = cm.extract_text(td_content, 'p')
                    elif 'mdi-account' in icon_class:
                        job["job_level"] = cm.extract_text(td_content, 'p')
                    elif 'fa-briefcase' in icon_class:
                        job["experience"] = re.sub(r'\s+', ' ', cm.extract_text(td_content, 'p'))
                    elif 'fa-calendar-times-o' in icon_class:
                        job["deadline"] = cm.extract_text(td_content, 'p')
                    elif 'fa-calendar' in icon_class:
                        job["updated_date_on_web"] = cm.extract_text(td_content, 'p')

    # PART 3: Extract BOTTOM section (Job Description, Requirements, More Information)
    bottom = soup.find('div', class_='bottom-template')
    if bottom:
        full_content = bottom.find('div', class_='full-content')
        if full_content:
            div_tags = full_content.find_all('div', class_='detail-row')
            if len(div_tags) > 2:
                job["job_description"] = ';'.join(re.sub(r'\s+', ' ', li.get_text(strip=True)) for li in div_tags[1].find_all('p'))
                job["job_requirement"] = ';'.join(re.sub(r'\s+', ' ', li.get_text(strip=True)) for li in div_tags[2].find_all('p'))
                job["more_information"] = ';'.join(re.sub(r'\s+', ' ', li.get_text(strip=True)) for li in div_tags[3].find_all('li'))

    return job

def crawl_job_post_template2(soup, job_url):
    """
    Crawls a job post with Template 2 format and returns job details in JSON format.

    Args:
        soup (BeautifulSoup object): Parsed HTML content of the job post page using BeautifulSoup.
        job_url (str): URL of the job post.

    Returns:
        dict: A dictionary containing the job details.
    """
    # Initialize job dictionary with default None values
    job = {
        "job_id": None,
        "job_url": job_url,
        "job_title": None,
        "company_url": None,
        "location": None,
        "updated_date_on_web": None,
        "industry": None,
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
        "updated_date": today,
        "worker": check_url_worker(job_url),
        "employer_id": None
    }

    # Extract job ID using the URL
    job["job_id"] = cm.extract_object_id(job_url, pattern_job_id)

    # PART 1: Extract Job Title, Company URL
    job_desc = soup.find('div', class_='job-desc')
    if job_desc:
        job_title = job_desc.find('h1')
        if job_title:
            job["job_title"] = job_title.get_text(strip=True)

        company_link = job_desc.find('a')
        if company_link:
            job["company_url"] = company_link.get('href')
            job["employer_id"] = cm.extract_object_id(job["company_url"], pattern_employer_id) if job["company_url"] else None
    # PART 2: OVERVIEW Section Extraction
    job_detail_content = soup.find('div', id='tab-1')
    if job_detail_content:
        overview_section = job_detail_content.find('section', class_='job-detail-content')
        if overview_section:
            overview_div_tags = overview_section.find('div', class_='bg-blue').find_all('div', class_='col-lg-4 col-sm-6 item-blue')

            if len(overview_div_tags) > 2:
                # Extract Location
                location_div = overview_div_tags[0].find('a', class_='map')
                if location_div:
                    job["location"] = location_div.get_text(strip=True)

                # Extract Updated Date, Industry, Job Type
                li_tags_1 = overview_div_tags[1].find_all('li')
                for li in li_tags_1:
                    icon = li.find('em')
                    if icon:
                        icon_class = icon.get('class', [None])[0]
                        content_p = li.find('p')
                        if icon_class == 'mdi-update' and content_p:
                            job["updated_date_on_web"] = content_p.get_text(strip=True)
                        elif icon_class == 'mdi-briefcase' and content_p:
                            job["industry"] = ' '.join(content_p.get_text(strip=True).split())
                        elif icon_class == 'mdi-briefcase-edit' and content_p:
                            job["job_type"] = content_p.get_text(strip=True)

                # Extract Salary, Experience, Job Level, Deadline
                li_tags_2 = overview_div_tags[2].find_all('li')
                for li in li_tags_2:
                    icon = li.find('i')
                    if icon:
                        icon_class = icon.get('class', [None])[0]
                        content_p = li.find('p')
                        if icon_class == 'fa-usd' and content_p:
                            job["salary"] = content_p.get_text(strip=True)
                        elif icon_class == 'fa-briefcase' and content_p:
                            job["experience"] = re.sub(r'\s+', ' ', content_p.get_text(strip=True))
                        elif icon_class == 'mdi-account' and content_p:
                            job["job_level"] = content_p.get_text(strip=True)
                        elif icon_class == 'mdi-calendar-check' and content_p:
                            job["deadline"] = content_p.get_text(strip=True)

    # PART 3: DETAIL Section Extraction
    if job_detail_content:
        detail_div_tags = job_detail_content.find_all('div', class_='detail-row')

        if len(detail_div_tags) > 3:
            # Extract Benefits, Job Description, Job Requirements, and More Information
            job["benefit"] = ';'.join(re.sub(r'\s+', ' ', li.get_text(strip=True)) for li in detail_div_tags[0].find_all('li'))
            job["job_description"] = ';'.join(re.sub(r'\s+', ' ', p.get_text(strip=True)) for p in detail_div_tags[1].find_all('p'))
            job["job_requirement"] = ';'.join(re.sub(r'\s+', ' ', p.get_text(strip=True)) for p in detail_div_tags[2].find_all('p'))
            job["more_information"] = ';'.join(re.sub(r'\s+', ' ', li.get_text(strip=True)) for li in detail_div_tags[3].find_all('li'))

    return job

def crawl_job_post_worker(job_url):
    """
    Crawls job post details from a given job URL and stores or updates them in MongoDB.

    Args:
        job_url (str): The URL of the job post to crawl.

    Returns:
        None
    """
    time.sleep(1)  # Rate-limiting to prevent overloading the server
    try:
        response = requests.get(url=job_url, headers=headers)
        parser = 'html.parser'

        if response.status_code == 410:
            print(f"Warning: XML resource might be unavailable (410 Gone).")
            return  # Exit the function if it's a 410 error
        elif response.status_code != 200:
            raise Exception(f"Failed to fetch XML: {response.status_code}, URL: {job_url}")

        # Parse the job post
        soup = BeautifulSoup(response.content, parser)
        job = {}

        # Determine job template and extract data
        if soup.find('section', class_='search-result-list-detail template-2'):
            job = crawl_job_post_template2(soup, job_url)
        elif soup.find('section', class_='template01-banner'):
            job = crawl_job_post_template1(soup, job_url)

        # Connect to MongoDB
        mongodb = connect_mongodb()
        mongodb.set_collection(mongo_conn['cv_job_post_detail'])

        if job:
            filter = {"job_id": job["job_id"]}

            # Update or insert job post
            if mongodb.select(filter):
                print("Update", filter)
                job.pop("created_date", None)  # Remove the 'created_date' key if exists
                mongodb.update_one(filter, job)
            else:
                print("Insert", filter)
                mongodb.insert_one(job)

            # Close the MongoDB connection    
            mongodb.close()

    except requests.exceptions.RequestException as e:
        print(f"Error occurred: {str(e)}")

def daily_job_url_generator_airflow(worker):
    """
    Generates job URLs assigned to a worker and crawls job posts using Airflow.

    This function fetches job URLs assigned to a specific worker, crawls each URL, 
    and stores the job post details into MongoDB.

    Args:
        worker (str): Worker identifier for assigning jobs.

    Returns:
        None
    """
    mongodb = connect_mongodb()
    mongodb.set_collection(mongo_conn['cv_job_post_sitemap'])

    # Filter for URLs assigned to the worker and created today
    filter = {
            "$or": [
                {"lastmod": today},
                {"created_date": today}
            ],
            "worker": worker
        }
    # filter = {"created_date": today, "worker": worker}
    projection = {"_id": False, "job_url": True}
    cursor = mongodb.select(filter, projection)

    # Crawl and process each job URL
    count = 0
    for document in cursor:
        job_url = document.get("job_url")
        if job_url:
            print(job_url)
            crawl_job_post_worker(job_url)
            count += 1
            if count >= cm.limited_item:
                break

    # Close the MongoDB connection
    mongodb.close()

def delete_duplicate_job_post_detail():
    """
    Deletes duplicate job post details in MongoDB.

    This function removes duplicate job post records from MongoDB based on specified key fields.
    Only records created on or after June 1, 2024, are considered for duplicate deletion.

    Returns:
        None
    """
    mongodb = None
    try:
        # Connect to MongoDB
        mongodb = connect_mongodb()
        mongodb.set_collection(mongo_conn['cv_job_post_detail'])

        # Define keys to identify duplicates and condition to filter documents
        key_fields = ["job_id", "job_title"]
        condition = {"created_date": {"$gte": "2024-06-01"}}

        # Delete duplicates
        mongodb.delete_duplicates_with_condition(key_fields, condition)

        print("Duplicate job post details deleted successfully.")

    except Exception as e:
        print(f"Error deleting duplicate job post details: {e}")

    finally:
        # Ensure the MongoDB connection is closed properly
        if mongodb:
            mongodb.close()

def daily_load_job_post_detail_to_postgres():
    """
    Transfers job post details from MongoDB to PostgreSQL using Airflow.

    This function loads all job post details from MongoDB and transfers the data to PostgreSQL.
    It first truncates the existing data in the target PostgreSQL table before inserting new records.

    Args:
        None

    Returns:
        None
    """
    mongodb = postgresdb = None
    try:
        # Connect to MongoDB
        mongodb = connect_mongodb()
        mongodb.set_collection(mongo_conn['cv_job_post_detail'])

        # Load all job post details from MongoDB
        employer_docs = mongodb.select()

        # Connect to PostgreSQL
        postgresdb = connect_postgresdb()

        # Truncate the target PostgreSQL table
        postgresdb.truncate_table(postgres_conn["cv_job_post_detail"])

        # Insert documents into PostgreSQL
        for doc in employer_docs:
            doc.pop('_id', None)  # Remove MongoDB specific ID
            inserted_id = postgresdb.insert(postgres_conn["cv_job_post_detail"], doc, "job_id")
            print(f"Inserting job_id: {inserted_id}")

        # Print success message
        print("Data transferred successfully")

    except Exception as e:
        print(f"Error transferring data: {e}")

    finally:
        # Ensure connections are properly closed
        if mongodb:
            mongodb.close()
        if postgresdb:
            postgresdb.close_pool()
       
# if __name__ == "__main__": 
#     daily_load_job_post_detail_to_postgres() 
    # delete_duplicate_job_post_detail()
#     # Process sitemap
#     job_post_sitemap_process()     
    
#     # Craw current jobs process
#     start_time = time.time()
#     current_job_post_process()
#     print('Execution time: ', time.time()-start_time)
    
#     # delete_duplicate_job_post_detail()    
    
    
# mongodb = connect_mongodb()
# mongodb.set_collection(mongo_conn['cv_job_post_detail'])
#     # Delete duplicates based on specified key fields
# key_fields = ["job_id", "job_title"]  # Fields to identify duplicates
# condition = {"created_date": {"$gte": "2024-06-01"}}  # Condition to filter documents
# mongodb.delete_duplicates_with_condition(key_fields, condition)
# mongodb.close()    
    
    


