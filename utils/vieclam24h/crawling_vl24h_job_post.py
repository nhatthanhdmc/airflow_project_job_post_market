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
                        collection_name = mongo_conn['vl24h_job_post_sitemap'],
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
        List of URLs and associated metadata.
    """    
    pattern = r'/(\d+)$'
    namespaces = {'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
    headers = {"User-Agent": "Mozilla/5.0"}  # Add your user agent here
    list_url = []

    try:
        # Step 1: Fetch the sitemap
        response = requests.get(url=url, headers=headers)
        response.raise_for_status()  # Automatically raise an exception for 4xx/5xx responses
        
        if response.status_code == 410:
            print(f"Warning: XML resource might be unavailable (410 Gone).")
            return list_url  # Exit the function if it's a 410 error
        
        # Step 2: Parse the sitemap using ElementTree
        root = ET.fromstring(response.content)
        print("Sitemap fetched and parsed successfully.")

        # Step 3: Extract relevant fields from each <url> tag
        for url_tag in root.findall('ns:url', namespaces):
            job_url = extract_text(url_tag, 'ns:loc', namespaces)
            job_id = re.search(pattern, job_url).group(1) if job_url else None
            
            list_url.append({
                'job_url': job_url,
                'job_id': job_id,
                'changefreq': extract_text(url_tag, 'ns:changefreq', namespaces),
                'lastmod': extract_text(url_tag, 'ns:lastmod', namespaces),
                'priority': extract_text(url_tag, 'ns:priority', namespaces),
                'created_date': datetime.today(),
                'worker': check_url_worker(job_url)
            })
            
        return list_url
    
    except requests.exceptions.RequestException as e:
        print(f"Error occurred during request: {str(e)}")
    except ET.ParseError as e:
        print(f"Error occurred during XML parsing: {str(e)}")
    
    return list_url 

def daily_job_post_sitemap_process(sitemap_urls=None):
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
    
    sitemap_urls = ["https://cdn1.vieclam24h.vn/file/sitemap/job/tintuyendung-0.xml",\
                    "https://cdn1.vieclam24h.vn/file/sitemap/job/tintuyendung-1.xml",\
                    "https://cdn1.vieclam24h.vn/file/sitemap/job/tintuyendung-2.xml",\
                    "https://cdn1.vieclam24h.vn/file/sitemap/job/tintuyendung-3.xml"]
    for sitemap_url in sitemap_urls:
        list_url = crawl_job_post_sitemap(sitemap_url)        
        # Load current data
        mongodb.insert_many(list_url)   
    
    # Close the connection    
    mongodb.close()
    
def daily_job_post_sitemap_to_postgres():     
    mongodb = postgresdb = None
    try:
        mongodb = connect_mongodb()
        mongodb.set_collection(mongo_conn['vl24h_job_post_sitemap']) 
        filter = {"created_date": today}
        employer_docs = mongodb.select(filter)
        
        postgresdb = connect_postgresdb()
        # delete current data
        condition_to_delete = {"created_date": today}
        deleted_rows = postgresdb.delete(postgres_conn['vl24h_job_post_sitemap'], condition_to_delete)
        # load current data
        print(f'Delete {deleted_rows} employer sitemap urls')
        # load new data
        for doc in employer_docs:
            doc_id = doc.pop('_id', None)  # Remove MongoDB specific ID
            inserted_id = postgresdb.insert(postgres_conn["vl24h_job_post_sitemap"], doc, "job_id")
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

def get_specific_text(elements, keyword, date_format=None):
    """
    Extracts text from an element matching the given keyword.

    Args:
        elements (list): List of BeautifulSoup elements to search.
        keyword (str): Keyword to identify the desired element.
        date_format (str, optional): Format to parse the text as a datetime.

    Returns:
        str, datetime, or None: The extracted text, parsed datetime, or None if not found.
    """
    specific_element = next((el for el in elements if keyword in el.text), None)
    if specific_element:
        text = specific_element.find_all('p')[1].text.strip()
        return datetime.strptime(text, date_format) if date_format else text
    return None

def crawl_job_post_template(soup, job_url):
    """
    Crawl a job with template 1
    Args: 
        job_url (string): job url
    Returns: job (json)
    """ 
    # Initialize job attributes as None
    job = {
        "job_id": None, "job_url": job_url, "job_title": None, "location": None,
        "company_url": None, "updated_date_on_web": None, "industry": None, "field": None,
        "job_type": None, "salary": None, "experience": None, "job_level": None,
        "deadline": None, "benefit": None, "job_description": None, "job_requirement": None,
        "more_information": None, "created_date": datetime.today(), "total_views": None,
        "posted_date": None, "probation_time": None, "num_of_recruitments": None,
        "working_type": None, "qualifications": None, "worker": check_url_worker(job_url)
    }
    
    # Extract job_id from job_url
    job_id_match = re.search(r'/(\d+)$', job_url)
    job["job_id"] = job_id_match.group(1) if job_id_match else None

    # PART 1: TOP - Extract <h1> title, <h2> salary and deadline, and updated date
    job["job_title"] = soup.find('h1').text.strip() if soup.find('h1') else None

    h2_elements = soup.find_all('h2', class_=['text-14', 'leading-6'])
    job["salary"] = get_specific_text(h2_elements, 'Mức lương')
    job["deadline"] = get_specific_text(h2_elements, 'Hạn nộp hồ sơ')
    job["updated_date_on_web"] = (
        soup.find('span', class_='font-semibold').text.strip()
        if soup.find('span', class_='font-semibold') else None
    )

    # PART 2: BODY - Map <h3> fields to attributes
    h3_elements = soup.find_all('h3', class_='ml-3')
    h3_mappings = {
        'Ngày đăng': ("posted_date", r"%d/%m/%Y"),
        'Thời gian thử việc': "probation_time",
        'Cấp bậc': "job_level",
        'Số lượng tuyển': "num_of_recruitments",
        'Hình thức làm việc': "working_type",
        'Yêu cầu bằng cấp': "qualifications",
        'Yêu cầu kinh nghiệm': "experience",
        'Ngành nghề': "industry"
    }

    for keyword, info in h3_mappings.items():
        if isinstance(info, tuple):  # If it includes date format
            job[info[0]] = get_specific_text(h3_elements, keyword, date_format=info[1])
        else:
            job[info] = get_specific_text(h3_elements, keyword)

    # Extract job description, requirements, and benefits using a helper function
    def get_section_text(soup, section_title):
        return '\n'.join(
            li.text.strip() for div in soup.find_all('div', class_="jsx-5b2773f86d2f74b")
            if div.find('h2') and section_title in div.find('h2').text.strip()
            for li in div.find_all('li')
        )

    job["job_description"] = get_section_text(soup, 'Mô tả công việc')
    job["job_requirement"] = get_section_text(soup, 'Yêu cầu công việc')
    job["benefit"] = get_section_text(soup, 'Quyền lợi')

    # Extract location if present
    location_div = next(
        (div for div in soup.find_all('div', class_="jsx-5b2773f86d2f74b") if 'Địa điểm làm việc' in div.text),
        None
    )
    job["location"] = location_div.find_all('span')[1].text.strip() if location_div else None

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
            mongodb.set_collection(mongo_conn['vl24h_job_post_detail'])
            
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
    mongodb.set_collection(mongo_conn['vl24h_job_post_sitemap'])
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
        mongodb.set_collection(mongo_conn['vl24h_job_post_detail']) 
        # load full
        employer_docs = mongodb.select()
        
        postgresdb = connect_postgresdb()
        # truncate
        postgresdb.truncate_table(postgres_conn["vl24h_job_post_detail"])
        # load full
        for doc in employer_docs:
            doc_id = doc.pop('_id', None)  # Remove MongoDB specific ID
            inserted_id = postgresdb.insert(postgres_conn["vl24h_job_post_detail"], doc, "job_id")
            print("Inserting job_id: ", inserted_id)
       
        # close connection
        mongodb.close()
        postgresdb.close_pool()
        print("Data transferred successfully")
    except Exception as e:
        print(f"Error transferring data: {e}")   
        
          
if __name__ == "__main__":  
    # Process sitemap
    job_url = 'https://vieclam24h.vn/short/job/3129570'
    crawl_job_post_worker(job_url)
    daily_load_job_post_detail_to_postgres()