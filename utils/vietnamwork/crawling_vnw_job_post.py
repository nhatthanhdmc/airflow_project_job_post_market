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
from utils import config as cfg
from datetime import date
import re
import time
import multiprocessing
import xml.etree.ElementTree as ET


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
                        collection_name = conn['vnw_job_post_sitemap'],
                        host = conn['host'], 
                        port = conn['port'], 
                        username = conn['username'], 
                        password = conn['password']
                    )
    mongodb.connect()
    
    return mongodb

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
    pattern = r'\-([0-9]+)\-'
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
                    'priority': priority.text.strip() if priority is not None else None
                })
                
        return list_url    
    except requests.exceptions.RequestException as e:
        print( f"Error occurred: {str(e)}")         
    
def job_post_sitemap_process():
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
    
     # Delete current data
    delete_filter = {"created_date": today}
    mongodb.delete_many(delete_filter)
    
    # Load current data
    mongodb.insert_many(list_url)
    
    # Close the connection    
    mongodb.close()
    
if __name__ == "__main__":  
    # Process sitemap
    job_post_sitemap_process()