"""
Cấu trúc sitemap của careerviet không chuẩn XML nên có lỗi khi dùng ElementTree để parse sang cấu trúc XML.
Sẽ có lỗi xml.etree.ElementTree.ParseError: mismatched tag
Chi tiết:
The xml.etree.ElementTree.ParseError: mismatched tag error indicates that the XML content is not well-formed
, meaning that there is a syntax issue in the XML document (such as an unclosed tag or a tag mismatch).
To handle this, we can first ensure the XML content is fetched correctly and then use BeautifulSoup with 
the xml parser to handle potential issues with malformed XML.
"""
# pwd
# export PYTHONPATH="${PYTHONPATH}:/home/thanhnn/airflow_project"
import multiprocessing.pool
import os
import sys 

module_path = os.path.abspath(os.getcwd())
print(module_path)
# Checks if module_path is already in the sys.path list. The sys.path list contains directories that the Python interpreter will search for modules to import.
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
import random
import multiprocessing

# import utils.mongodb_connection.MongoDB 

"""
Global variable
"""
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Accept-Encoding": "*",
    "Connection": "keep-alive"
}
mongodb = None

# Get current date in YYYY-MM-DD format
today = date.today().strftime("%Y-%m-%d")  

conn = cfg.mongodb['CRAWLING']

import random
def get_random_proxy(proxies):
    return random.choice(proxies)

"""
Return a connection to mongodb
Args: None
Returns: mongodb
"""
def connect_mongodb():   
        
    mongodb = MongoDB(  dbname = conn['dbname'], 
                        collection_name = conn['cv_sitemap_job'],
                        host = conn['host'], 
                        port = conn['port'], 
                        username = conn['username'], 
                        password = conn['password']
                    )
    mongodb.connect()
    
    return mongodb
  
"""
Crawling data of sitemap URL
Args: 
    url (string): site map url
Returns: list of url & related data
"""  
def crawl_sitemap(url):
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
        response = requests.get(url = url, 
                                headers = headers)
        
        if response.status_code == 410:
            print(f"Warning: XML resource might be unavailable (410 Gone).")
            return  # Exit the function if it's a 410 error
        elif response.status_code != 200:
            raise Exception(f"Failed to fetch XML: {response.status_code}")
        elif response.status_code == 200:
            # Crawl sitemap
            soup = BeautifulSoup(response.content, "xml")

            list_item = soup.find_all('url')

            for item in list_item:
                job_url = item.find('loc').get_text() if item.find('loc') else None
                image = item.find('image:loc').get_text() if item.find('image:loc') else None
                changefreq = item.find('changefreq').get_text() if item.find('changefreq') else None
                lastmod = item.find('lastmod').get_text() if item.find('lastmod') else None
                priority = item.find('priority').get_text() if item.find('priority') else None
                
                list_url.append(
                    {
                        "job_url": job_url,
                        "image": image,
                        "changefreq": changefreq,
                        "lastmod": lastmod,
                        "priority": priority,
                        "created_date": today
                    }
                ) 
                
        return list_url    
    except requests.exceptions.RequestException as e:
        print( f"Error occurred: {str(e)}")        

"""
Export list of url in sitemap into csv file
Args: 
    list_url (list): list of site map url
Returns: careerviet_sitemap_{today}.csv
""" 
def export_sitemap_urls_to_csv(list_url):
    df = pd.DataFrame(list_url)
    df.to_csv(f'careerviet_sitemap_{today}.csv', index=False, header=True, encoding='utf-8')

"""
Process the pipeline to crawl and store data of sitemap url into mongodb
Args: 
    mongodb: connection to mongodb
Returns: 
""" 
def sitemap_process():
    
    mongodb = connect_mongodb()
    # Crawling sitemap
    sitemap_url = "https://careerviet.vn/sitemap/job_vi.xml" 
    list_url = crawl_sitemap(sitemap_url)
    
     # Delete current data
    delete_filter = {"created_date": today}
    mongodb.delete_many(delete_filter)
    
    # Load current data
    mongodb.insert_many(list_url)
    
    # Close the connection    
    mongodb.close()
    
def crawl_job_post_template1(soup, url):
    # Attribute
    job = {}
    job_id = job_title = company_url  = updated_date = industry =  \
    job_type = salary = experience = job_level = deadline = benefit = \
    job_description = job_requirement = more_information = None
    
    pattern = r'\.([A-Z0-9]+)\.html'
    match = re.search(pattern, url)
    if match:
        job_id = match.group(1)    
        
    # PART 1: TOP 
    if soup.find('div', class_='head-left'):                
        job_title = soup.find('div', class_='head-left').find('div', 'title').find('h2').text
        if soup.find('div', class_='head-left').find('a'):
            company_url = soup.find('div', class_='head-left').find('a').get('href')
            
    # PART 2: BODY
    body = soup.find('div', class_='body-template').find('div', class_='content')
    tr_tags = body.find_all('tr')
    for tr in tr_tags:
        if tr.find('em', class_='fa-id-badge'):
            industry = ' '.join(a.text.strip() for a in tr.find('td', class_='content').find_all('a'))
        if tr.find('em', class_='fa-usd'):
            salary = tr.find('td', class_='content').find('strong').text.strip()
        if tr.find('em', class_='mdi-briefcase-edit'):
            job_type = tr.find('td', class_='content').find('p').text.strip()
        if tr.find('em', class_='mdi-account'):
            job_level = tr.find('td', class_='content').find('p').text.strip()
        if tr.find('em', class_='fa-briefcase'):
            experience = re.sub(r'\s+', ' ', tr.find('td', class_='content').find('p').text.strip())
        if tr.find('em', class_='fa-calendar-times-o'):
            deadline = tr.find('td', class_='content').find('p').text.strip()
        if tr.find('em', class_='fa-calendar'):
            updated_date = tr.find('td', class_='content').find('p').text.strip()
        
    # PART 1: BOTTOM
    bottom = soup.find('div', class_='bottom-template').find('div', class_='full-content')
    div_tags = bottom.find_all('div', class_= 'detail-row')
    
    if len(div_tags) > 2:
        job_description = ';'.join(re.sub(r'\s+', ' ', li.get_text(strip=True)) for li in div_tags[1].find_all('p'))            
        job_requirement = ';'.join(re.sub(r'\s+', ' ', li.get_text(strip=True)) for li in div_tags[2].find_all('p'))           
        # Replace all sequences of whitespace characters with a single space
        more_information =  ';'.join(re.sub(r'\s+', ' ', li.get_text(strip=True)) for li in div_tags[3].find_all('li'))
    
    job = {
        "job_id":job_id,
        "job_url": url,
        "job_title": job_title,
        "company_url": company_url,
        "updated_date": updated_date,
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
        "created_date": today
    }   
    return job

def crawl_job_post_template2(soup, url):
    # Attribute
    job = {}
    job_id = job_title = company_url = location = updated_date = industry =  \
    job_type = salary = experience = job_level = deadline = benefit = \
    job_description = job_requirement = more_information = None
    
    pattern = r'\.([A-Z0-9]+)\.html'
    match = re.search(pattern, url)
    if match:
        job_id = match.group(1)            
    
    
    # Job title 
    if soup.find('div', class_='job-desc'):                
        job_title = soup.find('div', class_='job-desc').find('h1').text
    
    if soup.find('div', class_='job-desc').find('a') is not None:
        company_url = soup.find('div', class_='job-desc').find('a').get('href') 
    
    job_detail_content = soup.find('div', id='tab-1').find('section', class_='job-detail-content')
    
    # PART 1: OVERVIEW
    overview_div_tags = job_detail_content.find('div', class_='bg-blue').find_all('div', class_='col-lg-4 col-sm-6 item-blue') 
    if len(overview_div_tags) > 2:        
        # 1st dev    
        location = overview_div_tags[0].find('div', class_='map').find('a').text.strip()                
        # 2nd dev
        li_tags = overview_div_tags[1].find_all('li')
        for li in li_tags:
            # updated_date
            if li.find('em', class_='mdi-update'):
                if li.find('em', class_='mdi-update').find_parent('li').find('p'):
                    updated_date = li.find('em', class_='mdi-update').find_parent('li').find('p').text.strip()
            # industry
            if li.find('em', class_='mdi-briefcase'):
                if li.find('em', class_='mdi-briefcase').find_parent('li').find('p'):
                    industry = ' '.join(li.find('em', class_='mdi-briefcase').find_parent('li').find('p').text.strip().split())
            # job_type
            if li.find('em', class_='mdi-briefcase-edit'):
                if li.find('em', class_='mdi-briefcase-edit').find_parent('li').find('p'):
                    job_type = li.find('em', class_='mdi-briefcase-edit').find_parent('li').find('p').text.strip()  
                    
        # 3rd dev
        li_tags = overview_div_tags[2].find_all('li')
        for li in li_tags:
            if li.find('i', class_="fa-usd"):
                salary = li.find('i', class_="fa-usd").find_parent('li').find('p').text.strip()
            if li.find('i', class_="fa-briefcase"):
                experience = li.find('i', class_="fa-briefcase").find_parent('li').find('p').text.strip()
            if li.find('i', class_="mdi-account"):
                job_level = li.find('i', class_="mdi-account").find_parent('li').find('p').text.strip()
            if li.find('i', class_="mdi-calendar-check"):
                deadline = li.find('i', class_="mdi-calendar-check").find_parent('li').find('p').text.strip()
    
    # PART 2: DETAIL       
    
    detail_div_tags = job_detail_content.find_all('div', class_='detail-row')
    
    if len(detail_div_tags) > 3:                 
        benefit = ';'.join(re.sub(r'\s+', ' ', li.get_text(strip=True)) for li in detail_div_tags[0].find_all('li'))            
        job_description = ';'.join(re.sub(r'\s+', ' ', li.get_text(strip=True)) for li in detail_div_tags[1].find_all('p'))            
        job_requirement = ';'.join(re.sub(r'\s+', ' ', li.get_text(strip=True)) for li in detail_div_tags[2].find_all('p'))           
        # Replace all sequences of whitespace characters with a single space
        more_information =  ';'.join(re.sub(r'\s+', ' ', li.get_text(strip=True)) for li in detail_div_tags[3].find_all('li'))
                    
    job = {
        "job_id":job_id,
        "job_url": url,
        "job_title": job_title,
        "company_url": company_url,
        "location": location,
        "updated_date": updated_date,
        "industry": industry,
        "job_type": job_type,
        "salary": salary,
        "experience": re.sub(r'\s+',' ', experience) if experience is not None else None,
        "job_level": job_level,
        "deadline": deadline,
        "benefit": benefit,
        "job_description": job_description,
        "job_requirement": job_requirement,
        "more_information": more_information,
        "created_date": today
    }
    # print(job)
    return job

"""
Crawl a job
Args: 
    url (string): job url
Returns: 
""" 
def crawl_job_post_worker(url):
    time.sleep(1) 
    try:
        response = requests.get(    url = url, 
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
            job = {}  
            if soup.find('section', class_='search-result-list-detail template-2'):
                job = crawl_job_post_template2(soup, url)
            elif soup.find('section', class_='template01-banner'):
                job = crawl_job_post_template1(soup, url)
            
            mongodb = connect_mongodb()    
            mongodb.set_collection(conn['cv_job_post_detail'])
            mongodb.insert_one(job)
            # Close the connection    
            mongodb.close()            
            # time.sleep(1) 
    except requests.exceptions.RequestException as e:
        print( f"Error occurred: {str(e)}")
  
  
"""
Crawl all jobs in sitemap data and store into mongodb
Args: 
    mongodb
Returns: 
"""     
def job_url_generator():    
    mongodb = connect_mongodb()
    mongodb.set_collection(conn['cv_sitemap_job'])
    # Filter
    filter = {"created_date": today}
    # Projecttion: select only the "job_url" field
    projection = {"_id": False, "job_url": True}
    cursor = mongodb.select(filter, projection)
    
    # Extract job_url
    for document in cursor:
        # print(document["job_url"])
        yield document["job_url"]            

    
    # Close the connection    
    mongodb.close()
    
def current_jobs_process():
    with multiprocessing.Pool(2) as pool:
        # parallel the scapring process
        pool.map(crawl_job_post_worker, job_url_generator())
        
# if __name__ == "__main__":  
#     # Process sitemap
#     sitemap_process()     
    
#     # Craw current jobs process
#     start_time = time.time()
#     # current_jobs_process()
#     print('Execution time: ', time.time()-start_time)
    
    
    
    
    
    

