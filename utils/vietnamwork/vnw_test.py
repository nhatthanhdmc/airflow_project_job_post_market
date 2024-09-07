# import requests
# import xml.etree.ElementTree as ET

# # Fetch sitemap
# sitemap_url = 'https://www.vietnamworks.com/sitemap/companies.xml'
# response = requests.get(sitemap_url)

# # Parse XML content
# tree = ET.ElementTree(ET.fromstring(response.content))
# root = tree.getroot()

# # Find all <loc> elements and extract URLs
# for elem in root.findall(".//{http://www.sitemaps.org/schemas/sitemap/0.9}loc"):
#     company_url = elem.text
#     print(company_url)

# import re

# company_url = 'https://www.vietnamworks.com/companies/12345'
# match = re.search(r'/companies/(\d+)', company_url)
# if match:
#     company_id = match.group(1)
#     print(f"Company ID: {company_id}")



import hashlib
from urllib.parse import urlparse

# Step 1: Extract the string from the URL
url = 'https://www.vietnamworks.com/nha-tuyen-dung/cong-ty-co-phan-ipos-vn-c181299'
parsed_url = urlparse(url)
# Extract the last part of the path
company_string = parsed_url.path.split('/')[-1]

# Step 2: Hash the extracted string using SHA256 (you can use MD5 if preferred)
hash_object = hashlib.sha256(company_string.encode())
hashed_id = hash_object.hexdigest()

# Print the original string and the hashed ID
print(f"Original String: {company_string}")
print(f"Hashed ID: {hashed_id}")


import hashlib
from urllib.parse import urlparse

# Step 1: Extract the string from the URL
url = 'https://www.vietnamworks.com/nha-tuyen-dung/cong-ty-co-phan-ipos-vn-c181299'
parsed_url = urlparse(url)
company_string = parsed_url.path.split('/')[-1]

# Step 2: Hash using MD5 (32 characters long)
hash_object = hashlib.md5(company_string.encode())
hashed_id = hash_object.hexdigest()

# Print the original string and the hashed ID
print(f"Original String: {company_string}")
print(f"MD5 Hashed ID: {hashed_id}")


import hashlib
from urllib.parse import urlparse

# Step 1: Extract the string from the URL
url = 'https://www.vietnamworks.com/nha-tuyen-dung/cong-ty-co-phan-ipos-vn-c181299'
parsed_url = urlparse(url)
company_string = parsed_url.path.split('/')[-1]

# Step 2: Hash using MD5 (32 characters long)
hash_object = hashlib.md5(company_string.encode())
hashed_id = hash_object.hexdigest()

# Print the original string and the hashed ID
print(f"Original String: {company_string}")
print(f"MD5 Hashed ID: {hashed_id}")


import hashlib
from urllib.parse import urlparse

# Step 1: Extract the string from the URL
url = 'https://www.vietnamworks.com/nha-tuyen-dung/cong-ty-co-phan-ipos-vn-c181299'
parsed_url = urlparse(url)
company_string = parsed_url.path.split('/')[-1]

# Step 2: Hash using SHA256 and truncate to 16 characters
hash_object = hashlib.sha256(company_string.encode())
hashed_id = hash_object.hexdigest()[:16]  # First 16 characters of the hash

# Print the original string and the truncated hashed ID
print(f"Original String: {company_string}")
print(f"Truncated SHA256 Hashed ID: {hashed_id}")
