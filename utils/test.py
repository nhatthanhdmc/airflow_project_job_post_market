import re

# Sample URLs
url1 = "https://vieclam24h.vn/danh-sach-tin-tuyen-dung-cong-ty-dich-thuat-tdn-ntd30729p73.html"
url2 = "https://vieclam24h.vn/danh-sach-tin-tuyen-dung-cong-ty-tnhh-vilean-ntd202273976p122.html"

# Regular expression pattern with a capturing group to exclude '.html'
pattern = r'(ntd\d+p\d+)\.html'
match1.group(1)
match1.group(0)
# Extract using re.search and retrieve only the captured group
match1 = re.search(pattern, url1)
match2 = re.search(pattern, url2)

# print(match1)
# Print the matched patterns
print( if match1 else "No match")  # Output: ntd30729p73
# print(match1.group(1) if match2 else "No match")  # Output: ntd202273976p122
