import re

text = "There are 25 apples, 30 bananas, and 100 oranges."

# Extract all numbers as strings
numbers = re.findall(r'\d+', text)[0]
print(numbers)  # Output: ['25', '30', '100']
