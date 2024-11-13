import re
from collections import defaultdict

def convert_list_of_dicts_to_dict(list_of_dicts):
    """
    Converts a list of dictionaries into a single dictionary where keys are 
    unique keys from the list and values are lists of all values associated with those keys.

    Args:
        list_of_dicts (list): A list of dictionaries. Each dictionary contains key-value pairs.
    
    Returns:
        dict: A dictionary where each key maps to a list of values from the dictionaries in the input list.

    Example:
        >>> list_of_dicts = [{"a": 1, "b": 2}, {"a": 3, "c": 4}, {"b": 5, "c": 6}]
        >>> convert_list_of_dicts_to_dict(list_of_dicts)
        {'a': [1, 3], 'b': [2, 5], 'c': [4, 6]}
    Old code:
        result = {}
        for d in list_of_dicts:
            for key, value in d.items():
                if key in result:
                    result[key].append(value)
                else:
                    result[key] = [value]
        return result
    """
    result = defaultdict(list)
    for d in list_of_dicts:
        for key, value in d.items():
            result[key].append(value)
    
    return dict(result)


def extract_text(parent, tag, namespaces=None):
    """
    Helper function to extract text from an XML element.

    Args:
        parent: Parent XML element.
        tag (str): Tag name to search within the parent element.
        namespaces (dict, optional): Namespaces to use for searching, if the XML uses namespaces.

    Returns:
        str or None: The stripped text of the tag or None if the tag is not found.
    """
    element = parent.find(tag, namespaces) if namespaces else parent.find(tag)
    return element.text.strip() if element is not None and element.text else None

def extract_object_id(object_url, pattern):
    """
    Helper function to extract emplobjectoyer ID using a regex pattern.

    Args:
        object (str): The URL from which to extract the object ID.
        pattern (str): The regex pattern used to extract the object ID.

    Returns:
        str or None: The extracted object ID, or None if it cannot be found.
    """
    match = re.search(pattern, object_url)
    return match.group(1) if match else None
