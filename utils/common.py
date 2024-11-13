def convert_list_of_dicts_to_dict(list_of_dicts):
    result = {}
    for d in list_of_dicts:
        for key, value in d.items():
            if key in result:
                result[key].append(value)
            else:
                result[key] = [value]
    return result

def extract_text_xml_tag_sitemap(parent, tag, namespaces=None):
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