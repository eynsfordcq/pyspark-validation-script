import time 
import logging
import xml.etree.ElementTree as ET

def timer(func):
    def wrapper(*args, **kwargs):
        start = time.time()
        res = func(*args, **kwargs)
        end = time.time()
        duration = end - start
        logging.info(f"{func.__name__}(): execution duration: {duration:.4f}s")
        return res 
    return wrapper

def parse_config(config_path):
    """
    Parses an XML configuration file into a nested dictionary.

    Args:
        config_path (str): The file path to the XML configuration file.

    Raises:
        FileNotFoundError: If the configuration file does not exist.
        ET.ParseError: If there is an error parsing the XML file.

    Returns:
        Dict[str, Any]: A dictionary representing the parsed configuration.
    """
    def _parse_element(element):
        if len(element) == 0:
            return element.text
        
        child_dict = {}
        for child in element:
            if child.tag in child_dict:
                # If a tag is encountered again, store values in a list
                if not isinstance(child_dict[child.tag], list):
                    child_dict[child.tag] = [child_dict[child.tag]]
                child_dict[child.tag].append(_parse_element(child))
            else:
                child_dict[child.tag] = _parse_element(child)
        return child_dict
    
    dict_config = {}
    root = ET.parse(config_path).getroot()

    for child in root:
        if child.tag in dict_config:
            if not isinstance(dict_config[child.tag], list):
                dict_config[child.tag] = [dict_config[child.tag]]
            dict_config[child.tag].append(_parse_element(child))
        else:
            dict_config[child.tag] = _parse_element(child)

    return dict_config

