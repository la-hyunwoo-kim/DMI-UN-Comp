import requests
import json
import xmltodict
import time
import logging
import pandas as pd

logger = logging.getLogger(__name__)

def get_raw_xml(URL, headers=None):
    start = time.time()
    res = requests.get(URL, headers=headers, timeout=30)
    res.encoding="utf-8"
    target_xml = res.text
    end = time.time()
    logger.info(f"Response status is {res.status_code}. Total time taken {end - start} seconds")
    return target_xml


def xml2dict(conlist_xml, profiletype_col):
    start = time.time()
    un_json_raw = xmltodict.parse(conlist_xml, dict_constructor=dict, encoding="utf-8")
    un_ind = un_json_raw['CONSOLIDATED_LIST']["INDIVIDUALS"]["INDIVIDUAL"]
    un_ent = un_json_raw['CONSOLIDATED_LIST']["ENTITIES"]["ENTITY"]
    
    source_update_time = un_json_raw['CONSOLIDATED_LIST']['@dateGenerated']

    for individual in un_ind:
        individual[profiletype_col] = "Individual"

    for entity in un_ent:
        entity[profiletype_col] = "Entity"
        
    un_json = un_ind + un_ent
    end = time.time()
    logger.info(f"Length of list = {len(un_json)}. Total time Taken {end - start} seconds")
    
    return un_json, source_update_time