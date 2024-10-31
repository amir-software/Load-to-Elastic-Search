from elasticsearch import Elasticsearch, helpers
from datetime import datetime
import csv
from collections import deque


MEDIA_ROOT = "media_root/"
ELASTIC_HOST = "127.0.0.1"
ELASTIC_USER = "admin"
ELASTIC_PASSWORD = "admin"
ELASTIC_IMPORT_CHUNK_SIZE = "5000"
ELASTIC_IMPORT_THREAD_COUNT= "8"
ELASTIC_IMPORT_QUEUE_SIZE = "20"

def datetime_to_string(date_time=datetime.now()):
    """ Convert the python datetime object to our fav string"""
    date_time = str(date_time).replace(':', '-')
    date_time = date_time.replace(' ', '-')
    date_time = date_time.split('.')[0]

    return date_time


def extract_from_file(file_name, file_format):
    "Convert the CSV file to CSV Reader instance"
    if file_format == "CSV":
        with open(f'{MEDIA_ROOT}/{file_name}') as f:
            data_object = csv.DictReader(f)
    elif file_format == "JSON": # Implement Later
        pass
    
    return data_object


def insert_into_elastic(index_name, data_object, mapping):
    client = Elasticsearch(ELASTIC_HOST, basic_auth=(ELASTIC_USER, ELASTIC_PASSWORD), verify_certs=False)
    try:
        ### this is non parallel way ### 
        # pb = helpers.bulk(client, data_object, index=index_name) 

        ## Create an index and transfer data from CSV object in it
        client.indices.create(index=index_name, body=mapping)
        pb = helpers.parallel_bulk(client, data_object, index=index_name, chunk_size=ELASTIC_IMPORT_CHUNK_SIZE, thread_count=ELASTIC_IMPORT_THREAD_COUNT, queue_size=ELASTIC_IMPORT_QUEUE_SIZE)
        deque(pb, maxlen=5)
    except Exception as e:
        print(e, "INSERT_INTO_ELASTIC")
        return "INSERT_INTO_ELASTIC_ERROR"


def main(file_name, file_format):
    """ Stage the data into elastic search """
    start_date = datetime.now()
    index_name = file_name.lower() + "-" + datetime_to_string().replace('/', '-')

    data_object = extract_from_file(file_name=file_name, file_format=file_format)
    response = insert_into_elastic(index_name=index_name, data_object=data_object)
    if response == "INSERT_INTO_ELASTIC_ERROR":
        return response, 0

    extraction_taken_time = (datetime.now() - start_date).total_seconds()

    print(f"END OF THE ELASTIC LOADING index name : {index_name} {extraction_taken_time}")
    return index_name, extraction_taken_time
