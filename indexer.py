from datetime import datetime
import logging
from os import getenv
import sys

from dateutil.parser import parse
from google.cloud import datastore
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import NotFoundError


# credentials_path is the path to the Google Cloud credentials JSON file
# used for authentication.
credentials_path = getenv('GCLOUD_DATASTORE_CREDENTIALS_PATH')

# spider_results_kind is the name of the database/entity storing spider results
# in the Google Cloud datastore.
spider_results_kind = 'spider-results'

datastore_client = datastore.Client.from_service_account_json(credentials_path)

es_index_name = spider_results_kind


def convert_datastore_datetime(field):
    """
    return datetime in different ways, depending on whether the lib returns
    a str, int, or datetime.datetime
    """
    dt = ''
    if type(field) == datetime:
        dt = field
    elif type(field) == int:
        dt = datetime.utcfromtimestamp(field / 1000000)
    elif type(field) == str:
        dt = datetime.utcfromtimestamp(int(field) / 1000000)
    return dt


def get_spider_results(client):
    query = client.query(kind=spider_results_kind,
                         order=['-created'])

    for entity in query.fetch(eventual=True):
        created = convert_datastore_datetime(entity.get('created'))
        
        yield {
            'url': entity.key.name,
            'created': created.isoformat(),
            'meta': entity.get('meta'),
            'score': entity.get('score'),
            'checks': entity.get('checks'),
            'rating': entity.get('rating'),
        }

def make_indexname(name_prefix):
    """
    creates a timestamped index name
    """
    return name_prefix + "-" + datetime.utcnow().strftime("%Y%m%d-%H%M%S")

def main():
    # Set up logging
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    root.addHandler(ch)

    print("Connecting to elasticsearch:9200...")
    es = Elasticsearch([{'host': 'elasticsearch', 'port': 9200}])
    es.cluster.health(wait_for_status='yellow', request_timeout=20)

    settings = {
        "index.mapping.total_fields.limit": 2000,
        "analysis": {
            "tokenizer": {
                "my_autocomplete_tokenizer": {
                    "type": "edge_ngram",
                    "min_gram": 2,
                    "max_gram": 10,
                    "token_chars": ["letter"]
                }
            },
            "analyzer": {
                "my_autocomplete_analyzer": {
                    "tokenizer": "my_autocomplete_tokenizer",
                }
            }
        }
    }

    mappings = {
        "properties": {
            "url": {"type": "text", "analyzer": "my_autocomplete_analyzer"},
            "meta": {
                "dynamic": "false",
                "properties": {
                    "type": {"type": "keyword"},
                    "level": {"type": "keyword"},
                    "city": {"type": "text", "analyzer": "my_autocomplete_analyzer"},
                    "district": {"type": "text", "analyzer": "my_autocomplete_analyzer"},
                    "state": {"type": "text", "analyzer": "my_autocomplete_analyzer"},
                }
            },
            "checks": {"dynamic": "false", "properties": {}},
            "rating": {"dynamic": "false", "properties": {}},
            "score": {"type": "float"},
            "created": {"type": "date"},
        }
    }

    # Sometimes useful in development
    #es.indices.delete(index=es_index_name)

    tempindex = make_indexname(es_index_name)

    # Create new index
    es.indices.create(index=tempindex, ignore=400)
    es.indices.close(index=tempindex)
    es.indices.put_settings(index=tempindex, body=settings)
    es.indices.put_mapping(index=tempindex, doc_type='result', body=mappings)
    es.indices.open(index=tempindex)

    # Index database content
    logging.info('Reading result documents from %s DB' % spider_results_kind)
    count = 0
    for doc in get_spider_results(datastore_client):
        es.index(index=tempindex, doc_type='result', id=doc['url'], body=doc)
        count += 1

    logging.info('Indexed %s documents' % count)

    # Set our index alias to the new index,
    # remove old index if existed, re-create alias.
    if es.indices.exists_alias(name=es_index_name):
        old_index = es.indices.get_alias(name=es_index_name)
        
        # here we assume there is only one index behind this alias
        old_indices = list(old_index.keys())

        if len(old_indices) > 0:
            logging.info("Old index on alias is: %s" % old_indices[0])

            try:
                es.indices.delete_alias(index=old_indices[0], name=es_index_name)
            except NotFoundError:
                logging.error("Could not delete index alias for %s" % old_indices[0])
                pass

            try:
                es.indices.delete(index=old_indices[0])
            except:
                logging.error("Could not delete index %s" % old_indices[0])
                pass

    # Delete legacy index with same name as alias
    if es.indices.exists(index=es_index_name):
        logging.info("Deleting legacy index with name %s" % es_index_name)
        es.indices.delete(index=es_index_name)

    logging.info("Setting alias '%s' to index '%s" % (es_index_name, tempindex))
    es.indices.put_alias(index=tempindex, name=es_index_name)


if __name__ == "__main__":
    main()
