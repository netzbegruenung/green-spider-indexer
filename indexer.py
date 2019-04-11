from datetime import datetime
from os import getenv

from google.cloud import datastore
from elasticsearch import Elasticsearch

credentials_path = getenv('GCLOUD_DATASTORE_CREDENTIALS_PATH')
datastore_client = datastore.Client.from_service_account_json(credentials_path)

spider_results_kind = 'spider-results'
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


def main():
    es = Elasticsearch([{'host': 'elasticsearch', 'port': 9200}])
    es.cluster.health(wait_for_status='yellow', request_timeout=1)

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
            "url": {"type": "string", "analyzer": "my_autocomplete_analyzer"},
            "meta": {
                "dynamic": "false",
                "properties": {
                    "type": {"type": "keyword"},
                    "level": {"type": "keyword"},
                    "city": {"type": "string", "analyzer": "my_autocomplete_analyzer"},
                    "district": {"type": "string", "analyzer": "my_autocomplete_analyzer"},
                    "state": {"type": "string", "analyzer": "my_autocomplete_analyzer"},
                }
            },
            "checks": {"dynamic": "false", "properties": {}},
            "rating": {"dynamic": "false", "properties": {}},
            "score": {"type": "float"},
            "created": {"type": "date"},
        }
    }

    #es.indices.delete(index=es_index_name)

    if not es.indices.exists(index=es_index_name):
        es.indices.create(index=es_index_name, ignore=400)
        es.indices.close(index=es_index_name)
        es.indices.put_settings(index=es_index_name, body=settings)
        es.indices.put_mapping(index=es_index_name, doc_type='result', body=mappings)
        es.indices.open(index=es_index_name)

    # TODO: get newest dataset creation date in index (if any)

    # Index database content
    count = 0
    for doc in get_spider_results(datastore_client):
        es.index(index=es_index_name, doc_type='result', id=doc['url'], body=doc)
        count += 1

    print('Done indexing %s documents' % count)


if __name__ == "__main__":
    main()
