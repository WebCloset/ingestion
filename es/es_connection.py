import os

from elasticsearch import Elasticsearch


def get_es_connection():
    es_url = os.getenv("ELASTICSEARCH_URL", "http://localhost:9200/")
    es_username = os.getenv("ELASTICSEARCH_USERNAME", None)
    es_password = os.getenv("ELASTICSEARCH_PASSWORD", None)

    if es_password and es_username:
        return Elasticsearch(es_url, basic_auth=(es_username, es_password))

    # Initialize Elasticsearch
    return Elasticsearch(
        es_url
    )

os.environ["ELASTICSEARCH_URL"] = "https://elasticsearch-production-3ce1.up.railway.app"
es = get_es_connection()
print(es.info())