"""
This module manage all ElasticSearch actions that will
be usefull to synchronize data with Cassandra
"""
from elasticsearch.exceptions import TransportError

from settings import *
import elasticsearch
import logging
import uuid


def _conn():
    """
    Connect to ElasticSearch
    """
    es = elasticsearch.Elasticsearch(**ELASTICSEARCH_CONNECTION)
    return es


def _results():
    """
    Retrieve all data in the index's doc_types
    """
    res = _conn().search(index=DATABASE)['hits']['hits']
    return res


def _get_doc_types():
    """
    Retrive all index's doc_types names
    """
    for r in _results():
        print(r['_type'])


def insert(doc_type, body):
    """
    Insert data to an specific doc_type
    """
    try:
        _conn().create(index=DATABASE, doc_type=doc_type, body=body)
        logging.info("Document inserted")
    except Exception as ex:
        logging.error("Unexpected Error. Details: " + str(ex))


def update(doc_type, uuid, body):
    """
    Update row to an specific doc_type
    """
    pass


def create_doc_type(**kwargs):
    """
    Create doc_type (table) inside an index
    """
    _conn().index()


def show_all():
    """
    Retrieve doc_types cleaned data
    """
    results = []
    for r in _results():
        results.append(r['_source'])

    return results


body = {"id": str, "name": "Adriane Maria"}
# _conn().delete(index=DATABASE, doc_type="t1", id="nkiLdJQXSE-4YTbXu3Nw3g")
# insert(doc_type="t1", body=body)
print(show_all())
