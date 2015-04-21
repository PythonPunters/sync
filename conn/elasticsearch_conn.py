"""
Write something here
"""

import elasticsearch


def get_elasticsearch_conn():
    """
    Write something here
    """
    es = elasticsearch.Elasticsearch(**ELASTICSEARCH_CONNECTION)
    return es
