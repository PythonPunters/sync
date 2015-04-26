"""
Sync settings
"""

import os

BASE_DIR = os.path.dirname(os.path.realpath(__file__))
LOG_DIR = BASE_DIR + '/logs/'

# Cassandra Connection
CASSANDRA_CONNECTION = {
    'contact_points': ['127.0.0.1'],
    'port': 9043,
}

# ElasticSearch Connection
ELASTICSEARCH_CONNECTION = {
    'host': '127.0.0.1',
    'port': 9200,
}

# Keyspace (Cassandra) or Index (ElasticSearch) to connect
DATABASE = "movies"
