"""
Sync settings
"""

# Cassandra Connection
CASSANDRA_CONNECTION = {
    'contact_points': ['127.0.0.1'],
    'port': 9042,
}

# ElasticSearch Connection
ELASTICSEARCH_CONNECTION = {
    'host': '127.0.0.1',
    'port': 9200,
}

# Keyspace (Cassandra) or Index (ElasticSearch) to connect
DATABASE = "sync"
