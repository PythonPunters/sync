"""
This module manage all Cassandra actions that will
be usefull to synchronize data with ElasticSearch
"""

from settings import *
from cassandra.cluster import Cluster


def get_cassandra_conn():
    """
    Write something here
    """
    cluster = Cluster(**CASSANDRA_CONNECTION)
    session = cluster.connect(DATABASE)
    return session
