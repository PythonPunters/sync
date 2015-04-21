"""
Write Something here
"""

from settings import *
from cassandra.cluster import Cluster


def get_cassandra_conn():
    """
    Write something here
    """
    cluster = Cluster(**CASSANDRA_CONNECTION)
    # session = cluster.connect("sync")
    return cluster
