from settings import *
from cassandra.cluster import Cluster
import elasticsearch

def get_cassandra_conn():
    cluster = Cluster(**CASSANDRA_CONNECTION)
    session = cluster.connect("sync")
    return session

def get_elasticsearch_conn():
    es = elasticsearch.Elasticsearch(**ELASTICSEARCH_CONNECTION)
    return es
