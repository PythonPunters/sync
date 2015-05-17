"""
Write something here
"""
from dao.elasticsearch_dao import ElasticSearchDAO
from dao.cassandra_dao import CassandraDAO

body = {
    "title": "The Godfather",
    "directors": ["Francis Ford Coppola"],
    "year": 1972,
    "genres": ["Crime", "Drama"]
}

cs = CassandraDAO()
cs.save(table='movies', body=body)

#es = ElasticSearchDAO()
# es.save(doc_type='movies', body=body)

#print(es.cleaned_data(doc_type='movies'))