"""
Write something here
"""
from dao.elasticsearch_dao import ElasticSearchDAO
from dao.cassandra_dao import CassandraDAO

body_movie = {
    "title": "The Godfather",
    "directors": ["Francis Ford Coppola"],
    "year": 1972,
    "genres": ["Crime", "Drama"]
}

body_serie = {
    "title": "Once Upon a Time",
    "directors": [
        "Adam Horowitz",
        "Edward Kitsis"
    ],
    "year": 2011,
    "genres": ["Crime", "Drama"],
    "seasons": 3
}

cs = CassandraDAO()
# cs.save(table='series', body=body_serie)
print(cs.get_all_tables())
#cs.get_all_data(table='movies')
# es = ElasticSearchDAO()
# es.save(doc_type='series', body=body_serie)
# print(es.get_doc_types())

# print(es.cleaned_data(doc_type='movies'))
