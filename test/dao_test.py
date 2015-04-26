from dao.elasticsearch_dao import ElasticSearchDAO
import unittest
import settings
import uuid


class TestCassandraDAO(unittest.TestCase):
    """
    CassandraDAO Test Case
    """

    pass


class TestElasticSearchDAO(unittest.TestCase):
    """
    ElasticSearchDAO Test Case
    """

    def setUp(self):
        self.es = ElasticSearchDAO()
        self.doc_type = 'movie'

        self.data = {
            "title": "Big Hero 6",
            "director": ["Don Hall", "Chris Williams"],
            "year": 2015,
            "genres": [
                "Comédia",
                "Animação"
            ]
        }

        self.updated_data = {
            "title": "Big Hero 6",
            "director": ["Don Hall", "Chris Williams"],
            "year": 2014,
            "genres": [
                "Comédia",
                "Animação"
            ]
        }

    def test_insert(self):
        self.assertEquals(self.es.insert(doc_type=self.doc_type, body=self.data), "Document inserted.")

    def test_insert_existing_values(self):
        self.assertEquals(self.es.insert(doc_type=self.doc_type, body=self.data), "Document not inserted.")

    def test_update(self):
        self.assertEquals(self.es.insert(doc_type=self.doc_type, body=self.updated_data), "Document updated.")

    def test_delete(self):
        self.assertEquals(self.es.delete(doc_type=self.doc_type, id=self.__get_last_id()), "Document deleted.")

    def doCleanups(self):
        print(self.__get_last_id())
        self.es.delete(doc_type=self.doc_type, id=self.__get_last_id())

    def __get_last_id(self):
        id_list = []
        for r in self.es.get_all_data():
            print(r)
            id_list.append(str(r['_id']))

        if id_list:
            return id_list[len(id_list) - 1]
