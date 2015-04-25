from dao.elasticsearch_dao import ElasticSearchDAO
import unittest
import settings

GENERATED_ID = 42


class TestCassandraDAO(unittest.TestCase):
    """
    Write something here
    """

    pass


class TestElasticSearchDAO(unittest.TestCase):
    """
    ElasticSearchDAO Test Case
    """

    def setUp(self):
        self.es = ElasticSearchDAO()
        self.doc_type = 'movie'
        self.new_data = {
            "title": "Big Hero 6",
            "director": ["Don Hall", "Chris Williams"],
            "year": 2015,
            "genres": [
                "Comédia",
                "Animação"
            ]
        }
        self.same_data = {
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
        self.es.insert(doc_type=self.doc_type, body=self.new_data, id=GENERATED_ID)
        pass

    def test_insert_existing_values(self):
        self.assertEquals(self.es.insert(doc_type=self.doc_type, body=self.same_data), "Not inserted.")

    def test_update(self):
        self.es.insert(doc_type=self.doc_type, body=self.updated_data)
        pass

    def test_delete(self):
        self.assertEquals(self.es.delete(doc_type=self.doc_type, id=12341234), "Not deleted.")

    def doCleanups(self):
        self.es.delete(doc_type=self.doc_type, id=GENERATED_ID)
