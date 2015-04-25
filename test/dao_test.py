import unittest
import time


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
        self.generated_id = time.time()
        self.new_data = {}
        self.same_data = {}
        self.updated_data = {}


    def test_insert(self):
        pass

    def test_update(self):
        pass

    def test_delete(self):
        pass

    def doCleanups(self):
        pass
