from dao.cassandra_dao import CassandraDAO
from dao.elasticsearch_dao import ElasticSearchDAO
import settings
import logging

# starting the logger
logging.basicConfig(filename=settings.LOG_DIR + 'synchronizer.log', level=logging.DEBUG, filemode='w')
logger = logging.getLogger("Synchronizer")


class Synchronizer():
    """
    Sync data between cassandra end elasticsearch
    """

    def __init__(self):
        self.elasticsearch = ElasticSearchDAO()
        self.cassandra = CassandraDAO()

    def is_updated(self, provider, table):
        """
        Check if provider is updated
        :param provider: "Database" to be checked
        :return: If provider is updated (True) or outdated (False)
        """

        if provider.lower() == 'elasticsearch':
            return False
        elif provider.lower() == 'cassandra':
            return False
        else:
            logger.error("Provider doesnt exists.")
            raise ValueError('Invalid provider.')

    def cassandra_to_elasticsearch(self):
        """
        Write something cool here
        """
        for table in self.cassandra.get_all_tables():
            # if size of table content is not equal between cassandra end elasticsearch return False
            if len(self.cassandra.get_all_data(table=table)) != len(self.elasticsearch.get_all_data(doc_type=table)):
                return False

    def elasticsearch_to_cassandra(self):
        """
        Write something cool here
        """
        for doc_type in self.elasticsearch.get_doc_types():
            if len(self.elasticsearch.get_all_data(doc_type=doc_type)) != len(
                    self.cassandra.get_all_data(table=doc_type)):
                return False
