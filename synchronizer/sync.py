from dao.cassandra_dao import CassandraDAO
from dao.elasticsearch_dao import ElasticSearchDAO
import settings
import logging

# starting the logger
logging.basicConfig(filename=settings.LOG_DIR + 'synchronizer.log', level=logging.DEBUG, filemode='w')
logger = logging.getLogger("Synchronizer")


class Synchronizer():
    """
    Write something cool here
    """

    def __init__(self):
        self.elasticsearch = ElasticSearchDAO()
        self.cassandra = CassandraDAO()

    def is_updated(self, provider):
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
            return False

    def cassandra_to_elasticsearch(self):
        """
        Write something cool here
        """
        pass

    def elasticsearch_to_cassandra(self):
        """
        Write something cool here
        """
        pass
