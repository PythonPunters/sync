from settings import *
from cassandra.cluster import Cluster, NoHostAvailable
import logging


# starting the logger
logging.basicConfig(filename=LOG_DIR + 'cassandra_dao.log', level=logging.DEBUG, filemode='w')
logger = logging.getLogger("CassandraDAO")


class CassandraDAO():
    """
    This class manage all Cassandra actions that will
    be usefull to synchronize data with ElasticSearch
    """

    def __init__(self):
        # creating connection
        try:
            logger.info("Creating connection")
            self.cluster = Cluster(**CASSANDRA_CONNECTION)
            self.cs = self.cluster.connect(DATABASE)
            self.cs.execute("USE %s", DATABASE)
        except NoHostAvailable as ex:
            logger.exception("Invalid host name. More info: %s", ex)
        except Exception as ex:
            logger.exception("An error ocurred. More info: %s", ex)

    def insert(self):
        pass

    def delete(self):
        pass


cs = CassandraDAO()
