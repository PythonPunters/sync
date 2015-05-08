from settings import *
from cassandra.cluster import Cluster, NoHostAvailable
import logging
import uuid


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
            logger.info("Creating connection.")
            self.cluster = Cluster(**CASSANDRA_CONNECTION)
            self.cs = self.cluster.connect(DATABASE)
            self.cs.execute("USE %s" % DATABASE)
        except NoHostAvailable as ex:
            logger.exception("Invalid host name. More info: %s" % ex)
        except Exception as ex:
            logger.exception("An error ocurred. More info: %s" % ex)

    def __generate_id(self, table):
        """
        Generate an uuid4 if it does not exists
        :param table: Keyspace's table
        :return: A generated id
        """
        id_list = []
        for r in self.get_all_data(table=table):
            logger.info("Id %s found.", r['id'])
            id_list.append(str(r['id']))

        logger.info("Generating a new uuid.")
        generated_id = str(uuid.uuid4())

        if generated_id in id_list:
            logger.info("Uuid already exists. Generating it again.")
            self.__generate_id(table=table)

        return str(generated_id)

    def get_all_data(self, table):
        """
        :param table: Keyspace's table
        :return: List with all table's data
        """
        query = 'SELECT * FROM %s' % table
        rows = self.cs.execute(query)
        result = []
        for row in rows:
            result.append(dict(row._asdict()))

        print(result)
        return result

    def save(self, table, body, id=None):
        """
        Insert or update doc_type data
        :param table: Keyspace's table
        :param body: Document content
        :param id: Row id. If it's none, insert data, else update it
        :return: The status of row insert/update
        """
        try:
            if not id:
                # insert a new data
                logger.info("Inserting a new data.")
                body['id'] = self.__generate_id(table=table)
                query = 'INSERT INTO ' + table + ' ' \
                        + str(tuple(body.keys())).replace("'", "").replace('"', '') + \
                        ' VALUES ' + str(tuple(body.values()))

                logger.info("Query: %s" % query)
                self.cs.execute(query)
            else:
                print('I am here')
        except Exception as ex:
            logger.exception("An error ocurred. More info: %s" % ex)

    def delete(self):
        pass


body = {
    "director": [
        "Don Hall",
        "Chris Williams"
    ],
    "year": 2014,
    "genres": [
        "Comédia",
        "Animação"
    ],
    "title": "Big Hero 6"
}
cs = CassandraDAO()
# cs.save(table='movie', body=body)
cs.get_all_data(table='movie')