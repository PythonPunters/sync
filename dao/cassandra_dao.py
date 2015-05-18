from settings import *
from cassandra.cluster import Cluster, NoHostAvailable
from cassandra.metadata import KeyspaceMetadata
import logging
import uuid
import time


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
            self.cs.set_keyspace(DATABASE)
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
        logger.info("Generating a new uuid.")
        generated_id = str(uuid.uuid4())

        if generated_id in self.__get_all_ids(table=table):
            logger.info("Uuid already exists. Generating it again.")
            self.__generate_id(table=table)

        return str(generated_id)

    def __get_all_ids(self, table):
        id_list = []
        for r in self.get_all_data(table=table):
            logger.info("Id %s found.", r['id'])
            id_list.append(str(r['id']))

        return id_list

    def get_all_tables(self):
        """
        Get all existing tables' names
        :return: List of existing tables
        """
        result = list(self.cluster.metadata.keyspaces[DATABASE].tables.keys())

        return result

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

        return result

    def cleaned_data(self, table):
        """
        :param table: Keyspace's table
        :return: List of documents without ids
        """
        result = []
        for data in self.get_all_data(table=table):
            data.pop('id')
            data.pop('saved_at')
            result.append(data)

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
            if body not in self.cleaned_data(table=table):
                if id is None:
                    # insert a new data
                    logger.info("Inserting a new data.")
                    body['id'] = self.__generate_id(table=table)
                    body['saved_at'] = "%s" % time.time()
                    query = "INSERT INTO " + table + " " \
                            + str(tuple(body.keys())).replace("'", "").replace('"', '') + \
                            " VALUES " + str(tuple(body.values()))

                    logger.info("Query: %s" % query)
                    self.cs.execute(query)
                else:
                    logger.info("Updating the document.")
                    body['saved_at'] = "%s" % time.time()
                    query = "UPDATE %s" % table + " SET "
                    # add quotation mark to id
                    id = "'%s'" % id
                    for key, value in body.items():
                        # add quotation marks if value is string
                        if isinstance(value, str):
                            value = "'%s'" % value
                        query += "%s = %s, " % (key, value)

                    # removing last comma
                    query = query[:-2]
                    query += " WHERE id = %s" % id

                    logger.info("Query: %s" % query)
                    self.cs.execute(query)
            else:
                logger.error("Document already exists.")
        except Exception as ex:
            logger.exception("An error ocurred. More info: %s" % ex)
        finally:
            self.cs.shutdown()
            self.cluster.shutdown()

    def delete(self, table, id):
        try:
            if id in self.__get_all_ids(table=table):
                # add quotation mark to id
                id = "'%s'" % id
                logger.info("Deleting the document.")
                query = "DELETE FROM %s WHERE id = %s" % (table, id)
                self.cs.execute(query)
            else:
                logger.error("Id don't exists.")
        except Exception as ex:
            logger.exception("An error ocurred. More info: %s" % ex)
        finally:
            self.cs.shutdown()
            self.cluster.shutdown()
