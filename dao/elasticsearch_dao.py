from settings import *
import elasticsearch
import logging
import time

# starting the logger
logging.basicConfig(filename=LOG_DIR + 'elasticsearch_dao.log', level=logging.DEBUG)
logger = logging.getLogger("ElasticSearchDAO")


class ElasticSearchDAO():
    """
    This class manage all ElasticSearch actions that will
    be usefull to synchronize data with Cassandra
    """

    def __init__(self):
        # creating connection
        logger.info("Creating connection")
        self.es = elasticsearch.Elasticsearch(**ELASTICSEARCH_CONNECTION)

    def __get_all_data(self, doc_type=None):
        """
        Retrieve doc_types cleaned data
        :param doc_type: Filter by doc_type
        :return a bit cleaned list of existing documents
        """
        result = []
        try:
            for r in self.es.search(index=DATABASE, doc_type=doc_type)['hits']['hits']:
                logging.info("Found document %s", r)
                result.append(r)
        except Exception as ex:
            logger.exception("An error ocurred. More info: %s", ex)
        return result

    def __cleaned_data(self, doc_type=None):
        """
        Retrieve document _source
        :param doc_type: Filter by doc_type
        :return a list of documents without any extra information
        """
        result = []
        try:
            for r in self.__get_all_data(doc_type=doc_type):
                logger.info("Found document _source %s", r)
                result.append(r['_source'])
        except Exception as ex:
            logger.exception("An error ocurred. More info: %s", ex)
        return result

    def __get_doc_types(self, doc_type=None):
        """
        Retrive all index's doc_types' names
        :param doc_type: Filter by doc_type
        :return list of all doc_types
        """
        doc_types = []
        try:
            for r in self.__get_all_data(doc_type=doc_type):
                logger.info("Found doc_type %s", r)
                doc_types.append(r)
        except Exception as ex:
            logger.exception("An error ocurred. More info: %s", ex)

        return doc_types

    def create_doc_type(self, name):
        """
        Create a doc_type inside an index
        :param name: Name of the doc_typel
        :return the status of document creation
        """
        try:
            if name not in self.__get_doc_types():
                logger.info("Creating a new doc_type")
                return self.es.create(DATABASE, doc_type=name, body={}, id=time.time())
            else:
                logger.error("doc_type already exists!")
        except Exception as ex:
            logger.exception("An error ocurred. More info: %s", ex)

    def insert(self, doc_type, body, id=None):
        """
        Insert or update doc_type data
        :param doc_type: Type of elasticsearch document
        :param body: Document content
        :param id: Document id. If it's none, insert data to an document, else update it
        :return the status of document insert/update
        """
        try:
            if not id:
                # create a new document
                if body not in self.__cleaned_data():
                    logger.info("Creating a new document")
                    return self.es.index(index=DATABASE, doc_type=doc_type, body=body, id=time.time())
            else:
                # update the document
                logger.info("Updating the document with id: %f", id)
                return self.es.index(index=DATABASE, doc_type=doc_type, body=body, id=id)
        except Exception as ex:
            logger.exception("An error ocurred. More info: %s", ex)
