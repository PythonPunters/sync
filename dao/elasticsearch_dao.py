from settings import *
import elasticsearch
import logging
import uuid
import time

# starting the logger
logger = logging.getLogger("ElasticSearchDAO")


class ElasticSearchDAO():
    """
    This class manage all ElasticSearch actions that will
    be usefull to synchronize data with Cassandra
    """

    def __init__(self):
        # creating connection
        self.es = elasticsearch.Elasticsearch(**ELASTICSEARCH_CONNECTION)

    def __create_doc_type(self, name):
        """
        Create a doc_type inside an index
        :param name: Name of the doc_typel
        :return the status of document creation
        """
        if name not in self.__get_doc_types():
            logger.info("Creating a new doc_type")
            return self.es.create(DATABASE, doc_type=name, body={}, id=time.time())
        else:
            logger.error("doc_type already exists!")

    def __get_all_data(self, doc_type=None):
        """
        Retrieve doc_types cleaned data
        :param doc_type: Filter by doc_type
        :return a bit cleaned list of existing documents
        """
        result = []
        for r in self.es.search(index=DATABASE, doc_type=doc_type)['hits']['hits']:
            logger.info("Found document %s", r)
            result.append(r)
        return result

    def __cleaned_data(self, doc_type=None):
        """
        Retrieve document _source
        :param doc_type: Filter by doc_type
        :return a list of documents without any extra information
        """
        result = []
        for r in self.__get_all_data(doc_type=doc_type):
            logger.info("Found document _source %s", r)
            result.append(r['_source'])
        return result

    def __get_doc_types(self, doc_type=None):
        """
        Retrive all index's doc_types' names
        :param doc_type: Filter by doc_type
        :return list of all doc_types
        """
        doc_types = []
        for r in self.__get_all_data(doc_type=doc_type):
            logger.info("Found doc_type %s", r)
            doc_types.append(r)

        return doc_types

    def insert(self, doc_type, body, id=None):
        """
        Insert or update doc_type data
        :param doc_type: Type of elasticsearch document
        :param body: Document content
        :param id: Document id. If it's none, insert data to an document, else update it
        :return the status of document insert/update
        """
        if not id:
            # create a new document
            logger.info("Creating a new document")
            return self.es.index(index=DATABASE, doc_type=doc_type, body=body, id=time.time())
        else:
            # update the document
            logger.info("Updating the document with id: %f", id)
            return self.es.index(index=DATABASE, doc_type=doc_type, body=body, id=id)
