from settings import *
import elasticsearch
import logging
import uuid

# starting the logger
logging.basicConfig(filename=LOG_DIR + 'elasticsearch_dao.log', level=logging.DEBUG, filemode='w')
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

    def __cleaned_data(self, doc_type=None):
        """
        Retrieve document _source
        :param doc_type: Filter by doc_type
        :return a list of documents without any extra information
        """
        result = []
        try:
            for r in self.get_all_data(doc_type=doc_type):
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
            for r in self.get_all_data(doc_type=doc_type):
                logger.info("Found doc_type %s", r)
                doc_types.append(r)
        except Exception as ex:
            logger.exception("An error ocurred. More info: %s", ex)

        return doc_types

    def __generate_id(self):
        """
        Generate an uuid4 if it does not exists
        :return a generated id
        """
        id_list = []
        for r in self.get_all_data():
            logger.info("Id %s found.", r['_id'])
            id_list.append(str(r['_id']))
        generated_id = str(uuid.uuid4())
        if generated_id in id_list:
            logger.info("Creating a new uuid")
            self.__generate_id()
        return generated_id

    def create_doc_type(self, name):
        """
        Create a doc_type inside an index
        :param name: Name of the doc_typel
        :return the status of document creation
        """
        try:
            if name not in self.__get_doc_types():
                logger.info("Creating a new doc_type")
                self.es.create(DATABASE, doc_type=name, body={}, id=str(uuid.uuid4()))
                return "Document created."
            else:
                logger.error("doc_type already exists!")
        except Exception as ex:
            logger.exception("An error ocurred. More info: %s", ex)

    def get_all_data(self, doc_type=None):
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

    def insert(self, doc_type, body, id=None):
        """
        Insert or update doc_type data
        :param doc_type: Type of elasticsearch document
        :param body: Document content
        :param id: Document id. If it's none, insert data to an document, else update it
        :return the status of document insert/update
        """
        try:
            id_list = []
            for r in self.get_all_data():
                logger.info("Id %s found.", r['_id'])
                id_list.append(str(r['_id']))
            if not id:
                # create a new document
                if body not in self.__cleaned_data():
                    logger.info("Creating a new document")
                    generated_id = self.__generate_id()
                    self.es.index(index=DATABASE, doc_type=doc_type, body=body, id=generated_id)
                    return "Document inserted."
                else:
                    logger.error("Document already exists.")
                    return "Document not inserted."
            else:
                # update the document
                logger.info("Updating the document with id: %s", str(id))
                if id in id_list:
                    self.es.index(index=DATABASE, doc_type=doc_type, body=body, id=id)
                    return "Document updated."
                else:
                    logger.error("Id %s não encontrado.", str(id))
        except Exception as ex:
            logger.exception("An error ocurred. More info: %s", ex)

    def delete(self, doc_type, id):
        """
        Delete a document by given id
        :param doc_type: Type of elasticsearch document
        :param id: Document id
        :return the status of document delete
        """
        try:
            logger.info("Deleting document with id %s", str(id))
            self.es.delete(index=DATABASE, doc_type=doc_type, id=id)
            return "Document deleted."
        except Exception as ex:
            logger.exception("An error ocurred. More info: %s", ex)
            return "Document not deleted."
