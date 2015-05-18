from settings import *
import elasticsearch
import logging
import uuid
import time

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

    def __generate_id(self):
        """
        Generate an uuid4 if it does not exists
        :return: A generated id
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

    def get_doc_types(self, doc_type=None):
        """
        Retrive all index's doc_types' names
        :param doc_type: Filter by doc_type
        :return: List of all doc_types
        """
        doc_types = []
        try:
            for r in self.get_all_data(doc_type=doc_type):
                logger.info("Found doc_type %s", r)
                doc_types.append(r['_type'])
        except Exception as ex:
            logger.exception("An error ocurred. More info: %s", ex)

        return doc_types

    def create_doc_type(self, name):
        """
        Create a doc_type inside an index
        :param name: Name of the doc_typel
        :return: The status of document creation
        """
        try:
            if name not in self.get_doc_types():
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
        :return: A bit cleaned list of existing documents
        """
        result = []
        try:
            for r in self.es.search(index=DATABASE, doc_type=doc_type)['hits']['hits']:
                logging.info("Found document %s", r)
                result.append(r)
        except Exception as ex:
            logger.exception("An error ocurred. More info: %s", ex)
        return result

    def cleaned_data(self, doc_type=None):
        """
        Retrieve document _source
        :param doc_type: Filter by doc_type
        :return: A list of documents without any extra information
        """
        result = []
        try:
            for r in self.get_all_data(doc_type=doc_type):
                logger.info("Found document _source %s", r)
                r['_source'].pop('saved_at')
                result.append(r['_source'])
        except Exception as ex:
            logger.exception("An error ocurred. More info: %s", ex)
        return result

    def save(self, doc_type, body, id=None):
        """
        Insert or update doc_type data
        :param doc_type: Type of elasticsearch document
        :param body: Document content
        :param id: Document id. If it's none, insert data to an document, else update it
        :return: The status of document insert/update
        """
        msg = ""
        try:
            id_list = []
            for r in self.get_all_data():
                logger.info("Id %s found.", r['_id'])
                id_list.append(str(r['_id']))
            if body not in self.cleaned_data():
                if id is None:
                    # create a new document
                    logger.info("Creating a new document")
                    body['saved_at'] = time.time()
                    generated_id = self.__generate_id()
                    self.es.index(index=DATABASE, doc_type=doc_type, body=body, id=generated_id)
                    msg = "Document inserted."
                else:
                    # update the document
                    logger.info("Updating the document with id: %s", str(id))
                    body['saved_at'] = time.time()
                    if id in id_list:
                        self.es.index(index=DATABASE, doc_type=doc_type, body=body, id=id)
                        msg = "Document updated."
                    else:
                        logger.error("Id %s não encontrado.", str(id))
                        msg = "Id %s não encontrado.", str(id)
            else:
                logger.error("Document already exists.")
                msg = "Document not inserted."
        except Exception as ex:
            logger.exception("An error ocurred. More info: %s", ex)
            msg = "An error occurred."

        return msg

    def delete(self, doc_type, id):
        """
        Delete a document by given id
        :param doc_type: Type of elasticsearch document
        :param id: Document id
        :return: The status of document delete
        """
        msg = ""
        try:
            logger.info("Deleting document with id %s", str(id))
            self.es.delete(index=DATABASE, doc_type=doc_type, id=id)
            msg = "Document deleted."
        except Exception as ex:
            logger.exception("An error ocurred. More info: %s", ex)
            msg = "Document not deleted."

        return msg
