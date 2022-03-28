import os
import copy
from arango import ArangoClient
# import arango.exceptions as ohoh
import dataplug.utils as utils

GRAPH_MARKER = "g--"

DEFAULT_PORT = os.getenv("DATAPLUG_DEFAULT_PORT", 8529)
DEFAULT_HOST = os.getenv("DATAPLUG_DEFAULT_HOST", "localhost")
DEFAULT_DOMAIN = os.getenv("DATAPLUG_DEFAULT_DOMAIN", "dataplug")
DEFAULT_SYSTEM_DB = "_system"

class Client():

    def __init__(self, domain, collection, db_config={}):
        """ Initiate a arango client

            Arango databases are referred as domains
            Arango collections are referred as collections
        """
        # Local and object initializations
        self._db_config = None
        self._domain = None
        self._collection = None
        self._client = None
        self._system = None

        # Check client info to the Arango Database
        self.db_config = db_config
        self.check_credentials()

        # Get domain and collection
        self.connect()
        if self.is_connected():
            if isinstance(domain, str) and len(domain) > 0:
                self.domain = domain
            if isinstance(collection, str) and len(collection) > 0:
                self.collection = collection

    def connect(self):
        """ It connects to the database using the server config
            And it establish link with the _system database
            to be able to create other databases and collections
        """
        if self._system is not None:
            self._system = None
            self.domain = None

        # Database server connection
        self._client = ArangoClient(
            hosts= str(self._db_config['protocol'])
                + "://"
                + str(self._db_config['host'])
                + ":"
                + str(self._db_config['port']))

        # connects to default system database u"_system"
        self._system = self._client.db(username=self._db_config["username"],
                                       password=self._db_config["password"])

    def is_connected(self):
        """ Check if the connection with database is established """
        status = False
        if self._system is not None:
            try:
                # verify() function does not exist anymore
                # so here using an indirect way.
                status = self._system.has_database(DEFAULT_SYSTEM_DB)
            except Exception as eee:
                status = False
        return status

    @property
    def db_config(self):
        return self._db_config

    @db_config.setter
    def db_config(self, conf):
        self._db_config = copy.deepcopy(conf)

    @property
    def db(self):
        """ Returns systemm db to do deletes or other actions """
        return self._system

    @property
    def domain(self):
        """ Returns the arango domain handler
        """
        return self._domain

    @domain.setter
    def domain(self, domain_name):
        """ Sets the current domain object and returns it

            If the domain name has already been used then the domain is
            reloaded from the database else it is created if non-existent.

            You won't be able to create databases if you are not a "_system"
            database authorized user.
        """
        if domain_name is None:
            self._domain = None
            return self._domain

        utils.raise_wrong_db_string(domain_name)
        if self._domain is not None:
            if domain_name == self._domain.db_name:
                return self._domain

        if domain_name not in self._system.databases():
            self._system.create_database(domain_name)

        # getting the defined domain.
        self._domain = self._client.db(domain_name,
                                       username=self._db_config["username"],
                                       password=self._db_config["password"])

        return self._domain

    @property
    def collection(self):
        """ Returns the arango collection handler
        """
        return self._collection

    @collection.setter
    def collection(self, collection_name):
        """ A collection is generally defined by configuration
            but sometimes it is guessed by node names

            :param collection_name: new collection to get, create or check
        """
        if collection_name is None:
            self._collection = None
            return self._collection

        utils.raise_wrong_db_string(collection_name)

        if self._domain is None:
            raise AttributeError("Undefined domain for collection {}"
                                 .format(collection_name))

        if self._collection is not None:
            if collection_name == self._collection.name:
                return self._collection

        if collection_name in \
           map(lambda c: c['name'], self._domain.collections()):
            self._collection = self._domain.collection(collection_name)
        else:
            is_edge = False
            if "edge" in self._db_config:
                is_edge = self._db_config["edge"]
            self._collection = self._domain.create_collection(collection_name,
                                                              edge=is_edge)

        return self._collection

    def check_credentials(self):
        """Fill missing fields with default values
        """
        if "protocol" not in self._db_config or not isinstance(self._db_config["protocol"], str):
            self._db_config["protocol"] = "http"
        if "host" not in self._db_config:
            self._db_config["host"] = DEFAULT_HOST
        if "port" not in self._db_config:
            self._db_config["port"] = DEFAULT_PORT
        if "username" not in self._db_config:
            self._db_config["username"] = ""
        if "password" not in self._db_config:
            self._db_config["password"] = ""
        return self._db_config

    def set(self, domain_name, collection_name):
        """ Set a new collection and domain at the same time """
        self.domain = domain_name
        self.collection = collection_name
        return self._collection

    def delete_collection(self):
        """ Delete the current collection

            return as arango.database.Database().delete_collection()

        """
        if self._domain is None or self._collection is None:
            return False
        try:
            self._collection.unload()
            self._domain.delete_collection(self._collection.name,
                                           ignore_missing=True)
        except Exception as eee:
            return False
        return True

    def all(self, qparams={}, only_fields=["_id"]):
        """ Return a list of this object
            This transforms the cursor list into a python list

            :param qparams: query parameters like; limit: limit the number of
            objects returned if any other parameter is given the find method is
            called.
            :param only_fields: fields key to be selected in the returned
            objects. If empty then all fields are provided but the reserved
            ones starting with "_".
        """
        listing = []
        limit = None
        if "limit" in qparams:
            limit = qparams["limit"]


        if ("limit" in qparams and len(qparams) > 1) \
           or ("limit" not in qparams and len(qparams) > 0):
            # then we have query parameters to search with.
            listing = self.find(self.qparams_to_dict(qparams), limit=limit)
        else:
            # --- Just select all documents in this collection
            cursor = None
            cursor = self._collection.all(limit=limit)
            if cursor.count() > 0:
                listing = cursor.batch()

        # Filtering the listing
        if only_fields == []:
            for L in listing:
                for x in [field.startswith("_") for field in L]:
                    L.pop(x, None)
        else:
            for L in listing:
                for x in [field not in only_fields for field in L]:
                    L.pop(x, None)

        return listing

    def get(self, key=""):
        """ Return the data of the node/document with given key

            :param key: key part of the full id "collection_name/key"
                       because used on the collection object already.
        """
        info = {}

        if key == "":
            return info

        try:
            info = self.collection.get(str(key))
            if info is None:
                info = {}
        # except ohoh.DocumentRevisionError as eee:
        # except ohoh.DocumentGetError as eee:
        except Exception as eee:
            info = {}

        return info

    def delete(self, key):
        """ Delete the node/document with given key, from current collection

            :param key: key of the championship, else the current data is used
            :return: a boolean representing the deletion status
        """
        if self._domain is None or self._collection is None:
            return False

        rinfo = False

        if not isinstance(key, str) or key == "":
            return rinfo

        try:
            rinfo = self.collection.delete(document=key,
                                           ignore_missing=True,
                                           return_old=False)
            if rinfo is not False:
                rinfo = True
        except Exception as eee:
            print(eee)
            rinfo = False

        return rinfo

    def qparams_to_dict(self, qparams):
        return qparams

    def find(self, sdict, skip=None, limit=None):
        """ Simple search that returns the dict for the found object information
            or all occurences of objects with sdict fields descriptions

            :param sdict: search dictionnary
            :return: a list of dictionaries of a found node's data 
        """
        listing = []

        if self._domain is None or self._collection is None or sdict == {}:
            return listing 

        try:
            cur = self.collection.find(sdict, skip, limit)
            if cur.count() == 1:
                listing = [cur.next()]
            else:
                listing = [c for c in cur]
        except Exception as eee:
            listing = []

        return listing

    def query(self, aql_str, bind_vars={}):
        """ Execute an AQL query

            :param aql_str: String of AQL commands to be executed
                            with @bindvars if needed
            :param bind_vars: Key values dictionnary for AQL bind vars
        """
        result = []

        if not self.is_connected():
            return result

        try:
            cursor = self._domain.aql.execute(
                aql_str,
                bind_vars=bind_vars,
                count=True
            )
            result = [v for v in cursor]
            cursor.close()
        except Exception as eee:
            print(eee)
            result = []

        return result

    def traverse(self, from_full_key, edges_list, depth="", what="vertex", rwhat="", direction="OUTBOUND", ignore_keys=["_key", "_rev"]):
        """ Anonymous simplistic traversal function using anonymous graph, so
            with direct use of a list of edges

            :param from_full_key: ID of the starting node point
            :param edges_list: list of edge collections to traverse
            :return: the array of "what" was traversed.
        """

        # --- Failsafes
        D = len(edges_list)
        if D == 0:
            return []

        # By default catching only the last level of the traversal
        if depth == "":
            depth = str(D)+".."+str(D)

        # Defining return: Attention NO CHECKS are made here on good rwhats
        if rwhat == "":
            rwhat = what

        # Building request string
        req_str = "FOR "+what+" IN "+depth+" "+direction+" @starter "

        is_first_edge = True
        for edge in edges_list:
            if is_first_edge:
                is_first_edge = False
                req_str += edge
            else:
                req_str += ", "+edge

        req_str += " RETURN "+rwhat

        # "FOR vertex IN 2..2 OUTBOUND @starter u_o, o_s RETURN vertex",
        result = []
        try:
            cursor = self._domain.aql.execute(
                req_str,
                bind_vars={'starter': from_full_key},
                # batch_size=1,
                count=True
            )
            result = [v for v in cursor]
            # do not forget to close the cursor, else it'll be painful
            cursor.close()
        except:
            result = []

        for r in result:
            for k in ignore_keys:
                if k in r:
                    del r[k]

        return result

