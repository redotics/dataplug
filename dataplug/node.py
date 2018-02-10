import dataplug
import copy


class Node():

    def __init__(self,
                 domain="",
                 collection="",
                 key="",
                 data={},
                 mandatory_features=[],
                 client_config={},
                 update=False):
        """
            :param key: full or only-key part part in id "collection_name/key"
                        If the only-key is givent the collection information
                        should appear in the given client.
                        Take precedence on an eventual "_key" field.
            :param data: dictionnary of data
            :param client: dependency injection for database client
            :param mandatory_features: a way to define a model of nodes, where
                                       some mandatory fields are required.
            :param update: if True, it gets the corresponding object
                           from the database and updates its local data with
                           the input data from `data`. Update to the database
                           must be done by calling upsave()
        """

        self.mandatory_features = mandatory_features
        self._collection_name = ""

        if domain == "":
            domain = dataplug.client.DEFAULT_DOMAIN
        self.client = dataplug.client.Client(domain, collection, client_config)

        # Data initialization can impact client, so done after
        self._data = {}
        self.data = data

        # locally extract key information and sets client collection
        # only if the domain is defined
        current_key = self.key(key)

        if current_key != "" and update:
            # Checking in data from provided key and checking mandatory fields
            self.data = self.client.get(current_key)
            # Updating data with eventual new fields from constructor inputs
            self._data.update(data)

    @property
    def collection_name(self):
        if self.client.collection is not None:
            return self.client.collection.name
        return self._collection_name

    @collection_name.setter
    def collection_name(self, col_name=""):
        self._collection_name = col_name

    @property
    def data(self):
        return self._data

    @data.setter
    def data(self, data={}):
        """ This setter also checks if the mandatory fields are also present

            :param data: New data dictionnary to input
        """
        if data != {}:
            self._data = copy.deepcopy(data)

        if self.mandatory_features != []:
            for f in self.mandatory_features:
                if f not in self._data:
                    self._data[f] = ""

    def key(self, new_key=""):
        """ Locally set or get node's key

            An empty key is never set so that any new node will be given a new
            key by the database.

            :new_key: nothing, key part or full id of a node
        """
        if not isinstance(new_key, str):
            new_key = str(new_key)

        # Agile local SET
        if len(new_key) > 0:
            self._collection_name, new_key = dataplug.utils.split_node_id(new_key)
            if len(self._collection_name) > 0 and self.client.domain is not None:
                    self.client.collection = self._collection_name

            self._data["_key"] = new_key
            return self._data["_key"]

        # Local GET
        if "_key" in self._data:
            return self._data["_key"]
        return ""

    def full_key(self):
        """ Returns the node id, reconstructed with available information
        """
        fk = ""
        if self.client is None:
            return fk
        if self.client.collection is not None:
            fk = self.client.collection.name+"/"+self.key()
        else:
            fk = self._collection_name+"/"+self.key()
        return fk

    def filter_data(self, keep_fields=[]):
        """ Takes off all the parameters initiated by the DB starting with "_"
            returning only the used fields, or user's initiated fields.
            Offline and local filtering.

            :param keep_fields: list of fields to keep, be they private or not.
        """
        pure_data = {}
        for k in self.data:
            if len(k) > 0:
                if k[0:1] != "_" or k in keep_fields:
                    pure_data[k] = self.data[k]
        return pure_data

    def delete(self):
        """ delete a document by full_key """
        try:
            #self.client.collection.delete(document=self.full_key(), return_old=False)
            self.client.delete(self.key())
        except Exception as eee:
            print("Node: problem during delete: {}".format(eee))

    def upsave(self, keep_private_fields=[], update=False):
        """ Save or update an category document

            :param by_similarity: if True, this function will try to
                find a similar object in the database according to the
                mandatory_features, only used if the key is not provided. It
                might be possible you want to create another object with
                the same data in this case set update to False.
            :param keep_private_fields: it will keep only the selected private
                fields. Useful to keep reserved database fields starting with
                "_".
        """
        if self.client.collection is None:
            return False 

        if "_key" not in self._data and update:
            other_key_fields = {}
            for k in self.mandatory_features:
                other_key_fields[k] = self._data[k]

            try:
                cur = self.client.collection.find(other_key_fields)
                if cur.count() == 1:
                    self.key(cur.next()["_key"])
                else:
                    if cur.count() > 1:
                        # TODO: log ("Warning: Using 1st; Found too many entries in "+self.client.collection.name+" with same fields "+str(other_key_fields))
                        self.key(cur.next()["_key"])
                        # TODO: select with more fields not in the mandatory list
                    # else:
                        # TODO: log ("Nothing found, not using any key so the database can define it automatically")
            # except ohoh.DocumentGetError as eee:
            except Exception as eee:
                # TODO: log ("ERROR when finding node in upsave() for "+self.client.collection.name)
                # if something bad happen here, it may well happen later. So let's stop here.
                print(eee)
                return False

        try:
            key = self.key()

            if key == "":
                # INSERT new object with no predefined key
                newbody = self.client.collection.insert(
                    self.filter_data(keep_fields=keep_private_fields),
                    return_new=True)
                if "_key" in newbody:
                    self.key(newbody["_key"])
            else:
                if self.client.collection.has(key):
                    # UPDATE existing object because this is the same key
                    self.client.collection.update_match(
                        {"_key": key},
                        self.filter_data(keep_fields=keep_private_fields))
                    # From here I could get all the updated data and set it to self.data
                else:
                    # INSERT new object with predefined key
                    newbody = self.client.collection.insert(self.data, return_new=True)
                    # synchronise keys in case database had to change it
                    if "_key" in newbody:
                        self.key(newbody["_key"])
        except Exception as eee:
            print(eee)
            print("ERROR: when saving in upsave() for "+self.client.collection.name)
            return False

        return True

    def add_field(self, key, value):
        self._data[key] = value
        return self
