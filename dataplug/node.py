# import arango.exceptions as ohoh


class Node():

    def __init__(self,
                 data={},
                 key="",
                 client=None,
                 mandatory_features=[],
                 update_if_exist=False):
        """
            :param data: dictionnary of data
            :param key: only the key part in id "collection_name/key"
                       because used on the collection object already.
                       Take precedence on an eventual "_key" field.
            :param client: dependency injection for database client
            :param mandatory_features: a way to define a model of nodes,
                        where some mandatory fields are required.
            :param update_if_exist: if True, it gets the corresponding
                        object from the database and updates its data
                        with the input data from `data`
        """

        self.client = client
        self.mandatory_features = mandatory_features
        self._data = {}
        self.data = data

        # First we see if we have a key and get this object
        current_key = self.key(key)
        if current_key != "" and update_if_exist and self.client is not None:
            # Checking in data from provided key and checking mandatory fields
            self.data = self.client.get(current_key)
            # Updating data with eventual new fields from constructor inputs
            self._data.update(data)

    @property
    def data(self):
        return self._data

    @data.setter
    def data(self, data={}):
        """ This setter also checks if the mandatory fields are also present

            :param data: New data dictionnary to input
        """
        if data != {}:
            self._data = data

        if self.mandatory_features != []:
            for f in self.mandatory_features:
                if f not in self._data:
                    self._data[f] = ""

    def key(self, new_key=""):
        """ Locally set or get node's key

            An empty key is never set so that any new node will be given a new
            key by the database.
        """
        # Local SET
        if len(new_key) > 0:
            self._data["_key"] = str(new_key)
            return self._data["_key"]
        # Local GET
        if "_key" in self._data:
            return str(self._data["_key"])
        return ""

    def full_key(self):
        fk = ""
        if self.client.collection is not None:
            fk = self.client.collection.name+"/"+self.key()
        return fk

    def filter_non_auto_data(self, keep_fields=[]):
        """ Takes off all the parameters initiated by the DB starting with "_"
            returining only the used fields, or user's initiated fields.
        """
        pure_data = {}
        for k in self.data:
            if len(k) > 0:
                if k[0:1] != "_" or k in keep_fields:
                    pure_data[k] = self.data[k]
        return pure_data

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
            return {}

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
                    self.filter_non_auto_data(keep_fields=keep_private_fields),
                    return_new=True)
                if "_key" in newbody:
                    self.key(newbody["_key"])
            else:
                if self.client.collection.has(key):
                    # UPDATE existing object because this is the same key
                    self.client.collection.update_match(
                        {"_key":key},
                        self.filter_non_auto_data(keep_fields=keep_private_fields))
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
