import dataplug

################################################
class Edge(dataplug.node.Node):

    def __init__(self,
                 from_id,
                 to_id,
                 data={},
                 key="",
                 client_src=None,
                 mandatory_features=[]):
        """ Create and manage an edge between two nodes

            An edge is a node with supplementary fields "_from" and "_to". An
            edge collection name is defined by the origin and destination
            collection's names.

            If client_src is provided anyway then it will be the config cloning
            source, overtaking any node/edge possible configurations.
            If one of the node is given as a full key text then the client of
            the other node will be used. If both are given as full key text
            then the client_src must be provided with a defined domain to clone
            it to the necessary connection.

            :param from_id: origin node or full key of the origin node
            :param to_id: destinatio node or full key of the destination node
            :param data: edge-attached information as a dict
            :param client_src: source of configuration to be cloned
        """
        self.from_collection = ""
        self.to_collection = ""
        self.from_domain = ""
        self.to_domain = ""
        self.from_id = ""
        self.to_id = ""
        db_config = {}

        self.from_collection, \
            self.from_domain, \
            self.from_id, \
            db_config = self.extract_info(from_id, client_src)

        print("DEBUG- -----------------")
        print(db_config)

        self.to_collection, \
            self.to_domain, \
            self.to_id, \
            db_config_to = self.extract_info(to_id, client_src)

        print(db_config_to)

        db_config.update(db_config_to)

        if "domain" not in db_config:
            if len(self.from_domain) > 0:
                db_config["domain"] = self.from_domain
            elif len(self.to_domain) > 0:
                db_config["domain"] = self.to_domain

        if "domain" not in db_config:
            print("I miss domain information to connect.")

        db_config["collection"] =\
            self.from_collection +\
            dataplug.client.EDGE_MARKER +\
            self.to_collection

        if "_from" not in mandatory_features:
            mandatory_features.append("_from")
        if "_to" not in mandatory_features:
            mandatory_features.append("_to")

        super(Edge, self).__init__(
           data=data,
           key=key,
           client=dataplug.client.Client(db_config),
           mandatory_features=mandatory_features)

        self.client.set_graph([self.from_collection], [self.to_collection])
        self._data.update({"_from": self.from_id, "_to": self.to_id})

    def extract_info(self, this_id, client_src):
        """ Extract collection name and client configuration from a nod
        """
        db_config = {}
        node_id = ""
        col_name = ""
        dom_name = ""

        if isinstance(this_id, str):
            if len(this_id) > 0:
                col_name = this_id.split("/")[0]
                node_id = this_id
        elif isinstance(this_id, dataplug.node.Node):
            node_id = this_id.full_key()
            if len(node_id) > 2:
                if this_id.client.collection is not None:
                    col_name = this_id.client.collection.name
                if client_src is None:
                    db_config = this_id.client.db_config

        if client_src is not None:
            db_config = client_src.db_config
            if "domain" in db_config:
                dom_name = db_config["domain"]
            if "collection" in db_config:
                col_name = db_config["collection"]

        return col_name, dom_name, node_id, db_config

    def upsave(self):
        """ Update/Save function adapted to edges
        """
        super(Edge, self).upsave(keep_private_fields=["_from", "_to"])

    @staticmethod
    def edge_naming(col_list, split_collections=True):
        """ This function normalize the naming of edges collections

            If split_collections is True an edge collection name will be
            generated between each listed collection in order.
            So if col_list = [A, B, C]
            result will be [A__B, B__C]

            :param col_list: ordered list of collection names
            :return: an array of edge collection names
        """
        result = []
        name = ""
        for v in col_list:
            if name == "":
                name = v
            else:
                name = name + dataplug.client.EDGE_MARKER + v
                if split_collections:
                    result.append(name)
                    name = v

        if len(result) == 0:
            result.append(name)

        return result

