import dataplug
import dataplug.utils

################################################
class Edge(dataplug.node.Node):
    FROM = "_from"
    TO = "_to"

    def __init__(self,
                 domain,
                 from_id,
                 to_id,
                 data={},
                 key="",
                 client_config={},
                 mandatory_features=[],
                 do_graph=False,
                 no_duplicate=True):
        """ Create and manage an edge between two nodes

            An edge is a node with supplementary fields "_from" and "_to". An
            edge collection name is defined by the origin and destination
            collection's names.

            If client_config is provided anyway then it will be the config cloning
            source, overtaking any node/edge possible configurations.
            If one of the node is given as a full key text then the client of
            the other node will be used. If both are given as full key text
            then the client_config must be provided with a defined domain to clone
            it to the necessary connection.

            :param domain: domain database to connect to.
            :param from_id: origin node or full key of the origin node
            :param to_id: destinatio node or full key of the destination node
            :param data: edge-attached information as a dict
            :param client_config: source of configuration to be cloned, a client
                               or a dict
            :param mandatory_features: cf. dataplug.node.Node
            :param do_graph: create an arango graph for this edge
            :param no_duplicate: if True, it will look for a similar edge node
            and prevent from creating a duplicate when updating this one
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
            db_config = dataplug.utils.extract_info(from_id, client_config)

        self.to_collection, \
            self.to_domain, \
            self.to_id, \
            db_config_to = dataplug.utils.extract_info(to_id, client_config)

        db_config.update(db_config_to)

        # --- Get domain and collection names
        #if len(self.from_domain) > 0:
        #    domain = self.from_domain
        #elif len(self.to_domain) > 0:
        #    domain = self.to_domain

        collection = \
            dataplug.utils.edge_naming([self.from_collection, self.to_collection])[0]

        # Setting up mandatory fields for an edge document
        if self.FROM not in mandatory_features:
            mandatory_features.append(self.FROM)
        if self.TO not in mandatory_features:
            mandatory_features.append(self.TO)

        # --- Initiatilize the edge-like document
        db_config["edge"] = True
        super(Edge, self).__init__(
           domain=domain,
           collection=collection,
           key=key,
           data=data,
           client_config=db_config,
           mandatory_features=mandatory_features)

        if do_graph is True:
            self.client.set_graph([self.from_collection], [self.to_collection])
        edge_data = {self.FROM: self.from_id, self.TO: self.to_id}
        self._data.update(edge_data)

        if no_duplicate is True:
            similar_node = self.client.find(edge_data)
            if "_key" in similar_node:
                self.key(similar_node["_key"])

    def upsave(self):
        """ Update/Save function adapted to edges
        """
        super(Edge, self).upsave(keep_private_fields=[self.FROM, self.TO])

    def delete(self):
        super(Edge, self).delete()

