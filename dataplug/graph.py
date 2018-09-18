import dataplug
import dataplug.utils as utils

################################################
class Graph():

    def __init__(self,
                 domain,
                 client_config={},
                 ):
        """ Graph operations

            :param domain: acting domain. for now graph operations can only
            happen on one domain. A multi-domain feature could be nice for
            problems that need the semantic for domain-based discrimination.
            domain source and destination should be encoded somewhere in
            documents or somewhere.

        """

        self._graph = None
        self._db_config = client_config
        self._client = dataplug.Client(domain=domain,
                                       collection="",
                                       db_config=client_config)

    def create_graph(self, graph_name):
        """ Create an Arango graph with given arguments
        """
        utils.raise_empty_string(graph_name)

        try:
            if graph_name in map(lambda c: c['name'], self._domain.graphs()):
                self._graph = self._client.db.graph(graph_name)
            else:
                self._graph = self._client.db.create_graph(graph_name)
        except Exception as eee:
            pass

    def set_graph(self, from_cols, to_cols):
        """ Set graph with an edge definition

            :param from_cols: array of strings of collection names as source
            collections

            :param to_cols: array of strings of collection names as destination
            collections

        """
        # TODO

        if "collection" in self._db_config:
            self.graph = GRAPH_MARKER+self._db_config["collection"]
        else:
            # TODO: setup a name with collections' prefixes extraction
            self.graph = GRAPH_MARKER+"noname"

        try:
            self._collection = self.graph.create_edge_definition(
                name=self.collection_name,
                from_collections=from_cols,
                to_collections=to_cols
            )
        except Exception as eee:
            pass

    def outbounds_from_node(self, from_full_key, output_json=False):
        """ Get outbounds nodes from a full node 'collection/key'

            :param from_full_key: full id of the node from which we can the
            traverse outbound
        """
        if from_full_key == "" or self._graph is None:
            return {}

        traversal_results = {}
        try:
            traversal_results = self._graph.traverse(
                start_vertex=from_full_key,
                direction="outbound",
                strategy="bfs",
                edge_uniqueness="global",
                vertex_uniqueness="global",
            )
        except:
            print("Error:get_outbounds_from: could not traverse graph")

        output_dict = self.traversal_filter(traversal_results, ignore_full_key=from_full_key)

        if output_json==False:
            return output_dict

        return json(output_dict)

    def hierarchical_outbounds(self, from_collections, to_collections, output_json=False):
        """ Get the outbounds from the from_collections to the to_collections
            and output that as a dictionary or JSON text.
        """
        pass

    def traversal_filter(self,
                         traversal_dict,
                         ignore_full_key=[],
                         vertices_field="vertices",
                         list_name="list"):
        """ Filter traversal output in a more simple customized dict
        """
        output = {}
        output[list_name] = []
        if vertices_field not in traversal_dict:
            print("ERROR traversal results does not contain field: "
                  + vertices_field)
            print(traversal_dict)
            return output

        for vert in traversal_dict[vertices_field]:
            if "_id" in vert:
                if vert["_id"] not in ignore_full_key:
                    output[list_name].append(self.clean_dict(vert))

        return output
