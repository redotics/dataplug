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

        self.graph = None
        self._db_config = client_config
        self.client = dataplug.Client(domain=domain,
                                       collection="",
                                       db_config=client_config)

    def create_graph(self, graph_name):
        """ Create an Arango graph with given arguments
        """
        utils.raise_empty_string(graph_name)

        try:
            if not self.client.domain.has_graph(graph_name):
                self.graph = self.client.domain.create_graph(graph_name)
        except Exception as eee:
            self.graph = None
            print(eee)
            print("OOps could not create graph")
            pass

    def set_graph(self, from_cols, to_cols, gname="default"):
        """ Set graph with an edge definition

            :param from_cols: array of strings of collection names as source
            collections

            :param to_cols: array of strings of collection names as destination
            collections

        """
        self.graph = None
        # TODO: have a utils function for this to be uniform
        graph_name = utils.GRAPH_MARKER + gname
        #from_cols + "_" + to_cols

        if self.client.domain is None:
            return self.graph

        if self.client.domain.has_graph(graph_name):
            self.graph = self.client.domain.graph(graph_name)
            return self.graph
        else:
            self.create_graph(graph_name)

        # set edge definition
        from_cols.extend(to_cols)
        edge_def = self.graph.create_edge_definition(
            edge_collection=utils.edge_naming(from_cols)[0],
            from_vertex_collections=from_cols,
            to_vertex_collections=to_cols
        )

        return self.graph

    def outbounds_from_node(self, from_full_key, output_json=False, list_name="list"):
        """ Get outbounds nodes from a full node 'collection/key'

            :param from_full_key: full id of the node from which we can the
            traverse outbound
        """
        if from_full_key == "" or self.graph is None:
            return {}

        traversal_results = {}
        try:
            traversal_results = self.graph.traverse(
                start_vertex=from_full_key,
                direction="outbound",
                strategy="bfs",
                edge_uniqueness="global",
                vertex_uniqueness="global",
            )
        except:
            print("Error:get_outbounds_from: could not traverse graph")

        output_dict = self.traversal_filter(traversal_results,
                                            ignore_full_key=from_full_key,
                                            list_name=list_name)

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
                    output[list_name].append(vert)

        return output
