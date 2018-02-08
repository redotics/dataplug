import dataplug
import copy

EDGE_MARKER = "__"

def split_node_id(node_id):
    """ split and extract collection name """
    col_name = ""
    key_only = node_id
    if isinstance(node_id, str):
        splits = node_id.split("/")
        if len(splits) == 2:
            col_name = splits[0]
            key_only = splits[1]
    return col_name, key_only


def probe_node_object(this_id):
    """ Get collection name and node full name from node object """
    col_name = ""
    full_id = ""
    if isinstance(this_id, dataplug.node.Node):
        full_id = this_id.full_key()
        col_name, key_only = split_node_id(full_id)

    return col_name, full_id


def extract_info(this_id, client_src):
    """ Extract collection name and client configuration from a node string
    name or node object

    """
    config = {}
    dom_name = ""

    col_name, key_only = split_node_id(this_id)
    if col_name == "":
        col_name, node_id = probe_node_object(this_id)
    else:
        node_id = this_id

    if client_src is None and isinstance(this_id, dataplug.node.Node):
        config = copy.deepcopy(this_id.client.db_config)

    if client_src is not None:

        if isinstance(client_src, dataplug.client.Client):
            config = copy.deepcopy(client_src.db_config)
        elif isinstance(client_src, dict):
            config = copy.deepcopy(client_src)

        if "domain" in config:
            dom_name = config["domain"]
        if "collection" in config:
            col_name = config["collection"]

    return col_name, dom_name, node_id, config


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
            name = name + EDGE_MARKER + v
            if split_collections:
                result.append(name)
                name = v

    if len(result) == 0:
        result.append(name)

    return result
