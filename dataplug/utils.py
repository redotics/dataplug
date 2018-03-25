import dataplug
import copy

EDGE_MARKER = "__"

def split_node_id(node_id):
    """ split and extract collection name """
    col_name = ""
    key_only = ""
    if isinstance(node_id, str):
        key_only = node_id
        splits = node_id.split("/")
        if len(splits) == 2:
            col_name = splits[0]
            key_only = splits[1]
    return col_name, key_only


def probe_node_object(this_id):
    """ Get collection name and node full name from node object """
    col_name = ""
    full_id = ""
    dom_name = ""
    if isinstance(this_id, dataplug.node.Node):
        full_id = this_id.full_key()
        col_name, key_only = split_node_id(full_id)
        if this_id.client.domain is not None:
            dom_name = this_id.client.domain.name

    return col_name, full_id, dom_name


def extract_info(this_id, client_src):
    """ Extract collection name and client configuration from a node string
    name or node object

    """
    col_name = ""
    dom_name = ""
    node_id = ""
    config = {}

    if isinstance(this_id, dataplug.node.Node):
        col_name, node_id, dom_name = probe_node_object(this_id)
    else:
        col_name, key_only = split_node_id(this_id)
        if col_name != "":
            node_id = this_id

    if isinstance(this_id, dataplug.node.Node):
        config = copy.deepcopy(this_id.client.db_config)

    if client_src is not None:
        if isinstance(client_src, dataplug.client.Client):
            config.update(client_src.db_config)
        elif isinstance(client_src, dict):
            config.update(client_src)

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


def raise_non_string(param):
    """ Will raise TypeError if the given param is not a string """
    if not isinstance(param, str):
        raise TypeError("Given parameter is not a string.")


def raise_empty_string(param):
    """ Will raise TypeError if the given param is not a string """
    raise_non_string(param)
    if len(param) == 0:
        raise ValueError("Given parameter is an empty string.")


def raise_wrong_db_string(param):
    raise_empty_string(param)
    if param.startswith("_"):
        raise ValueError("Given parameter looks like a reserved database name.")
