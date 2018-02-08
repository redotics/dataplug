# import pytest
import os
import sys
sys.path.insert(
    0,
    os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import dataplug
import dataplug.utils


"""
    Use database parameters:
        - port 7144
        - no authentication

"""


def test_edge_naming():
    esep = dataplug.utils.EDGE_MARKER
    assert dataplug.utils.edge_naming(["A", "B"]) == ["A"+esep+"B"]
    names = dataplug.utils.edge_naming(["A", "B", "C"])
    assert len(names) == 2
    assert names[0] == "A"+esep+"B"
    assert names[1] == "B"+esep+"C"


def test_extract_info():
    (collection,
     domain,
     node_id,
     db_config) = dataplug.utils.extract_info("COL1/A1", None)
    assert collection == "COL1"
    assert domain == ""
    assert node_id == "COL1/A1"
    assert len(db_config) == 0  # == {}

    (collection,
     domain,
     node_id,
     db_config) = dataplug.utils.extract_info("COL1/A1", {"domain": "DB1"})
    assert collection == "COL1"
    assert domain == "DB1"
    assert node_id == "COL1/A1"
    assert len(db_config) == 1
    assert "domain" in db_config
    assert db_config["domain"] == "DB1"

    node_A = dataplug.Node(key="A/1")
    (collection,
     domain,
     node_id,
     db_config) = dataplug.utils.extract_info(node_A, {"domain": "DB1"})
    assert collection == "A"
    assert domain == "DB1"
    assert node_id == "A/1"
    assert len(db_config) == 1
    assert "domain" in db_config
    assert db_config["domain"] == "DB1"


def test_creation():
    # only text
    NODEA = "A/111"
    NODEB = "B/222"
    # node objects without clients
    node_A = dataplug.Node(key=NODEA)
    node_B = dataplug.Node(key=NODEB)
    # node objects with clients
    CONN = dataplug.Client({"protocol": "http", "port": 7144, "domain": "edgetest"})
    node_1 = dataplug.Node(key=NODEA, client=CONN)
    node_2 = dataplug.Node(key=NODEB, client=CONN)
    # node objects with clients with defined collection
    #   CONN = dataplug.Client({"protocol": "http", "port": 7144, "collection":"C", "domain": "edgetest"})
    #   node_3 = dataplug.Node(key="333", client=CONN)
    #   node_4 = dataplug.Node(key="444", client=CONN)
    #   # node objects with clients with defined collection
    #   CONN = dataplug.Client({"protocol": "http", "port": 7144, "collection":"C", "domain": "edgetest"})
    #   node_5 = dataplug.Node(key=555, client=CONN)
    #   node_6 = dataplug.Node(key=666, client=CONN)

    def test_one_edge(edge):
        assert edge.from_collection == "A"
        assert edge.to_collection == "B"
        assert edge.from_id == NODEA
        assert edge.to_id == NODEB
        assert edge.client is not None
        assert edge.client.db_config["collection"] == "A__B"

    # edge from text/node to text/node
    edge = dataplug.Edge(NODEA, NODEB)
    test_one_edge(edge)

    edge = dataplug.Edge(node_A, node_B)
    test_one_edge(edge)

    edge = dataplug.Edge(node_1, node_2)
    test_one_edge(edge)

    edge = dataplug.Edge(NODEA, node_B)
    test_one_edge(edge)

    edge = dataplug.Edge(node_A, NODEB)
    test_one_edge(edge)

    # edge from edge to edge
    # edge from text to edge
    # edge from edge to text
    pass


