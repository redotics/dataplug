# import pytest
import os
import sys
sys.path.insert(
    0,
    os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import dataplug


"""
    Use database parameters:
        - port 7144
        - no authentication

"""


def test_edge_naming():
    esep = dataplug.client.EDGE_MARKER
    assert dataplug.Edge.edge_naming(["A", "B"]) == ["A"+esep+"B"]
    names = dataplug.Edge.edge_naming(["A", "B", "C"])
    assert len(names) == 2
    assert names[0] == "A"+esep+"B"
    assert names[1] == "B"+esep+"C"


def test_creation():
    NODEA = "A/111"
    NODEB = "B/222"
    node_A = dataplug.Node(node_id=NODEA)
    node_B = dataplug.Node(node_id=NODEB)
    CONN = dataplug.Client({"protocol": "http", "port": 7144, "domain": "edgetest"})
    node_1 = dataplug.Node(key=NODEA, client=CONN)
    node_2 = dataplug.Node(key=NODEB, client=CONN)

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
