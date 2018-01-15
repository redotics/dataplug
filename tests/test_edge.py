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


def create_node_a(client=None):
    return dataplug.Node(
        data={"A": 3.4},
        client=None,
        mandatory_features=["B", "C"])


def test_edge_naming():
    esep = dataplug.client.EDGE_MARKER
    assert dataplug.Edge.edge_naming(["A", "B"]) == ["A"+esep+"B"]
    names = dataplug.Edge.edge_naming(["A", "B", "C"])
    assert len(names) == 2
    assert names[0] == "A"+esep+"B"
    assert names[1] == "B"+esep+"C"


def test_creation():
    # test from text to text
    edge_t_t = dataplug.Edge("A/1", "BBB/123")
    # test from node to node
    # test from text to node
    # test from node to text
    # test from edge to edge
    # test from text to edge
    # test from edge to text
    pass
