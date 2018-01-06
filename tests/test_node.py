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


def test_check_mandatory_fields():
    node_a = create_node_a()
    data = {"B": 2.3, "D": 0.0}
    for field in ["A", "B", "C"]:
        assert field in node_a.data
    assert "D" not in node_a.data

    node_a.data_checkin(data)
    for field in ["B", "C", "D"]:
        assert field in node_a.data
    assert "A" not in node_a.data


def test_full_key():
    client = dataplug.Client({
        "protocol": "http",
        "port": 7144,
        "domain": "dataflex",
        "collection": "node_test"})
    node_a = dataplug.Node(data={"A": 3.4, "_key": "albert"}, client=client, mandatory_features=["B", "C"])
    assert node_a.key() == "albert"
    assert node_a.full_key() == "node_test/albert"
