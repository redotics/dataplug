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

    node_a.data = data
    for field in ["B", "C", "D"]:
        assert field in node_a.data
    assert "A" not in node_a.data


def test_full_key():
    client = dataplug.Client({
        "protocol": "http",
        "port": 7144,
        "domain": "dataflex",
        "collection": "node_test"})
    assert client.is_connected is True

    node_a = dataplug.Node(data={"A": 3.4, "_key": "albert"},
                           client=client,
                           mandatory_features=["B", "C"])
    assert node_a.key() == "albert"
    assert node_a.full_key() == "node_test/albert"

    client.delete_collection()


def test_filter_node_data():
    client = dataplug.Client({
        "protocol": "http",
        "port": 7144,
        "domain": "dataflex",
        "collection": "node_test"})
    assert client.is_connected is True

    node_a = dataplug.Node(data={"A": 3.4, "_key": "albert"},
                           client=client,
                           mandatory_features=["B", "C"])
    node_data = node_a.filter_non_auto_data()
    assert "_key" not in node_data
    assert "_rev" not in node_data
    assert "_id" not in node_data
    assert "A" in node_data
    assert "B" in node_data
    assert "C" in node_data

    node_data = node_a.filter_non_auto_data(keep_fields=["_key"])
    assert "_key" in node_data
    assert "_rev" not in node_data
    assert "_id" not in node_data
    assert "A" in node_data
    assert "B" in node_data
    assert "C" in node_data

    client.delete_collection()


def test_node_upsave():
    client = dataplug.Client({
        "protocol": "http",
        "port": 7144,
        "domain": "dataflex",
        "collection": "node_test"})
    assert client.is_connected is True

    node_nokey1 = dataplug.Node(
                           data={"A": 1.41},
                           client=client,
                           mandatory_features=["A", "B", "C"])
    node_nokey2 = dataplug.Node(
                           data={"A": 1.41, "more": "is_less"},
                           client=client,
                           mandatory_features=["A", "B", "C"])
    node_nokey3 = dataplug.Node(
                           data={"A": 1.41, "tiny": "angstrom"},
                           client=client,
                           mandatory_features=["A", "B", "C"])
    node_key = dataplug.Node(
                           data={"A": 3.14, "_key": "einstein"},
                           client=client,
                           mandatory_features=["A", "B", "C"])

    assert node_nokey1.key() == ""
    assert node_nokey2.key() == ""
    assert node_nokey3.key() == ""
    assert node_key.key() == "einstein"

    assert node_key.upsave() is True
    assert node_key.key() == "einstein"

    assert node_nokey1.upsave() is True
    assert len(node_nokey1.key()) > 0
    assert "more" not in node_nokey1.data
    assert "tiny" not in node_nokey1.data

    assert node_nokey2.upsave(update=True) is True
    assert node_nokey2.key() == node_nokey1.key()
    assert "more" not in node_nokey1.data
    assert "more" in node_nokey2.data
    assert "tiny" not in node_nokey2.data

    assert node_nokey3.upsave() is True
    assert node_nokey3.key() != node_nokey1.key()
    assert node_nokey3.key() != node_nokey2.key()
    assert "more" not in node_nokey3.data
    assert "tiny" in node_nokey3.data

    client.delete_collection()
