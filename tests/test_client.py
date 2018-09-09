# import pytest
import os
import sys
import pytest
import arango
sys.path.insert(
    0,
    os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import dataplug


def test_empty_client():
    CONN = dataplug.Client("","")
    assert CONN is not None
    CONN = dataplug.Client("","", {})
    assert CONN is not None

def test_connection():
    # Port is incorrect
    # pytest.raises(TypeError, message="...1 required positional argument"):
    with pytest.raises(TypeError, match=r".* required positional argument.*"):
        CONN = dataplug.Client({"protocol": "http",
                                "port": 44000, "domain": "dataflex"})
    CONN = dataplug.Client("D0", "C0", {"protocol": "http",
                           "port": 44000, "domain": "dataflex"})
    assert CONN.is_connected() is False

    # Port is correct and connection is established,
    # even though domain does not exist
    D1 = "newtestdomain"
    CONN = dataplug.Client(D1, "waka",
                           {"protocol": "http",
                            "port": 7144, "unused": "datadoesnotexist"})
    assert CONN.is_connected() is True
    assert CONN.domain.name == D1
    assert CONN.db.delete_database(D1, ignore_missing=True) is True
    assert CONN.domain.name == D1

    #with pytest.raises(arango.exceptions.ServerConnectionError):
    #    CONN.is_connected()
    assert CONN.is_connected() is True

    # Testing when graph is None
    assert CONN.graph_outbounds_from("nonexistant/123Node") == {}
    assert CONN.graph_outbounds_from("") == {}


def test_create_collections():
    D1 = "dataflex"
    C1 = "new_collection"

    Client = dataplug.Client(D1, C1, {"protocol": "http",
                                      "port": 7144, "domain": "dataflex"})
    assert Client.is_connected() is True
    assert Client.delete_collection() is True
    assert Client.delete_collection() is False
