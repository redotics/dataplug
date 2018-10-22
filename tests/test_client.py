# import pytest
import os
import sys
import pytest
import arango
sys.path.insert(
    0,
    os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import dataplug

TEST_PORT=7144

def test_empty_client():
    CONN = dataplug.Client("","")
    assert CONN is not None
    CONN = dataplug.Client("","", {})
    assert CONN is not None


def test_check_credentials():
    CONN = dataplug.Client("","", {})
    assert CONN is not None
    CONN.db_config[""] = ""
    assert CONN.db_config["protocol"] == "http"
    CONN.check_credentials()
    assert CONN.db_config["port"] == 8529
    assert CONN.db_config["protocol"] == "http"

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
                            "port": TEST_PORT, "unused": "datadoesnotexist"})
    assert CONN.is_connected() is True
    assert CONN.domain.name == D1
    assert CONN.db.delete_database(D1, ignore_missing=True) is True
    assert CONN.domain.name == D1

    #with pytest.raises(arango.exceptions.ServerConnectionError):
    #    CONN.is_connected()
    assert CONN.is_connected() is True


def test_settings():
    CONN = dataplug.Client("D0", "C0", {"protocol": "http",
                           "port": TEST_PORT, "domain": "dataflex"})
    assert CONN.is_connected() is True 
    assert CONN.domain.name == "D0"
    assert CONN.collection.name == "C0"

    CONN.set("dataflex", "col1")
    assert CONN.domain.name == "dataflex"
    assert CONN.collection.name == "col1"


def test_create_collections():
    D1 = "dataflex"
    C1 = "new_collection"

    Client = dataplug.Client(D1, C1, {"protocol": "http",
                                      "port": TEST_PORT, "domain": "dataflex"})
    assert Client.is_connected() is True
    assert Client.delete_collection() is True
    assert Client.delete_collection() is False
