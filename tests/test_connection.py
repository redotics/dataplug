# import pytest
import os
import sys
sys.path.insert(
    0,
    os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import dataplug

def test_connection():
    # Port is incorrect
    CONN = dataplug.Client({"protocol": "http", "port": 44000, "domain": "dataflex"})
    assert CONN.is_connected is False
    # Port is correct and connection is established, even though domain does not exist
    CONN = dataplug.Client({"protocol": "http", "port": 7144, "domain": "datadoesnotexist"})
    assert CONN.is_connected is True


def test_create_collections():
    Client = dataplug.Client({"protocol": "http", "port": 7144, "domain": "dataflex"})
    assert Client.is_connected
    assert Client.delete_collection() is False
    Client.collection = "new_collection"
    assert Client.delete_collection() is True
    assert Client.delete_collection() is False
