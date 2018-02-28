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
    assert domain == ""
    assert collection == "COL1"
    assert node_id == "COL1/A1"
    assert len(db_config) == 0  # == {}

    (collection,
     domain,
     node_id,
     db_config) = dataplug.utils.extract_info("COL1/A1", {"domain": "DB1"})
    assert domain == ""  # because domain should be defined by a client
    assert collection == "COL1"
    assert node_id == "COL1/A1"
    assert len(db_config) == 1
    assert "domain" in db_config
    assert db_config["domain"] == "DB1"

    node_A = dataplug.Node(key="A/1")

    (collection,
     domain,
     node_id,
     db_config) = dataplug.utils.extract_info(node_A, {"domain": "DB1", "blabla":"yipee"})
    assert domain == ""  # because domain should be defined to a client
    assert collection == "A"
    assert node_id == "A/1"
    assert len(db_config) == 7  # 5 client credentials + 2 here
    assert "domain" in db_config
    assert db_config["domain"] == "DB1"
    assert "blabla" in db_config
    assert db_config["blabla"] == "yipee"
    assert "host" in db_config  # inserted by credentials check
    assert db_config["host"] == "localhost"

    node_B = dataplug.Node(domain="DB2", key="B/81")
    (collection,
     domain,
     node_id,
     db_config) = dataplug.utils.extract_info(node_B, {"domain": "DB1", "blabla":"yipee"})
    assert domain == ""  # No connection info to establish the creation of a domain
    assert collection == "B"
    assert node_id == "B/81"
    assert len(db_config) == 7

    CONNB = {"protocol": "http", "port": 7144}
    node_B = dataplug.Node(domain="DB2", key="B/81", client_config=CONNB)
    (collection,
     domain,
     node_id,
     db_config) = dataplug.utils.extract_info(node_B, {"domain": "DB1", "blabla":"yipee"})
    assert domain == "DB2"
    assert collection == "B"
    assert node_id == "B/81"
    assert len(db_config) == 7
    assert "host" in db_config  # inserted by credentials check
    assert db_config["host"] == "localhost"
    assert "port" in db_config  # inserted by credentials check
    assert db_config["port"] == 7144
    assert "blabla" in db_config
    assert db_config["blabla"] == "yipee"

def test_creation():
    domain = "edgetest"
    # only text
    NODEA = "A/111"
    NODEB = "B/222"
    # node objects without clients
    node_A = dataplug.Node(key=NODEA)
    node_B = dataplug.Node(key=NODEB)
    # node objects with clients
    CONN1 = {"protocol": "http", "port": 7144, "domain": "edgetest_unused"}
    CONN2 = {"protocol": "http", "port": 7144}
    node_1 = dataplug.Node(domain=domain, key=NODEA, client_config=CONN1)
    node_2 = dataplug.Node(domain=domain, key=NODEB, client_config=CONN2)
    # node objects with clients with defined collection
    #   CONN = dataplug.Client({"protocol": "http", "port": 7144, "collection":"C", "domain": "edgetest"})
    #   node_3 = dataplug.Node(key="333", client=CONN)
    #   node_4 = dataplug.Node(key="444", client=CONN)
    #   # node objects with clients with defined collection
    #   CONN = dataplug.Client({"protocol": "http", "port": 7144, "collection":"C", "domain": "edgetest"})
    #   node_5 = dataplug.Node(key=555, client=CONN)
    #   node_6 = dataplug.Node(key=666, client=CONN)

    def test_one_edge(edge, connected=False):
        assert edge.from_collection == "A"
        assert edge.to_collection == "B"
        assert edge.from_id == NODEA
        assert edge.to_id == NODEB
        assert edge.client is not None
        if connected is True:
            assert edge.client.domain.name == domain
            assert edge.client.collection.name == "A__B"
        else:
            assert edge.client.domain is None
            assert edge.client.collection is None

    # edge from text/node to text/node
    edge = dataplug.Edge(domain, NODEA, NODEB)
    test_one_edge(edge)

    edge = dataplug.Edge(domain, node_A, node_B)
    test_one_edge(edge)

    edge = dataplug.Edge(domain, node_1, node_2)
    test_one_edge(edge, connected=True)

    edge = dataplug.Edge(domain, NODEA, node_B)
    test_one_edge(edge)

    edge = dataplug.Edge(domain, node_A, NODEB)
    test_one_edge(edge)

    edge = dataplug.Edge(domain, node_1, NODEB)
    test_one_edge(edge, connected=True)

    edge = dataplug.Edge(domain, NODEA, node_2)
    test_one_edge(edge, connected=True)

    # --- most useful case for backends
    edge = dataplug.Edge(domain, NODEA, NODEB, client_config=CONN2)
    test_one_edge(edge, connected=True)

    # edge from edge to edge
    # edge from text to edge
    # edge from edge to text

def test_update():
    domain = "edgonomy"
    NODEA = "R/111"
    NODEB = "Q/222"
    NODEC = "Q/333"
    CONN = {"protocol": "http", "port": 7144}

    node_1 = dataplug.Node(domain=domain, key=NODEA, client_config=CONN)
    node_1.upsave()
    node_2 = dataplug.Node(domain=domain, key=NODEB, client_config=CONN)
    node_2.upsave()

    edge = dataplug.Edge(domain, NODEA, NODEB, client_config=CONN)
    edge.add_field("A", 34.5).add_field("C", 67)
    edge.upsave()

    edge2 = dataplug.Edge(domain, NODEA, NODEB, client_config=CONN)

    assert "A" not in edge2.data
    assert "C" not in edge2.data

    edge2.sync()

    assert "A" in edge2.data
    assert "C" in edge2.data

    assert edge2.data["A"] == 34.5
    assert edge2.data["C"] == 67
    assert edge2.data["_from"] == NODEA
    assert edge2.data["_to"] == NODEB

    # sync has no impact on non-pre-existing objects
    edge3 = dataplug.Edge(domain, NODEA, NODEC, client_config=CONN)
    edge3.sync()
    assert edge3.data["_from"] == NODEA
    assert edge3.data["_to"] == NODEC
    assert len(edge3.data) == 2

    # check edge3 was not created on the database (no calls to upsave)
    search_result = edge3.client.find(edge3.data)
    assert len(search_result) == 0
