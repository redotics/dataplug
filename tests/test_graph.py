import os
import sys
sys.path.insert(
    0,
    os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import dataplug
import testconfig as cfg


def test_graph():
    # Port is incorrect
    client_config = {"protocol": "http", "port": cfg.TEST_PORT, "domain": cfg.TEST_DOM}
    graph_o = dataplug.Graph(client_config["domain"], client_config=client_config)

    # Testing when graph is None
    assert graph_o.outbounds_from_node("nonexistant/123Node") == {}
    assert graph_o.outbounds_from_node("") == {}

