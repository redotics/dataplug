""" High level functions

"""
import dataplug

CLIENT = dataplug.Client({"host":"localhost", "port": 7144}}

def create_node(domain, collection, data):
    #CLIENT.set(domain, collection)
    pass

def all(domain, collection, qparams):
    CLIENT.set(domain, collection)
    return CLIENT.all()

