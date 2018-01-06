# DataPlug

A simple timeseries and graph data manager, in other words: on the fly schemaless multi-model data client.

Inspired by InfluxDB, ElasticSearch and other cool stuffs that always miss a little thing: Graphization !


# Main requirements

	+ [Python driver for Arango](https://github.com/joowani/python-arango)
		pip install python-arango
	+ [ArangoDB](https://www.arangodb.com) version > 3.2
	    A multi-model no-sql graph database

# Installation

TODO: pip install dataplug (or/and similar)

Manually: copy the *dataplug/* directory somewhere in your [PYTHONPATH](https://docs.python.org/3/using/cmdline.html#envvar-PYTHONPATH)

# Testing

pytest -v
