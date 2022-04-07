""" dataplug setuptools-based setup.

Created thanks to:
https://packaging.python.org/en/latest/distributing.html
https://github.com/pypa/sampleproject
"""

from setuptools import setup, find_packages
from codecs import open
from os import path

here = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    version="3.0.2",
    name="dataplug",
    description="Schemaless, NoSQL, multi-model data interactions on top ArangoDB",
    long_description_content_type="text/markdown",
    long_description=long_description,
    url="https://github.com/redsharpbyte/dataplug",
    license="MIT",
    author="Red Boumghar",
    install_requires=["python-arango"],
    python_requires='>=3',
    extras_require={
        "test": ["pytest"]
    },
    packages=find_packages(exclude=["tests", "docs", "backends"]),
    keywords="schemaless, no-sql, multi-model, data, graph, databasea",
    classifiers=[
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License"
    ],
)
