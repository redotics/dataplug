#
# Dataplug dev's helper
#
# Helpful commands for packaging dataplug
#
# This makefile only concerns packaging and building.
# To be used only if you plan to build your own packages
# for different target platforms
#

clean:
	rm -rf dataplug.egg-info/ build/
	@echo "Cleaning eventual __pycache__ directories"
	find dataplug/ -name "__pycache__"  -type d -exec rm -r "{}" \; > /dev/null 2>&1

build: clean
	@echo " ----- Building universal wheel package"
	python setup.py bdist_wheel
	@echo " ----- built dataplug: $(grep version setup.py)"

pypi: build
	twine upload --repository pypi dist/*

testpypi: build
	twine upload --repository testpypi dist/*
	@# twine upload --repository-url https://test.pypi.org/legacy/ dist/*

test:
	export DATAPLUG_DEFAULT_PORT=7144
	pytest -v tests
