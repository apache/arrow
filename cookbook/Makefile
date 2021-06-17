all: html


html: py
	@echo "\n\n>>> Cookbooks Available in ./build <<<"


test: pytest


help:
	@echo "make all         Build cookbook for all platforms in HTML, will be available in ./build"
	@echo "make test        Test cookbook for all platforms."
	@echo "make py          Build the Cookbook for Python only."
	@echo "make pytest      Verify the cookbook for Python only."


pydeps:
	@echo ">>> Installing Python Dependencies <<<\n"
	cd python && pip install -r requirements.txt


py: pydeps
	@echo ">>> Building Python Cookbook <<<\n"
	cd python && make html
	mkdir -p build/py
	cp -r python/build/html/* build/py


pytest: pydeps
	@echo ">>> Testing Python Cookbook <<<\n"
	cd python && make doctest

