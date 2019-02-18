# build docker compose images:
# $ make cpp
# run the built image:
# $ make run cpp

.PHONY: clean run cpp cpp-alpine go js java rust r

DC := docker-compose

clean:
	$(DC) down -v

run:
	$(DC) run --rm $(filter-out $@,$(MAKECMDGOALS))

go:
	$(DC) build go

js:
	$(DC) build js

java:
	$(DC) build java

rust:
	$(DC) build rust

cpp:
	$(DC) build cpp

cpp-alpine:
	$(DC) build cpp-alpine

cpp-cmake32:
	$(DC) build cpp-cmake32

c_glib: cpp
	$(DC) build c_glib

r: cpp
	$(DC) build r

python: cpp
	$(DC) build python

python-alpine: cpp-alpine
	$(DC) build python-alpine

lint: python
	$(DC) build lint

iwyu: lint

clang-format: lint

docs: python

dask: python
	$(DC) build dask

hdfs: python
	$(DC) build hdfs-integration

spark: python
	$(DC) build spark-integration

pandas-master: python
	$(DC) build --no-cache pandas-master
