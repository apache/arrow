<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
  -->


> NOTE: For those deploying this database, Postgres does not by default use
> UTF-8, however it is [required for the jsonb][pg-jsonb] format used in
> some columns to always work. This [stackoverflow post][so-utf8] describes
> how to do it for Amazon RDS. This [section of the docs][pg-charset]
> states how to do it in general, i.e.: `initdb -E UTF8`.

# Benchmark database

This directory contains files related to the benchmark database.

- 'ddl/\*.sql' contains the database definition.
- 'examples/' contain code to test the database and demonstrate its use.
- 'Dockerfile' and 'docker-compose.yml' are for developing benchmarks
  against a testing database.
- An auto-generated summary of views in the [Data model][./data_model.rst].

## Setup

To create a 'machine.json' file that will uniquely identify a computer for
benchmark submission, run the provided shell script and fill in the prompts
to identify the GPU.

> NOTE: this does not work on VMs or Windows.

```shell
./make_machine_json.sh
```

Submit the machine details via http using the command

> NOTE: This will only work if we have selected graphql as a client
> and have it running in production or if during development
> you have run `docker-compose up` to create and run both a
> database Docker container and graphql client Docker container.

```shell
./graphql_submit.sh machine machine.json localhost:5000/graphql
```

or submit after starting up the psql client from this directory, using

```
\set content `cat machine.json`
SELECT ingest_machine_view(:'content'::jsonb);
```

> NOTE: If you don't have a "machine.json" file generated,
> use the example file "examples/machine.json" instead.

## Local testing

There is a file named "[.env][.env]" in this directory that is used by
`docker-compose` to set up the postgres user and password for the
local containers. Currentlty the name and password are both
`benchmark`. This will be the password for the psql client as well.

The Postgres Alpine image runs any added '\*.sql' and '\*.sh' scripts placed
in '/docker-entrypoint-initdb.d/' during its startup script, so the local
database will be set up automatically once the container is running.

To start the containers, be sure to have [Docker installed][docker],
and then run the following from this directory (arrow/dev/benchmarking).


```
docker-compose up
```

This will start a process that will show logs from both the running
Postgres container and the running GraphQL container.
To stop the running containers gracefully, background the process
and run

```
docker-compose down
fg  # To re-foreground the backgrounded process while it exits
```

You will still have the container images "benchmarking_pg",
"graphile/postgraphile", and "postgres:11-alpine" on your
computer. You should keep them if you want to run this again.
If you don't, then remove them with the command:

```
docker rmi benchmarking_pg postgres:11-alpine graphile/postgraphile
```

### Postgres client

The `psql` shell client is bundled with the PostgreSQL core distribution
available from the [Postgres download page][postgres-downloads].
Using the `PG_USER` defined in the `.env` file (currently "benchmark"),
the command to connect to the container is:
```shell
psql -h localhost -p 5432 -U benchmark
```
There is an example script in [examples/example.sql](examples/example.sql) that
runs some queries against the database. To run it in the psql client, type
the following in the psql command-line interface:

```
\i examples/example.sql
```

#### Bulk ingestion using CSV

An example CSV file for bulk ingestion is in
[examples/benchmark_run_example.csv](examples/benchmark_run_example.csv).
The columns are listed in the same order as they are defined, to avoid having
to explicitly name every column in ingestion. The "id" column is left empty
and will be automatically assigned on insert.

To ingest the example CSV file from the command line,
use the command below:

```shell
CSV='examples/benchmark_run_example.csv' && \
psql -U benchmark -h localhost -p 5432 \
 -c "\copy benchmark_run_view FROM '${CSV}' WITH (FORMAT csv, HEADER);"
```

#### Bulk ingestion using JSON

To ingest the example JSON file using the psql client, use the command below.

```
\set content `cat examples/benchmark_example.json`
SELECT ingest_benchmark_view(:'content'::jsonb);
```

### HTTP client

This section requires an actual HTTP client to be up, either
for the production database or via the testing setup.
(See the [local testing section](#local-testing) for how to set it up).

The 'graphile/postgraphile' container provides an HTTP interface
to the database via two url routes:

- A GraphiQL page ([localhost:5000/graphiql][graphiql])
  to aid visual exploration of the data model.
  (The `--watch` flag on the command line. Not recommended for production.)
- An endpoint that receives POST requests only (localhost:5000/graphql).

#### Ingestion

The script [graphql_submit.sh](./graphql_submit.sh) simplifies submission
to the database via curl. Examples:

```shell
./graphql_submit.sh benchmarks examples/benchmark_example.json 
./graphql_submit.sh runs examples/benchmark_run_example.json
```

#### Querying

The output of the query is a JSON object that is hard to read on the command line.
Here is an example query in the shell:
```shell
curl -X POST \
  -H "Content-Type: application/json"  \
  --data '{"query": "{projectDetails{ projectName }}"}' \
  localhost:5000/graphql
```

which (if you have previously run the "examples.sql" command) yields

```
{"data":{"projectDetails":{"projectName":"Apache Arrow"}}}
```

Here is an example query using Python:
```python
import json
import requests

uri = "http://localhost:5000/graphql"
query = json.load(open("examples/graphql_query_environment_view.json"))
response = requests.post(uri, json=query)
message = "{benchmarkLanguage}: {languageImplementationVersion}, {dependencies}"

for row in response.json()['data']['allEnvironmentViews']['edges']:
    print(message.format(**row['node']))

# result:
#
# Python: CPython 2.7, {"six":"","numpy":"1.14","other_lib":"1.0"}
# Python: CPython 2.7, {"six":"","numpy":"1.15","other_lib":"1.0"}
# Python: CPython 3.6, {"boost":"1.42","numpy":"1.15"}
```

## Deployment

(work in progress).

> NOTE: For those deploying this database, Postgres does not by default use
> UTF-8, however it is [required for the jsonb][pg-jsonb] format used in
> some columns to always work. This [stackoverflow post][so-utf8] describes
> how to do it for Amazon RDS. This [section of the docs][pg-charset]
> states how to do it in general, i.e.: `initdb -E UTF8`.


## Quick reference

- String variables `'have single quotes'`
- Arrays `'{"have", "curly", "braces"}'::text[]` or `'{1, 2, 3}'::integer[]`
- JSONb `'{"has":"this", "format":42}'::jsonb`
- Elements inserted using JSON-formatted strings can use standard
  JSON-formatted arrays (`[1, 2, 3]`) and do not have to use the above
  string formats.
- When comparing nullable values use `x IS NOT DISTINCT FROM y` rather than `x = y`
- An auto-generated summary of the [Data model][./data_model.rst].

## Data model documentation

To recreate the data model documentation,
(1) install the [psql client][postgres-downloads]
(sorry you need to download the whole thing),
(2) start the docker container using `docker-compose up`,
(3) and then run these scripts:

```
./make_dotfile.sh
./make_data_model_rst.sh
```

[pg-jsonb]: https://www.postgresql.org/docs/11/datatype-json.html#id-1.5.7.22.3
[so-utf8]: https://stackoverflow.com/a/33557023
[pg-charset]: https://www.postgresql.org/docs/9.3/multibyte.html#AEN34424
[docker]: https://www.docker.com/get-started
[citext-limitations]: https://www.postgresql.org/docs/11/citext.html#id-1.11.7.17.7
[postgres-downloads]: https://www.postgresql.org/download/
[graphiql]: http://localhost:5000/graphiql
[postgraphile-lambda]: https://github.com/graphile/postgraphile-lambda-example
[postgraphile-cli]: https://www.graphile.org/postgraphile/usage-cli/
