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

# Developing with Archery

Archery is documented on the Arrow website:

* [Daily development using Archery](https://arrow.apache.org/docs/developers/continuous_integration/archery.html)
* [Using Archery and Crossbow](https://arrow.apache.org/docs/developers/continuous_integration/crossbow.html)
* [Using Archery and Docker](https://arrow.apache.org/docs/developers/continuous_integration/docker.html)

# Installing Archery

See the pages linked above for more details. As a general overview, Archery
comes in a number of subpackages, each needing to be installed if you want
to use the functionality of it:

* lint – lint (and in some cases auto-format) code in the Arrow repo
  To install: `pip install -e "arrow/dev/archery[lint]"`
* benchmark – to run Arrow benchmarks using Archery
  To install: `pip install -e "arrow/dev/archery[benchmark]"`
* docker – to run docker compose based tasks more easily
  To install: `pip install -e "arrow/dev/archery[docker]"`
* release – release related helpers
  To install: `pip install -e "arrow/dev/archery[release]"`
* crossbow – to trigger + interact with the crossbow build system
  To install: `pip install -e "arrow/dev/archery[crossbow]"`

Additionally, if you would prefer to install everything at once,
`pip install -e "arrow/dev/archery[all]"` is an alias for all of
the above subpackages.

## Local development


Archery requires Python 3.11 or newer. From the root of the Arrow repository,
create and activate a virtual environment:

```console
$ python3 -m venv .venv
$ source .venv/bin/activate
```

Install Archery in editable mode with all optional dependencies, followed by
the test dependencies:

```console
$ python -m pip install --upgrade pip
$ python -m pip install -e "dev/archery[all]"
$ python -m pip install -r dev/archery/requirements-test.txt
```

Verify the installation:

```console
$ archery --help
```

Some release tests require historical Arrow tags. If they are not available in
your clone, fetch them from the upstream repository:

```console
$ git fetch upstream --tags
```

Run the complete Archery test suite from the repository root:

```console
$ python -m pytest dev/archery
```

To run only the Crossbow tests:

```console
$ python -m pytest dev/archery/archery/crossbow/tests
```

For some prior art on benchmarking in Arrow, see [this prototype](https://github.com/apache/arrow/tree/0409498819332fc479f8df38babe3426d707fb9e/dev/benchmarking).