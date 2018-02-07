<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Benchmarks

The `pyarrow` package comes with a suite of benchmarks meant to
run with [ASV](https://asv.readthedocs.io).  You'll need to install
the `asv` package first (`pip install asv`).

## Running with your local tree

When developing, the simplest and fastest way to run the benchmark suite
against your local changes is to use the `asv dev` command.  This will
use your current Python interpreter and environment.

## Running with arbitrary revisions

ASV allows to store results and generate graphs of the benchmarks over
the project's evolution.  Doing this requires a bit more massaging
currently.

First you have to install ASV's development version:

```shell
pip install git+https://github.com/airspeed-velocity/asv.git
```

<!--- TODO remove the above once https://github.com/airspeed-velocity/asv/pull/611 is merged -->

Then you need to set up a few environment variables:

```shell
export SETUPTOOLS_SCM_PRETEND_VERSION=0.0.1
export PYARROW_BUNDLE_ARROW_CPP=1
```

Now you should be ready to run `asv run` or whatever other command
suits your needs.
