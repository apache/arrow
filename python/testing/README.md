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

# Testing tools for odds and ends

## Testing HDFS file interface

```shell
./test_hdfs.sh
```

## Testing Dask integration

Initial integration testing with Dask has been Dockerized.
To invoke the test run the following command in the `arrow`
root-directory:

```shell
bash dev/dask_integration.sh
```

This script will create a `dask` directory on the same level as
`arrow`. It will clone the Dask project from Github into `dask`
and do a Python `--user` install. The Docker code will use the parent
directory of `arrow` as `$HOME` and that's where Python will
install `dask` into a `.local` directory.

The output of the Docker session will contain the results of tests
of the Dask dataframe followed by the single integration test that
now exists for Arrow. That test creates a set of `csv`-files and then
does parallel reading of `csv`-files into a Dask dataframe. The code
for this test resides here in the `dask_test` directory.
