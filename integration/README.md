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

# Arrow integration testing

Our strategy for integration testing between Arrow implementations is as follows:

* Test datasets are specified in a custom human-readable, JSON-based format
  designed for Arrow

* Each implementation provides a testing executable capable of converting
  between the JSON and the binary Arrow file representation

* The test executable is also capable of validating the contents of a binary
  file against a corresponding JSON file

## Environment setup

The integration test data generator and runner is written in Python and
currently requires Python 3.5 or higher. You can create a standalone Python
distribution and environment for running the tests by using [miniconda][1]. On
Linux this is:

```shell
MINICONDA_URL=https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh
wget -O miniconda.sh $MINICONDA_URL
bash miniconda.sh -b -p miniconda
export PATH=`pwd`/miniconda/bin:$PATH

conda create -n arrow-integration python=3.6 nomkl numpy six
conda activate arrow-integration
```

If you are on macOS, instead use the URL:

```shell
MINICONDA_URL=https://repo.continuum.io/miniconda/Miniconda3-latest-MacOSX-x86_64.sh
```

After this, you can follow the instructions in the next section.

## Running the existing integration tests

First, build the Java and C++ projects. For Java, you must run

```
mvn package
```

Now, the integration tests rely on two environment variables which point to the
Java `arrow-tool` JAR and the build path for the C++ executables:

```bash
JAVA_DIR=$ARROW_HOME/java
CPP_BUILD_DIR=$ARROW_HOME/cpp/build

VERSION=0.11.0-SNAPSHOT
export ARROW_JAVA_INTEGRATION_JAR=$JAVA_DIR/tools/target/arrow-tools-$VERSION-jar-with-dependencies.jar
export ARROW_CPP_EXE_PATH=$CPP_BUILD_DIR/debug
```

Here `$ARROW_HOME` is the location of your Arrow git clone. The
`$CPP_BUILD_DIR` may be different depending on how you built with CMake
(in-source or out-of-source).

Once this is done, run the integration tests with (optionally adding `--debug`
for additional output)

```
python integration_test.py

python integration_test.py --debug  # additional output
```

[1]: https://conda.io/miniconda.html
