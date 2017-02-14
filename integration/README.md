<!---
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->

# Arrow integration testing

Our strategy for integration testing between Arrow implementations is as follows:

* Test datasets are specified in a custom human-readable, JSON-based format
  designed for Arrow

* Each implementation provides a testing executable capable of converting
  between the JSON and the binary Arrow file representation

* The test executable is also capable of validating the contents of a binary
  file against a corresponding JSON file

## Running the existing integration tests

First, build the Java and C++ projects. For Java, you must run

```
mvn package
```

Now, the integration tests rely on two environment variables which point to the
Java `arrow-tool` JAR and the build path for the C++ executables:

```bash
JAVA_DIR=$ARROW_HOME/java
CPP_BUILD_DIR=$ARROW_HOME/cpp/test-build

VERSION=0.1.1-SNAPSHOT
export ARROW_JAVA_INTEGRATION_JAR=$JAVA_DIR/tools/target/arrow-tools-$VERSION-jar-with-dependencies.jar
export ARROW_CPP_EXE_PATH=$CPP_BUILD_DIR/debug
```

Here `$ARROW_HOME` is the location of your Arrow git clone. The
`$CPP_BUILD_DIR` may be different depending on how you built with CMake
(in-source of out-of-source).

Once this is done, run the integration tests with (optionally adding `--debug`
for additional output)

```
python integration_test.py

python integration_test.py --debug  # additional output
```