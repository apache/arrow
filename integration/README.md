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
Java `arrow-tool` JAR and the C++ `json-integration-test` executable:

```bash
JAVA_DIR=$ARROW_HOME/java
CPP_BUILD_DIR=$ARROW_HOME/cpp/test-build

VERSION=0.1.1-SNAPSHOT
export ARROW_JAVA_INTEGRATION_JAR=$JAVA_DIR/tools/target/arrow-tools-$VERSION-jar-with-dependencies.jar
export ARROW_CPP_TESTER=$CPP_BUILD_DIR/debug/json-integration-test
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