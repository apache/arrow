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

# Arrow Java

## Setup Build Environment

install:
 - java 7 or later
 - maven 3.3 or later

## Building and running tests

```
cd java
mvn install
```

## Building and running tests for gandiva (optional)

[Gandiva cpp][2] must be built before this step. The cpp build directory must
be provided as the value for argument gandiva.cpp.build.dir. eg.

```
cd java
mvn install -P gandiva -pl gandiva -am -Dgandiva.cpp.build.dir=../../debug
```

This library is still in Alpha stages, and subject to API changes without
deprecation warnings.

## Building and running tests for arrow jni (optional)
Arrow Cpp must be built before this step. The cpp build directory must
be provided as the value for argument arrow.cpp.build.dir. eg.

```
cd java
mvn install -P native-orc -pl arrow-jni -am -Darrow.cpp.build.dir=../../release
```

## Java Code Style Guide

Arrow Java follows the Google style guide [here][3] with the following
differences:

* Imports are grouped, from top to bottom, in this order: static imports,
standard Java, org.\*, com.\*
* Line length can be up to 120 characters
* Operators for line wrapping are at end-of-line
* Naming rules for methods, parameters, etc. have been relaxed
* Disabled `NoFinalizer`, `OverloadMethodsDeclarationOrder`, and
`VariableDeclarationUsageDistance` due to the existing code base. These rules
should be followed when possible.

Refer to `java/dev/checkstyle/checkstyle.xml for rule specifics.

## Test Logging Configuration

When running tests, Arrow Java uses the Logback logger with SLF4J. By default,
it uses the logback.xml present in the corresponding module's src/test/resources
directory, which has the default log level set to INFO.
Arrow Java can be built with an alternate logback configuration file using the
following command run in the project root directory:

```bash
mvn -Dlogback.configurationFile=file:<path-of-logback-file>
```

See [Logback Configuration][1] for more details.

[1]: https://logback.qos.ch/manual/configuration.html
[2]: https://github.com/apache/arrow/blob/master/cpp/README.md
[3]: http://google.github.io/styleguide/javaguide.html
