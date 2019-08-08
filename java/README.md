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
git submodule update --init --recursive # Needed for flight
cd java
mvn install
```
## Building and running tests for arrow jni modules like gandiva and orc (optional)

[Arrow Cpp][2] must be built before this step. The cpp build directory must
be provided as the value for argument arrow.cpp.build.dir. eg.

```
cd java
mvn install -P arrow-jni -am -Darrow.cpp.build.dir=../../release
```

The gandiva library is still in Alpha stages, and subject to API changes without
deprecation warnings.

## Flatbuffers dependency

Arrow uses Google's Flatbuffers to transport metadata.  The java version of the library
requires the generated flatbuffer classes can only be used with the same version that
generated them.  Arrow packages a verion of the arrow-vector module that shades flatbuffers
and arrow-format into a single JAR.  Using the classifier "shade-format-flatbuffers" in your
pom.xml will make use of this JAR, you can then exclude/resolve the original dependency to
a version of your choosing.

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
