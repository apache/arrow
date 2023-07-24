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

## Getting Started

The following guides explain the fundamental data structures used in the Java implementation of Apache Arrow.

- [ValueVector](https://arrow.apache.org/docs/java/vector.html) is an abstraction that is used to store a sequence of values having the same type in an individual column.
- [VectorSchemaRoot](https://arrow.apache.org/docs/java/vector_schema_root.html) is a container that can hold multiple vectors based on a schema.
- The [Reading/Writing IPC formats](https://arrow.apache.org/docs/java/ipc.html) guide explains how to stream record batches as well as serializing record batches to files.

Generated javadoc documentation is available [here](https://arrow.apache.org/docs/java/).

## Building from source

Refer to [Building Apache Arrow](https://arrow.apache.org/docs/dev/developers/java/building.html) for documentation of environment setup and build instructions.

## Flatbuffers dependency

Arrow uses Google's Flatbuffers to transport metadata.  The java version of the library
requires the generated flatbuffer classes can only be used with the same version that
generated them.  Arrow packages a version of the arrow-vector module that shades flatbuffers
and arrow-format into a single JAR.  Using the classifier "shade-format-flatbuffers" in your
`pom.xml` will make use of this JAR, you can then exclude/resolve the original dependency to
a version of your choosing.

### Updating the flatbuffers generated code

1. Verify that your version of flatc matches the declared dependency:

```bash
$ flatc --version
flatc version 1.12.0

$ grep "dep.fbs.version" java/pom.xml
    <dep.fbs.version>1.12.0</dep.fbs.version>
```

2. Generate the flatbuffer java files by performing the following:

```bash
cd $ARROW_HOME

# remove the existing files
rm -rf java/format/src

# regenerate from the .fbs files
flatc --java -o java/format/src/main/java format/*.fbs

# prepend license header
find java/format/src -type f | while read file; do
  (cat header | while read line; do echo "// $line"; done; cat $file) > $file.tmp
  mv $file.tmp $file
done
```

## Performance Tuning

There are several system/environmental variables that users can configure.  These trade off safety (they turn off checking) for speed.  Typically they are only used in production settings after the code has been thoroughly tested without using them.

* Bounds Checking for memory accesses: Bounds checking is on by default.  You can disable it by setting either the
system property(`arrow.enable_unsafe_memory_access`) or the environmental variable
(`ARROW_ENABLE_UNSAFE_MEMORY_ACCESS`) to `true`. When both the system property and the environmental
variable are set, the system property takes precedence.

* null checking for gets: `ValueVector` get methods (not `getObject`) methods by default verify the slot is not null.  You can disable it by setting either the
system property(`arrow.enable_null_check_for_get`) or the environmental variable
(`ARROW_ENABLE_NULL_CHECK_FOR_GET`) to `false`. When both the system property and the environmental
variable are set, the system property takes precedence.

## Java Properties

 * For Java 9 or later, should set `-Dio.netty.tryReflectionSetAccessible=true`.
This fixes `java.lang.UnsupportedOperationException: sun.misc.Unsafe or java.nio.DirectByteBuffer.(long, int) not available`. thrown by Netty.
 * To support duplicate fields in a `StructVector` enable `-Darrow.struct.conflict.policy=CONFLICT_APPEND`.
Duplicate fields are ignored (`CONFLICT_REPLACE`) by default and overwritten. To support different policies for
conflicting or duplicate fields set this JVM flag or use the correct static constructor methods for `StructVector`s.

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

Refer to [checkstyle.xml](dev/checkstyle/checkstyle.xml) for rule specifics.

## Test Logging Configuration

When running tests, Arrow Java uses the Logback logger with SLF4J. By default,
it uses the `logback.xml` present in the corresponding module's `src/test/resources`
directory, which has the default log level set to `INFO`.
Arrow Java can be built with an alternate logback configuration file using the
following command run in the project root directory:

```bash
mvn -Dlogback.configurationFile=file:<path-of-logback-file>
```

See [Logback Configuration][1] for more details.

## Integration Tests

Integration tests which require more time or more memory can be run by activating
the `integration-tests` profile. This activates the [maven failsafe][4] plugin
and any class prefixed with `IT` will be run during the testing phase. The integration
tests currently require a larger amount of memory (>4GB) and time to complete. To activate
the profile:

```bash
mvn -Pintegration-tests <rest of mvn arguments>
```

[1]: https://logback.qos.ch/manual/configuration.html
[2]: https://github.com/apache/arrow/blob/main/cpp/README.md
[3]: http://google.github.io/styleguide/javaguide.html
[4]: https://maven.apache.org/surefire/maven-failsafe-plugin/
