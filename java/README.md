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
pom.xml will make use of this JAR, you can then exclude/resolve the original dependency to
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
system property("arrow.enable_unsafe_memory_access") or the environmental variable
("ARROW_ENABLE_UNSAFE_MEMORY_ACCESS") to "true". When both the system property and the environmental
variable are set, the system property takes precedence.

* null checking for gets: ValueVector get methods (not getObject) methods by default verify the slot is not null.  You can disable it by setting either the
system property("arrow.enable_null_check_for_get") or the environmental variable
("ARROW_ENABLE_NULL_CHECK_FOR_GET") to "false". When both the system property and the environmental
variable are set, the system property takes precedence.

## Java Properties

 * For java 9 or later, should set "-Dio.netty.tryReflectionSetAccessible=true".
This fixes `java.lang.UnsupportedOperationException: sun.misc.Unsafe or java.nio.DirectByteBuffer.(long, int) not available`. thrown by netty.
 * To support duplicate fields in a `StructVector` enable "-Darrow.struct.conflict.policy=CONFLICT_APPEND".
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

## Integration Tests

Integration tests which require more time or more memory can be run by activating
the `integration-tests` profile. This activates the [maven failsafe][4] plugin
and any class prefixed with `IT` will be run during the testing phase. The integration
tests currently require a larger amount of memory (>4GB) and time to complete. To activate
the profile:

```bash
mvn -Pintegration-tests <rest of mvn arguments>
```

## Java Module System

We are starting to modularize the Arrow Java Components. This is the journey about how do 
we migrate to module system:

```bash

#################
# Main Blockers #
#################

# 1.- There are some modules with the same package name (i.e. org.apache.arrow.memory / io.netty.buffer) 
#     that is consumed by another module with the same package name to access protected methods.
# 2.- Need to rename some modules package name to be complaint with unique package names needed by JPMS module naming.

############################
# Review Arrow Java Format #
############################

$ cd arrow/java/format

# Review Arrow Java Format dependencies
$ jar --describe-module --file target/arrow-format-8.0.0-SNAPSHOT.jar
No module descriptor found. Derived automatic module.
arrow.format@8.0.0-SNAPSHOT automatic
requires java.base mandated
contains org.apache.arrow.flatbuf

$ jdeps target/arrow-format-8.0.0-SNAPSHOT.jar 
arrow-format-8.0.0-SNAPSHOT.jar -> java.base
arrow-format-8.0.0-SNAPSHOT.jar -> not found
   org.apache.arrow.flatbuf                           -> com.google.flatbuffers                             not found
   org.apache.arrow.flatbuf                           -> java.lang                                          java.base
   org.apache.arrow.flatbuf                           -> java.nio                                           java.base
# Then, create module-info.java and requires flatbuffers.java (the name is base on the jar name
# downloaded by Maven that it is flatbuffers-java-1.12.0.jar, and from - to . by conventions)

# Validate new module created
$ jar --describe-module --file target/arrow-format-8.0.0-SNAPSHOT.jar
org.apache.arrow.flatbuf@8.0.0-SNAPSHOT jar:file:///Users/arrow/java/format/target/arrow-format-8.0.0-SNAPSHOT.jar!/module-info.class
exports org.apache.arrow.flatbuf
requires flatbuffers.java
requires java.base mandated

# TODO: 0

############################
# Review Arrow Java Memory #
############################

# 1.- Review Arrow Java Memory -> Core
$ cd arrow/java/memory/memory-core

# Review Arrow Java Memory
$ jar --describe-module --file target/arrow-memory-core-8.0.0-SNAPSHOT.jar 
No module descriptor found. Derived automatic module.

arrow.memory.core@8.0.0-SNAPSHOT automatic
requires java.base mandated
contains org.apache.arrow.memory
contains org.apache.arrow.memory.rounding
contains org.apache.arrow.memory.util
contains org.apache.arrow.memory.util.hash
contains org.apache.arrow.util

$ jdeps target/arrow-memory-core-8.0.0-SNAPSHOT.jar 
arrow-memory-core-8.0.0-SNAPSHOT.jar -> java.base
arrow-memory-core-8.0.0-SNAPSHOT.jar -> jdk.unsupported
arrow-memory-core-8.0.0-SNAPSHOT.jar -> not found
   org.apache.arrow.memory                            -> java.io                                            java.base
   org.apache.arrow.memory                            -> java.lang                                          java.base
   org.apache.arrow.memory                            -> java.lang.invoke                                   java.base
   org.apache.arrow.memory                            -> java.lang.reflect                                  java.base
   org.apache.arrow.memory                            -> java.net                                           java.base
   org.apache.arrow.memory                            -> java.nio                                           java.base
   org.apache.arrow.memory                            -> java.util                                          java.base
   org.apache.arrow.memory                            -> java.util.concurrent.atomic                        java.base
   org.apache.arrow.memory                            -> java.util.function                                 java.base
   org.apache.arrow.memory                            -> javax.annotation                                   not found
   org.apache.arrow.memory                            -> org.apache.arrow.memory.rounding                   arrow-memory-core-8.0.0-SNAPSHOT.jar
   org.apache.arrow.memory                            -> org.apache.arrow.memory.util                       arrow-memory-core-8.0.0-SNAPSHOT.jar
   org.apache.arrow.memory                            -> org.apache.arrow.util                              arrow-memory-core-8.0.0-SNAPSHOT.jar
   org.apache.arrow.memory                            -> org.immutables.value                               not found
   org.apache.arrow.memory                            -> org.slf4j                                          not found
   org.apache.arrow.memory                            -> sun.misc                                           JDK internal API (jdk.unsupported)
   org.apache.arrow.memory.rounding                   -> java.lang                                          java.base
   org.apache.arrow.memory.rounding                   -> org.apache.arrow.memory.util                       arrow-memory-core-8.0.0-SNAPSHOT.jar
   org.apache.arrow.memory.rounding                   -> org.apache.arrow.util                              arrow-memory-core-8.0.0-SNAPSHOT.jar
   org.apache.arrow.memory.rounding                   -> org.slf4j                                          not found
   org.apache.arrow.memory.util                       -> java.lang                                          java.base
   org.apache.arrow.memory.util                       -> java.lang.reflect                                  java.base
   org.apache.arrow.memory.util                       -> java.nio                                           java.base
   org.apache.arrow.memory.util                       -> java.security                                      java.base
   org.apache.arrow.memory.util                       -> java.util                                          java.base
   org.apache.arrow.memory.util                       -> java.util.concurrent.locks                         java.base
   org.apache.arrow.memory.util                       -> org.apache.arrow.memory                            arrow-memory-core-8.0.0-SNAPSHOT.jar
   org.apache.arrow.memory.util                       -> org.apache.arrow.memory.util.hash                  arrow-memory-core-8.0.0-SNAPSHOT.jar
   org.apache.arrow.memory.util                       -> org.apache.arrow.util                              arrow-memory-core-8.0.0-SNAPSHOT.jar
   org.apache.arrow.memory.util                       -> org.slf4j                                          not found
   org.apache.arrow.memory.util                       -> sun.misc                                           JDK internal API (jdk.unsupported)
   org.apache.arrow.memory.util.hash                  -> java.lang                                          java.base
   org.apache.arrow.memory.util.hash                  -> org.apache.arrow.memory                            arrow-memory-core-8.0.0-SNAPSHOT.jar
   org.apache.arrow.memory.util.hash                  -> org.apache.arrow.memory.util                       arrow-memory-core-8.0.0-SNAPSHOT.jar
   org.apache.arrow.memory.util.hash                  -> sun.misc                                           JDK internal API (jdk.unsupported)
   org.apache.arrow.util                              -> java.lang                                          java.base
   org.apache.arrow.util                              -> java.lang.annotation                               java.base
   org.apache.arrow.util                              -> java.lang.invoke                                   java.base
   org.apache.arrow.util                              -> java.util                                          java.base
   org.apache.arrow.util                              -> java.util.function                                 java.base
   org.apache.arrow.util                              -> java.util.stream                                   java.base

# Validate new module created
$ jar --describe-module --file target/arrow-memory-core-8.0.0-SNAPSHOT.jar 
arrow.memory_core@8.0.0-SNAPSHOT jar:file:///Users/arrow/java/memory/memory-core/target/arrow-memory-core-8.0.0-SNAPSHOT.jar/!module-info.class
requires java.base mandated
requires jdk.unsupported
requires jsr305
requires org.immutables.value
requires org.slf4j
opens org.apache.arrow.memory
opens org.apache.arrow.util

# 2.- Review Arrow Java Memory -> Netty
$ cd arrow/java/memory/memory-netty
# Consider: Was needed to patch io.netty.buffer with arrow functionalities extended

# Review Arrow Java Memory Netty
$ jar --describe-module --file target/arrow-memory-netty-8.0.0-SNAPSHOT.jar 
No module descriptor found. Derived automatic module.

arrow.memory.netty@8.0.0-SNAPSHOT automatic
requires java.base mandated
contains io.netty.buffer
contains org.apache.arrow.memory

$ jdeps target/arrow-memory-netty-8.0.0-SNAPSHOT.jar 
arrow-memory-netty-8.0.0-SNAPSHOT.jar -> java.base
arrow-memory-netty-8.0.0-SNAPSHOT.jar -> not found
   io.netty.buffer                                    -> io.netty.util                                      not found
   io.netty.buffer                                    -> io.netty.util.internal                             not found
   io.netty.buffer                                    -> java.io                                            java.base
   io.netty.buffer                                    -> java.lang                                          java.base
   io.netty.buffer                                    -> java.lang.reflect                                  java.base
   io.netty.buffer                                    -> java.nio                                           java.base
   io.netty.buffer                                    -> java.nio.channels                                  java.base
   io.netty.buffer                                    -> java.nio.charset                                   java.base
   io.netty.buffer                                    -> java.util.concurrent.atomic                        java.base
   io.netty.buffer                                    -> org.apache.arrow.memory                            arrow-memory-netty-8.0.0-SNAPSHOT.jar
   io.netty.buffer                                    -> org.apache.arrow.memory                            not found
   io.netty.buffer                                    -> org.apache.arrow.memory.util                       not found
   io.netty.buffer                                    -> org.apache.arrow.util                              not found
   io.netty.buffer                                    -> org.slf4j                                          not found
   org.apache.arrow.memory                            -> io.netty.buffer                                    not found
   org.apache.arrow.memory                            -> io.netty.buffer                                    arrow-memory-netty-8.0.0-SNAPSHOT.jar
   org.apache.arrow.memory                            -> io.netty.util.internal                             not found
   org.apache.arrow.memory                            -> java.lang                                          java.base

# Validate new module created
$ jar --describe-module --file target/arrow-memory-netty-8.0.0-SNAPSHOT.jar 
arrow.memory.netty@8.0.0-SNAPSHOT jar:file:///Users/arrow/java/memory/memory-netty/target/arrow-memory-netty-8.0.0-SNAPSHOT.jar/!module-info.class
exports org.apache.arrow.memory.netty
requires arrow.memory.core
requires io.netty.buffer
requires io.netty.common
requires java.base mandated
requires org.immutables.value
requires org.slf4j

# 2.- Review Arrow Java Memory -> Unsafe
$ cd arrow/java/memory/memory-unsafe

# Review Arrow Java Memory Netty
$ jar --describe-module --file target/arrow-memory-unsafe-8.0.0-SNAPSHOT.jar 
No module descriptor found. Derived automatic module.

arrow.memory.unsafe@8.0.0-SNAPSHOT automatic
requires java.base mandated
contains org.apache.arrow.memory

$ jdeps target/arrow-memory-unsafe-8.0.0-SNAPSHOT.jar 
arrow-memory-unsafe-8.0.0-SNAPSHOT.jar -> java.base
arrow-memory-unsafe-8.0.0-SNAPSHOT.jar -> jdk.unsupported
arrow-memory-unsafe-8.0.0-SNAPSHOT.jar -> not found
   org.apache.arrow.memory                            -> java.lang                                          java.base
   org.apache.arrow.memory                            -> org.apache.arrow.memory.util                       not found
   org.apache.arrow.memory                            -> sun.misc                                           JDK internal API (jdk.unsupported)

# Validate new module created
$ jar --describe-module --file target/arrow-memory-unsafe-8.0.0-SNAPSHOT.jar
arrow.memory.unsafe@8.0.0-SNAPSHOT jar:file:///Users/arrow/java/memory/memory-unsafe/target/arrow-memory-unsafe-8.0.0-SNAPSHOT.jar/!module-info.class
exports org.apache.arrow.memory.unsafe
requires arrow.memory.core
requires java.base mandated
requires jdk.unsupported

# TODO:
# Main code: OK
# Test code: Need refactor to access protected methods for unit test. Current workaround is expose protected methods 
# as public methods, this is only for testing purpose.


#######################
# Review Arrow Vector #
#######################

# 1.- Review Arrow Vector
$ cd arrow/java/vector

# Review Arrow Java Vector
$ jar --describe-module --file target/arrow-vector-8.0.0-SNAPSHOT.jar 
No module descriptor found. Derived automatic module.

arrow.vector@8.0.0-SNAPSHOT automatic
requires java.base mandated
contains org.apache.arrow.vector
contains org.apache.arrow.vector.compare
contains org.apache.arrow.vector.compare.util
contains org.apache.arrow.vector.complex
contains org.apache.arrow.vector.complex.impl
contains org.apache.arrow.vector.complex.reader
contains org.apache.arrow.vector.complex.writer
contains org.apache.arrow.vector.compression
contains org.apache.arrow.vector.dictionary
contains org.apache.arrow.vector.holders
contains org.apache.arrow.vector.ipc
contains org.apache.arrow.vector.ipc.message
contains org.apache.arrow.vector.types
contains org.apache.arrow.vector.types.pojo
contains org.apache.arrow.vector.util
contains org.apache.arrow.vector.validate

$ jdeps target/arrow-vector-8.0.0-SNAPSHOT.jar 
arrow-vector-8.0.0-SNAPSHOT.jar -> java.base
arrow-vector-8.0.0-SNAPSHOT.jar -> not found
   org.apache.arrow.vector                            -> io.netty.util.internal                             not found
   org.apache.arrow.vector                            -> java.io                                            java.base
   org.apache.arrow.vector                            -> java.lang                                          java.base
   org.apache.arrow.vector                            -> java.lang.invoke                                   java.base
   org.apache.arrow.vector                            -> java.math                                          java.base
   org.apache.arrow.vector                            -> java.nio                                           java.base
   org.apache.arrow.vector                            -> java.nio.charset                                   java.base
   org.apache.arrow.vector                            -> java.time                                          java.base
   org.apache.arrow.vector                            -> java.util                                          java.base
   org.apache.arrow.vector                            -> java.util.concurrent                               java.base
   org.apache.arrow.vector                            -> java.util.function                                 java.base
   org.apache.arrow.vector                            -> java.util.stream                                   java.base
   org.apache.arrow.vector                            -> org.apache.arrow.memory                            not found
   org.apache.arrow.vector                            -> org.apache.arrow.memory.rounding                   not found
   org.apache.arrow.vector                            -> org.apache.arrow.memory.util                       not found
   org.apache.arrow.vector                            -> org.apache.arrow.memory.util.hash                  not found
   org.apache.arrow.vector                            -> org.apache.arrow.util                              not found
   org.apache.arrow.vector                            -> org.apache.arrow.vector.compare                    arrow-vector-8.0.0-SNAPSHOT.jar
   org.apache.arrow.vector                            -> org.apache.arrow.vector.compare.util               arrow-vector-8.0.0-SNAPSHOT.jar
   org.apache.arrow.vector                            -> org.apache.arrow.vector.complex                    arrow-vector-8.0.0-SNAPSHOT.jar
   org.apache.arrow.vector                            -> org.apache.arrow.vector.complex.impl               arrow-vector-8.0.0-SNAPSHOT.jar
   org.apache.arrow.vector                            -> org.apache.arrow.vector.complex.reader             arrow-vector-8.0.0-SNAPSHOT.jar
   org.apache.arrow.vector                            -> org.apache.arrow.vector.compression                arrow-vector-8.0.0-SNAPSHOT.jar
   org.apache.arrow.vector                            -> org.apache.arrow.vector.holders                    arrow-vector-8.0.0-SNAPSHOT.jar
   org.apache.arrow.vector                            -> org.apache.arrow.vector.ipc.message                arrow-vector-8.0.0-SNAPSHOT.jar
   org.apache.arrow.vector                            -> org.apache.arrow.vector.types                      arrow-vector-8.0.0-SNAPSHOT.jar
   org.apache.arrow.vector                            -> org.apache.arrow.vector.types.pojo                 arrow-vector-8.0.0-SNAPSHOT.jar
   org.apache.arrow.vector                            -> org.apache.arrow.vector.util                       arrow-vector-8.0.0-SNAPSHOT.jar
   org.apache.arrow.vector                            -> org.slf4j                                          not found
   org.apache.arrow.vector.compare                    -> java.lang                                          java.base
   org.apache.arrow.vector.compare                    -> java.lang.invoke                                   java.base
   org.apache.arrow.vector.compare                    -> java.util                                          java.base
   org.apache.arrow.vector.compare                    -> java.util.function                                 java.base
   org.apache.arrow.vector.compare                    -> org.apache.arrow.memory                            not found
   org.apache.arrow.vector.compare                    -> org.apache.arrow.memory.util                       not found
   org.apache.arrow.vector.compare                    -> org.apache.arrow.util                              not found
   org.apache.arrow.vector.compare                    -> org.apache.arrow.vector                            arrow-vector-8.0.0-SNAPSHOT.jar
   org.apache.arrow.vector.compare                    -> org.apache.arrow.vector.compare.util               arrow-vector-8.0.0-SNAPSHOT.jar
   org.apache.arrow.vector.compare                    -> org.apache.arrow.vector.complex                    arrow-vector-8.0.0-SNAPSHOT.jar
   org.apache.arrow.vector.compare                    -> org.apache.arrow.vector.types.pojo                 arrow-vector-8.0.0-SNAPSHOT.jar
   org.apache.arrow.vector.compare.util               -> java.lang                                          java.base
   org.apache.arrow.vector.compare.util               -> org.apache.arrow.vector                            arrow-vector-8.0.0-SNAPSHOT.jar
   org.apache.arrow.vector.compare.util               -> org.apache.arrow.vector.compare                    arrow-vector-8.0.0-SNAPSHOT.jar
   org.apache.arrow.vector.complex                    -> java.lang                                          java.base
   org.apache.arrow.vector.complex                    -> java.lang.invoke                                   java.base
   org.apache.arrow.vector.complex                    -> java.util                                          java.base
   org.apache.arrow.vector.complex                    -> java.util.function                                 java.base
   org.apache.arrow.vector.complex                    -> java.util.stream                                   java.base
   org.apache.arrow.vector.complex                    -> org.apache.arrow.memory                            not found
   org.apache.arrow.vector.complex                    -> org.apache.arrow.memory.util                       not found
   org.apache.arrow.vector.complex                    -> org.apache.arrow.memory.util.hash                  not found
   org.apache.arrow.vector.complex                    -> org.apache.arrow.util                              not found
   org.apache.arrow.vector.complex                    -> org.apache.arrow.vector                            arrow-vector-8.0.0-SNAPSHOT.jar
   org.apache.arrow.vector.complex                    -> org.apache.arrow.vector.compare                    arrow-vector-8.0.0-SNAPSHOT.jar
   org.apache.arrow.vector.complex                    -> org.apache.arrow.vector.complex.impl               arrow-vector-8.0.0-SNAPSHOT.jar
   org.apache.arrow.vector.complex                    -> org.apache.arrow.vector.complex.reader             arrow-vector-8.0.0-SNAPSHOT.jar
   org.apache.arrow.vector.complex                    -> org.apache.arrow.vector.complex.writer             arrow-vector-8.0.0-SNAPSHOT.jar
   org.apache.arrow.vector.complex                    -> org.apache.arrow.vector.holders                    arrow-vector-8.0.0-SNAPSHOT.jar
   org.apache.arrow.vector.complex                    -> org.apache.arrow.vector.ipc.message                arrow-vector-8.0.0-SNAPSHOT.jar
   org.apache.arrow.vector.complex                    -> org.apache.arrow.vector.types                      arrow-vector-8.0.0-SNAPSHOT.jar
   org.apache.arrow.vector.complex                    -> org.apache.arrow.vector.types.pojo                 arrow-vector-8.0.0-SNAPSHOT.jar
   org.apache.arrow.vector.complex                    -> org.apache.arrow.vector.util                       arrow-vector-8.0.0-SNAPSHOT.jar
   org.apache.arrow.vector.complex                    -> org.slf4j                                          not found
   org.apache.arrow.vector.complex.impl               -> java.lang                                          java.base
   org.apache.arrow.vector.complex.impl               -> java.math                                          java.base
   org.apache.arrow.vector.complex.impl               -> java.time                                          java.base
   org.apache.arrow.vector.complex.impl               -> java.util                                          java.base
   org.apache.arrow.vector.complex.impl               -> java.util.concurrent                               java.base
   org.apache.arrow.vector.complex.impl               -> org.apache.arrow.memory                            not found
   org.apache.arrow.vector.complex.impl               -> org.apache.arrow.memory.util                       not found
   org.apache.arrow.vector.complex.impl               -> org.apache.arrow.util                              not found
   org.apache.arrow.vector.complex.impl               -> org.apache.arrow.vector                            arrow-vector-8.0.0-SNAPSHOT.jar
   org.apache.arrow.vector.complex.impl               -> org.apache.arrow.vector.complex                    arrow-vector-8.0.0-SNAPSHOT.jar
   org.apache.arrow.vector.complex.impl               -> org.apache.arrow.vector.complex.reader             arrow-vector-8.0.0-SNAPSHOT.jar
   org.apache.arrow.vector.complex.impl               -> org.apache.arrow.vector.complex.writer             arrow-vector-8.0.0-SNAPSHOT.jar
   org.apache.arrow.vector.complex.impl               -> org.apache.arrow.vector.holders                    arrow-vector-8.0.0-SNAPSHOT.jar
   org.apache.arrow.vector.complex.impl               -> org.apache.arrow.vector.types                      arrow-vector-8.0.0-SNAPSHOT.jar
   org.apache.arrow.vector.complex.impl               -> org.apache.arrow.vector.types.pojo                 arrow-vector-8.0.0-SNAPSHOT.jar
   org.apache.arrow.vector.complex.impl               -> org.apache.arrow.vector.util                       arrow-vector-8.0.0-SNAPSHOT.jar
   org.apache.arrow.vector.complex.impl               -> org.slf4j                                          not found
   org.apache.arrow.vector.complex.reader             -> java.lang                                          java.base
   org.apache.arrow.vector.complex.reader             -> java.math                                          java.base
   org.apache.arrow.vector.complex.reader             -> java.time                                          java.base
   org.apache.arrow.vector.complex.reader             -> org.apache.arrow.vector                            arrow-vector-8.0.0-SNAPSHOT.jar
   org.apache.arrow.vector.complex.reader             -> org.apache.arrow.vector.complex                    arrow-vector-8.0.0-SNAPSHOT.jar
   org.apache.arrow.vector.complex.reader             -> org.apache.arrow.vector.complex.impl               arrow-vector-8.0.0-SNAPSHOT.jar
   org.apache.arrow.vector.complex.reader             -> org.apache.arrow.vector.complex.writer             arrow-vector-8.0.0-SNAPSHOT.jar
   org.apache.arrow.vector.complex.reader             -> org.apache.arrow.vector.holders                    arrow-vector-8.0.0-SNAPSHOT.jar
   org.apache.arrow.vector.complex.reader             -> org.apache.arrow.vector.types                      arrow-vector-8.0.0-SNAPSHOT.jar
   org.apache.arrow.vector.complex.reader             -> org.apache.arrow.vector.types.pojo                 arrow-vector-8.0.0-SNAPSHOT.jar
   org.apache.arrow.vector.complex.reader             -> org.apache.arrow.vector.util                       arrow-vector-8.0.0-SNAPSHOT.jar
   org.apache.arrow.vector.complex.writer             -> java.lang                                          java.base
   org.apache.arrow.vector.complex.writer             -> java.math                                          java.base
   org.apache.arrow.vector.complex.writer             -> org.apache.arrow.memory                            not found
   org.apache.arrow.vector.complex.writer             -> org.apache.arrow.vector.complex                    arrow-vector-8.0.0-SNAPSHOT.jar
   org.apache.arrow.vector.complex.writer             -> org.apache.arrow.vector.complex.reader             arrow-vector-8.0.0-SNAPSHOT.jar
   org.apache.arrow.vector.complex.writer             -> org.apache.arrow.vector.holders                    arrow-vector-8.0.0-SNAPSHOT.jar
   org.apache.arrow.vector.complex.writer             -> org.ap**ache.arrow.vector.types                      arrow-vector-8.0.0-SNAPSHOT.jar
   org.apache.arrow.vector.complex.writer             -> org.apache.arrow.vector.types.pojo                 arrow-vector-8.0.0-SNAPSHOT.jar
   org.apache.arrow.vector.compression                -> java.lang                                          java.base
   org.apache.arrow.vector.compression                -> org.apache.arrow.flatbuf                           not found
   org.apache.arrow.vector.compression                -> org.apache.arrow.memory                            not found
   org.apache.arrow.vector.compression                -> org.apache.arrow.memory.util                       not found
   org.apache.arrow.vector.compression                -> org.apache.arrow.util                              not found
   org.apache.arrow.vector.compression                -> org.apache.arrow.vector.ipc.message                arrow-vector-8.0.0-SNAPSHOT.jar
   org.apache.arrow.vector.dictionary                 -> java.lang                                          java.base
   org.apache.arrow.vector.dictionary                 -> java.lang.invoke                                   java.base
   org.apache.arrow.vector.dictionary                 -> java.util                                          java.base**
   org.apache.arrow.vector.dictionary                 -> java.util.function                                 java.base
   org.apache.arrow.vector.dictionary                 -> org.apache.arrow.memory                            not found
   org.apache.arrow.vector.dictionary                 -> org.apache.arrow.memory.util.hash                  not found
   org.apache.arrow.vector.dictionary                 -> org.apache.arrow.util                              not found
   org.apache.arrow.vector.dictionary                 -> org.apache.arrow.vector                            arrow-vector-8.0.0-SNAPSHOT.jar
   org.apache.arrow.vector.dictionary                 -> org.apache.arrow.vector.compare                    arrow-vector-8.0.0-SNAPSHOT.jar
   org.apache.arrow.vector.dictionary                 -> org.apache.arrow.vector.complex                    arrow-vector-8.0.0-SNAPSHOT.jar
   org.apache.arrow.vector.dictionary                 -> org.apache.arrow.vector.ipc.message                arrow-vector-8.0.0-SNAPSHOT.jar
   org.apache.arrow.vector.dictionary                 -> org.apache.arrow.vector.types.pojo                 arrow-vector-8.0.0-SNAPSHOT.jar
   org.apache.arrow.vector.dictionary                 -> org.apache.arrow.vector.util                       arrow-vector-8.0.0-SNAPSHOT.jar
   org.apache.arrow.vector.holders                    -> java.lang                                          java.base
   org.apache.arrow.vector.holders                    -> org.apache.arrow.memory                            not found
   org.apache.arrow.vector.holders                    -> org.apache.arrow.vector.complex.reader             arrow-vector-8.0.0-SNAPSHOT.jar
   org.apache.arrow.vector.holders                    -> org.apache.arrow.vector.types                      arrow-vector-8.0.0-SNAPSHOT.jar
   org.apache.arrow.vector.ipc                        -> com.fasterxml.jackson.core                         not found
   org.apache.arrow.vector.ipc                        -> com.fasterxml.jackson.core.util                    not found
   org.apache.arrow.vector.ipc                        -> com.fasterxml.jackson.databind                     not found
   org.apache.arrow.vector.ipc                        -> com.google.flatbuffers                             not found
   org.apache.arrow.vector.ipc                        -> java.io                                            java.base
   org.apache.arrow.vector.ipc                        -> java.lang                                          java.base
   org.apache.arrow.vector.ipc                        -> java.math                                          java.base
   org.apache.arrow.vector.ipc                        -> java.nio                                           java.base
   org.apache.arrow.vector.ipc                        -> java.nio.channels                                  java.base
   org.apache.arrow.vector.ipc                        -> java.nio.charset                                   java.base
   org.apache.arrow.vector.ipc                        -> java.util                                          java.base
   org.apache.arrow.vector.ipc                        -> org.apache.arrow.flatbuf                           not found
   org.apache.arrow.vector.ipc                        -> org.apache.arrow.memory                            not found
   org.apache.arrow.vector.ipc                        -> org.apache.arrow.util                              not found
   org.apache.arrow.vector.ipc                        -> org.apache.arrow.vector                            arrow-vector-8.0.0-SNAPSHOT.jar
   org.apache.arrow.vector.ipc                        -> org.apache.arrow.vector.compression                arrow-vector-8.0.0-SNAPSHOT.jar
   org.apache.arrow.vector.ipc                        -> org.apache.arrow.vector.dictionary                 arrow-vector-8.0.0-SNAPSHOT.jar
   org.apache.arrow.vector.ipc                        -> org.apache.arrow.vector.ipc.message                arrow-vector-8.0.0-SNAPSHOT.jar
   org.apache.arrow.vector.ipc                        -> org.apache.arrow.vector.types                      arrow-vector-8.0.0-SNAPSHOT.jar
   org.apache.arrow.vector.ipc                        -> org.apache.arrow.vector.types.pojo                 arrow-vector-8.0.0-SNAPSHOT.jar
   org.apache.arrow.vector.ipc                        -> org.apache.arrow.vector.util                       arrow-vector-8.0.0-SNAPSHOT.jar
   org.apache.arrow.vector.ipc                        -> org.apache.arrow.vector.validate                   arrow-vector-8.0.0-SNAPSHOT.jar
   org.apache.arrow.vector.ipc                        -> org.apache.commons.codec                           not found
   org.apache.arrow.vector.ipc                        -> org.apache.commons.codec.binary                    not found
   org.apache.arrow.vector.ipc                        -> org.slf4j                                          not found
   org.apache.arrow.vector.ipc.message                -> com.google.flatbuffers                             not found
   org.apache.arrow.vector.ipc.message                -> java.io                                            java.base
   org.apache.arrow.vector.ipc.message                -> java.lang                                          java.base
   org.apache.arrow.vector.ipc.message                -> java.lang.invoke                                   java.base
   org.apache.arrow.vector.ipc.message                -> java.nio                                           java.base
   org.apache.arrow.vector.ipc.message                -> java.util                                          java.base
   org.apache.arrow.vector.ipc.message                -> java.util.function                                 java.base
   org.apache.arrow.vector.ipc.message                -> java.util.stream                                   java.base
   org.apache.arrow.vector.ipc.message                -> org.apache.arrow.flatbuf                           not found
   org.apache.arrow.vector.ipc.message                -> org.apache.arrow.memory                            not found
   org.apache.arrow.vector.ipc.message                -> org.apache.arrow.memory.util                       not found
   org.apache.arrow.vector.ipc.message                -> org.apache.arrow.util                              not found
   org.apache.arrow.vector.ipc.message                -> org.apache.arrow.vector.compression                arrow-vector-8.0.0-SNAPSHOT.jar
   org.apache.arrow.vector.ipc.message                -> org.apache.arrow.vector.ipc                        arrow-vector-8.0.0-SNAPSHOT.jar
   org.apache.arrow.vector.ipc.message                -> org.apache.arrow.vector.types                      arrow-vector-8.0.0-SNAPSHOT.jar
   org.apache.arrow.vector.ipc.message                -> org.apache.arrow.vector.types.pojo                 arrow-vector-8.0.0-SNAPSHOT.jar
   org.apache.arrow.vector.ipc.message                -> org.apache.arrow.vector.util                       arrow-vector-8.0.0-SNAPSHOT.jar
   org.apache.arrow.vector.ipc.message                -> org.slf4j                                          not found
   org.apache.arrow.vector.types                      -> java.lang                                          java.base
   org.apache.arrow.vector.types                      -> java.util                                          java.base
   org.apache.arrow.vector.types                      -> org.apache.arrow.flatbuf                           not found
   org.apache.arrow.vector.types                      -> org.apache.arrow.memory                            not found
   org.apache.arrow.vector.types                      -> org.apache.arrow.vector                            arrow-vector-8.0.0-SNAPSHOT.jar
   org.apache.arrow.vector.types                      -> org.apache.arrow.vector.complex                    arrow-vector-8.0.0-SNAPSHOT.jar
   org.apache.arrow.vector.types                      -> org.apache.arrow.vector.complex.impl               arrow-vector-8.0.0-SNAPSHOT.jar
   org.apache.arrow.vector.types                      -> org.apache.arrow.vector.complex.writer             arrow-vector-8.0.0-SNAPSHOT.jar
   org.apache.arrow.vector.types                      -> org.apache.arrow.vector.types.pojo                 arrow-vector-8.0.0-SNAPSHOT.jar
   org.apache.arrow.vector.types                      -> org.apache.arrow.vector.util                       arrow-vector-8.0.0-SNAPSHOT.jar
   org.apache.arrow.vector.types.pojo                 -> com.fasterxml.jackson.annotation                   not found
   org.apache.arrow.vector.types.pojo                 -> com.fasterxml.jackson.core                         not found
   org.apache.arrow.vector.types.pojo                 -> com.fasterxml.jackson.databind                     not found
   org.apache.arrow.vector.types.pojo                 -> com.google.flatbuffers                             not found
   org.apache.arrow.vector.types.pojo                 -> java.io                                            java.base
   org.apache.arrow.vector.types.pojo                 -> java.lang                                          java.base
   org.apache.arrow.vector.types.pojo                 -> java.lang.invoke                                   java.base
   org.apache.arrow.vector.types.pojo                 -> java.nio                                           java.base
   org.apache.arrow.vector.types.pojo                 -> java.util                                          java.base
   org.apache.arrow.vector.types.pojo                 -> java.util.concurrent                               java.base
   org.apache.arrow.vector.types.pojo                 -> java.util.function                                 java.base
   org.apache.arrow.vector.types.pojo                 -> java.util.stream                                   java.base
   org.apache.arrow.vector.types.pojo                 -> org.apache.arrow.flatbuf                           not found
   org.apache.arrow.vector.types.pojo                 -> org.apache.arrow.memory                            not found
   org.apache.arrow.vector.types.pojo                 -> org.apache.arrow.util                              not found
   org.apache.arrow.vector.types.pojo                 -> org.apache.arrow.vector                            arrow-vector-8.0.0-SNAPSHOT.jar
   org.apache.arrow.vector.types.pojo                 -> org.apache.arrow.vector.complex                    arrow-vector-8.0.0-SNAPSHOT.jar
   org.apache.arrow.vector.types.pojo                 -> org.apache.arrow.vector.ipc.message                arrow-vector-8.0.0-SNAPSHOT.jar
   org.apache.arrow.vector.types.pojo                 -> org.apache.arrow.vector.types                      arrow-vector-8.0.0-SNAPSHOT.jar
   org.apache.arrow.vector.types.pojo                 -> org.apache.arrow.vector.util                       arrow-vector-8.0.0-SNAPSHOT.jar
   org.apache.arrow.vector.types.pojo                 -> org.slf4j                                          not found
   org.apache.arrow.vector.util                       -> com.fasterxml.jackson.core                         not found
   org.apache.arrow.vector.util                       -> com.fasterxml.jackson.databind                     not found
   org.apache.arrow.vector.util                       -> com.fasterxml.jackson.databind.annotation          not found
   org.apache.arrow.vector.util                       -> com.fasterxml.jackson.databind.cfg                 not found
   org.apache.arrow.vector.util                       -> com.fasterxml.jackson.databind.json                not found
   org.apache.arrow.vector.util                       -> com.fasterxml.jackson.databind.ser.std             not found
   org.apache.arrow.vector.util                       -> com.fasterxml.jackson.datatype.jsr310              not found
   org.apache.arrow.vector.util                       -> io.netty.util.collection                           not found
   org.apache.arrow.vector.util                       -> io.netty.util.internal                             not found
   org.apache.arrow.vector.util                       -> java.io                                            java.base
   org.apache.arrow.vector.util                       -> java.lang                                          java.base
   org.apache.arrow.vector.util                       -> java.lang.invoke                                   java.base
   org.apache.arrow.vector.util                       -> java.math                                          java.base
   org.apache.arrow.vector.util                       -> java.nio                                           java.base
   org.apache.arrow.vector.util                       -> java.nio.channels                                  java.base
   org.apache.arrow.vector.util                       -> java.nio.charset                                   java.base
   org.apache.arrow.vector.util                       -> java.text                                          java.base
   org.apache.arrow.vector.util                       -> java.time                                          java.base
   org.apache.arrow.vector.util                       -> java.time.format                                   java.base
   org.apache.arrow.vector.util                       -> java.time.temporal                                 java.base
   org.apache.arrow.vector.util                       -> java.util                                          java.base
   org.apache.arrow.vector.util                       -> java.util.concurrent                               java.base
   org.apache.arrow.vector.util                       -> java.util.function                                 java.base
   org.apache.arrow.vector.util                       -> java.util.stream                                   java.base
   org.apache.arrow.vector.util                       -> org.apache.arrow.flatbuf                           not found
   org.apache.arrow.vector.util                       -> org.apache.arrow.memory                            not found
   org.apache.arrow.vector.util                       -> org.apache.arrow.memory.util                       not found
   org.apache.arrow.vector.util                       -> org.apache.arrow.memory.util.hash                  not found
   org.apache.arrow.vector.util                       -> org.apache.arrow.util                              not found
   org.apache.arrow.vector.util                       -> org.apache.arrow.vector                            arrow-vector-8.0.0-SNAPSHOT.jar
   org.apache.arrow.vector.util                       -> org.apache.arrow.vector.compare                    arrow-vector-8.0.0-SNAPSHOT.jar
   org.apache.arrow.vector.util                       -> org.apache.arrow.vector.complex                    arrow-vector-8.0.0-SNAPSHOT.jar
   org.apache.arrow.vector.util                       -> org.apache.arrow.vector.dictionary                 arrow-vector-8.0.0-SNAPSHOT.jar
   org.apache.arrow.vector.util                       -> org.apache.arrow.vector.ipc                        arrow-vector-8.0.0-SNAPSHOT.jar
   org.apache.arrow.vector.util                       -> org.apache.arrow.vector.ipc.message                arrow-vector-8.0.0-SNAPSHOT.jar
   org.apache.arrow.vector.util                       -> org.apache.arrow.vector.types                      arrow-vector-8.0.0-SNAPSHOT.jar
   org.apache.arrow.vector.util                       -> org.apache.arrow.vector.types.pojo                 arrow-vector-8.0.0-SNAPSHOT.jar
   org.apache.arrow.vector.util                       -> org.apache.arrow.vector.validate                   arrow-vector-8.0.0-SNAPSHOT.jar
   org.apache.arrow.vector.util                       -> org.slf4j                                          not found
   org.apache.arrow.vector.validate                   -> java.io                                            java.base
   org.apache.arrow.vector.validate                   -> java.lang                                          java.base
   org.apache.arrow.vector.validate                   -> java.util                                          java.base
   org.apache.arrow.vector.validate                   -> org.apache.arrow.memory                            not found
   org.apache.arrow.vector.validate                   -> org.apache.arrow.vector                            arrow-vector-8.0.0-SNAPSHOT.jar
   org.apache.arrow.vector.validate                   -> org.apache.arrow.vector.compare                    arrow-vector-8.0.0-SNAPSHOT.jar
   org.apache.arrow.vector.validate                   -> org.apache.arrow.vector.complex                    arrow-vector-8.0.0-SNAPSHOT.jar
   org.apache.arrow.vector.validate                   -> org.apache.arrow.vector.types                      arrow-vector-8.0.0-SNAPSHOT.jar
   org.apache.arrow.vector.validate                   -> org.apache.arrow.vector.types.pojo                 arrow-vector-8.0.0-SNAPSHOT.jar

# Validate new module created
$ jar --describe-module --file target/arrow-vector-8.0.0-SNAPSHOT.jar 
arrow.vector@8.0.0-SNAPSHOT jar:file:///Users/dsusanibar/voltron/jiraarrow/fork/arrow/java/vector/target/arrow-vector-8.0.0-SNAPSHOT.jar/!module-info.class
requires arrow.memory.core
requires com.fasterxml.jackson.databind
requires com.fasterxml.jackson.datatype.jsr310
requires commons.codec
requires io.netty.common
requires java.base mandated
requires java.sql
requires org.apache.arrow.flatbuf
requires org.slf4j

#######################
# Review Arrow Flight #
#######################

# 1.- Review Arrow Flight
$ cd arrow/java/flight/flight-core

# Review Arrow Java Vector
$ jar --describe-module --file target/flight-core-8.0.0-SNAPSHOT.jar
No module descriptor found. Derived automatic module.

flight.core@8.0.0-SNAPSHOT automatic
requires java.base mandated
contains org.apache.arrow.flight
contains org.apache.arrow.flight.auth
contains org.apache.arrow.flight.auth2
contains org.apache.arrow.flight.client
contains org.apache.arrow.flight.grpc
contains org.apache.arrow.flight.impl
contains org.apache.arrow.flight.perf.impl
contains org.apache.arrow.flight.sql.impl

# Validate new module created
# error: module flight.core reads package io.grpc from both grpc.api and grpc.context

```

[1]: https://logback.qos.ch/manual/configuration.html
[2]: https://github.com/apache/arrow/blob/master/cpp/README.md
[3]: http://google.github.io/styleguide/javaguide.html
[4]: https://maven.apache.org/surefire/maven-failsafe-plugin/
