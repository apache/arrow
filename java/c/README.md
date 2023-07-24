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

# C Interfaces for Arrow Java

## Setup Build Environment

install:
 - Java 8 or later
 - Maven 3.3 or later
 - A C++17-enabled compiler
 - CMake 3.11 or later
 - Make or ninja build utilities

## Building JNI wrapper shared library

```
mkdir -p build
pushd build
cmake ..
cmake --build .
popd
```

## Building and running tests

Run tests with

```
mvn test
```

To install Apache Arrow (Java) with this module enabled run the following from the project root directory:

```
cd java
mvn -Parrow-c-data install
```
