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

# MATLAB Interface to Apache Arrow

## Status

This is a very early stage MATLAB interface to the Apache Arrow C++ libraries.

The current code only supports reading/writing numeric types from/to Feather v1 files.

## Prerequisites

## Setup
To set up a local working copy of the source code, start by cloning the repository:
```bash
$ git clone https://github.com/apache/arrow.git
```

Change the working directory to the `matlab` subdirectory:
```bash
$ cd arrow/matlab
```

## Build
To build the project:
```bash
$ cmake -S . -B build 
$ cmake --build build --config Release
```

## Install
To install the MATLAB interface to the default software location (e.g. `/usr/local` or `C:\Program Files`):
```bash
$ cmake --build build --config Release --target install
```

As part of the install step, the install directory is added to the [MATLAB Search Path](https://mathworks.com/help/matlab/matlab_env/what-is-the-matlab-search-path.html).

Note: this step may fail if the current user is lacking the necessary filesystem permissions. Please see Troubleshooting for more information.

## Test
There are two kinds of tests for the MATLAB Interface to Arrow: C++ and MATLAB. 

### C++
To enable the C++ tests, the `MATLAB_BUILD_TESTS` flag must enabled at build time: 
```bash
$ cmake -S . -B build -D MATLAB_BUILD_TESTS=ON
$ cmake --build build --config Release
```

After building with the `MATLAB_BUILD_TESTS` flag enabled, the C++ tests can be run using [`ctest`](https://cmake.org/cmake/help/v3.22/manual/ctest.1.html):
```bash
$ ctest --test-dir build
```

### MATLAB
To run the MATLAB tests, start MATLAB in the `arrow/matlab` directory and call the [`runtests`](https://mathworks.com/help/matlab/ref/runtests.html) command on the `test` directory:
``` matlab
>> runtests test;
```

## Usage
Included below are some example code snippets that illustrate how to use the MATLAB Interface To Arrow.

### Write a MATLAB table to a Feather v1 file

``` matlab
>> t = array2table(rand(10, 10));
>> filename = 'table.feather';
>> featherwrite(filename,t);
```

### Read a Feather v1 file into a MATLAB table

``` matlab
>> filename = 'table.feather';
>> t = featherread(filename);
```

