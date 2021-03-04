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

## MATLAB Library for Apache Arrow

## Status

This is a very early stage MATLAB interface to the Apache Arrow C++ libraries.

The current code only supports reading/writing numeric types from/to Feather files.

## Building from source

### Get Arrow and build Arrow CPP

See: [Arrow CPP README](../cpp/README.md)

### Build MATLAB interface to Apache Arrow using MATLAB R2018a:

    cd arrow/matlab
    mkdir build
    cd build
    cmake ..
    make

#### Non-standard MATLAB and Arrow installations

To specify a non-standard MATLAB install location, use the Matlab_ROOT_DIR CMake flag:

    cmake .. -DMatlab_ROOT_DIR=/<PATH_TO_MATLAB_INSTALL>

To specify a non-standard Arrow install location, use the ARROW_HOME CMake flag:

    cmake .. -DARROW_HOME=/<PATH_TO_ARROW_INSTALL>

### Build MATLAB interface to Arrow using MATLAB R2018b or later:

This may be preferred if you are using MATLAB R2018b or later and have encountered [linker errors](https://gitlab.kitware.com/cmake/cmake/issues/18391) when using CMake.

Prerequisite: Ensure that the Arrow C++ library is already installed and the `ARROW_HOME` environment variable is set to the installation root.

To verify this, you can run:

``` matlab
>> getenv ARROW_HOME
```

This should print a path that contains `include` and `lib` directories with Arrow C++ headers and libraries.

Navigate to the `build_support` subfolder and run the `compile` function to build the necessary MEX files:

``` matlab
>> cd build_support
>> compile
```

Run the `test` function to execute the unit tests:

``` matlab
>> test
```

## Try it out

### Add the src and build directories to your MATLAB path

``` matlab
>> cd(fullfile('arrow', 'matlab'));
>> addpath src;
>> addpath build;
```

### Write a MATLAB table to a Feather file

``` matlab
>> t = array2table(rand(10, 10));
>> filename = 'table.feather';
>> featherwrite(filename,t);
```

### Read a Feather file into a MATLAB table

``` matlab
>> filename = 'table.feather';
>> t = featherread(filename);
```

## Running the tests

``` matlab
>> cd(fullfile('arrow', 'matlab'));
>> addpath src;
>> addpath build;
>> cd test;
>> runtests .;
```
