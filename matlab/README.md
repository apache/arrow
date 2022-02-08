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

## Build

## Install

After building the MATLAB Interface to Arrow, install the library by running the following command:

``` CMake
cmake --build build --config Release --target install
```

The installation process includes 

Once installed, the interface's source files and libraries are relocatable, on all platforms. If the interface is relocated, then the user must manually add the new location to the [MATLAB Search Path](https://uk.mathworks.com/help/matlab/matlab_env/what-is-the-matlab-search-path.html).

## Test

### C++

### MATLAB

``` matlab
>> runtests test;
```

## Usage

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

## Troubleshooting

### Non-standard MATLAB and Arrow installations

To specify a non-standard MATLAB install location, use the Matlab_ROOT_DIR CMake flag:

    cmake .. -DMatlab_ROOT_DIR=/<PATH_TO_MATLAB_INSTALL>

To specify a non-standard Arrow install location, use the ARROW_HOME CMake flag:

    cmake .. -DARROW_HOME=/<PATH_TO_ARROW_INSTALL>


