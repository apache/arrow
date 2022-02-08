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

### Install MATLAB Interface to Arrow using CMake
After building the MATLAB Interface to Arrow, install the library by running the following command:
``` CMake
cmake --build build --config Release --target install
```
The installation process includes 

Once installed, the interface's source files and libraries are relocatable, on all platforms. If the interface is relocated, then the user must manually add the new location to the [MATLAB Search Path](https://uk.mathworks.com/help/matlab/matlab_env/what-is-the-matlab-search-path.html).

The following flags can be passed to the CMake generation command to configure the installation step:
| CMake Flag                               | Default Value       | Values accepted     | Description   |
| ---------------------------------------- | ------------------- | ---------------------------------------------- | ------------- |
| `CMAKE_INSTALL_PREFIX`                   | platform dependent  | A path to any directory with write permissions | The location that the MATLAB Interface to Arrow will be installed.
| `MATLAB_ADD_INSTALL_DIR_TO_SEARCH_PATH`  | `ON`        | `ON` or `OFF` | Whether the path to the install directory should be added directly added to the MATLAB Search Path.
| `MATLAB_ADD_INSTALL_DIR_TO_STARTUP_FILE` | `ON`        | `ON` or `OFF` | Whether a command to add the path to the install directory should be added to the `startup.m` file located at the MATLAB `userpath`.

###### `CMAKE_INSTALL_PREFIX`   
The install command will install the interface to the location pointed to by `CMAKE_INSTALL_PREFIX`. The default value for this CMake variable is platform dependent. Default values for the location on different platforms can be found here: [`CMAKE_INSTALL_PREFIX`](https://cmake.org/cmake/help/v3.0/variable/CMAKE_INSTALL_PREFIX.html). 

###### `MATLAB_ADD_INSTALL_DIR_TO_SEARCH_PATH`   
Call `addpath` and `savepath` to modify the default `pathdef.m` file that MATLAB uses on startup. This option is on by default. However, it can only be used if CMake has the appropriate permissions to modify `pathdef.m`.

###### `MATLAB_ADD_INSTALL_DIR_TO_STARTUP_FILE`   
Add an `addpath` command to the `startup.m` file located at the [`userpath`](https://uk.mathworks.com/help/matlab/matlab_env/what-is-the-matlab-search-path.html#:~:text=on%20Search%20Path.-,userpath%20Folder%20on%20the%20Search%20Path,-The%20userpath%20folder). This option can be used if a user does not have the permissions to modify the default `pathdef.m` file. 

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
