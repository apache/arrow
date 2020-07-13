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

# Minimal C++ build example

This directory showcases a minimal build of Arrow C++ (in `build_arrow.sh`).
This minimal build is then used by an example third-party C++ project
using CMake logic to compile and link against the Arrow C++ library
(in `build_example.sh` and `CMakeLists.txt`).

When run, the example executable reads a file named `test.csv`,
displays its parsed contents, and then saves them in Arrow IPC format in
a file named `test.arrow`.

## Running the example

You can run this simple example using `Docker` and the given `Dockerfile`,
which installs a minimal Ubuntu image with a basic C++ toolchain.

Just open a terminal in this directory and, assuming `$ARROW_ROOT` points to
the Arrow C++ source tree (either a stable release or a git checkout), run
the following commands:

```bash

# Build the Docker image
docker build -t arrow_cpp_minimal .

# Run the example inside the image
docker run -it -v $PWD:/io -v $ARROW_ROOT:/arrow arrow_cpp_minimal /io/run.sh
```

Note that this example mounts two volumes inside the Docker image:
* `/arrow` points to the Arrow source tree
* `/io` points to this example directory

## Statically-linked builds

We've provided an example build configuration here with CMake to show how to
create a statically-linked executable .

To run it on Linux, you can use the above Docker image:

```bash
docker run -it -v $PWD:/io -v $ARROW_ROOT:/arrow arrow_cpp_minimal /io/run_static.sh
```

On macOS, you can use the `run_static.sh` but you must set some environment
variables to point the script to your Arrow checkout, for example:

```bash
export ARROW_DIR=path/to/arrow-clone
export EXAMPLE_DIR=$ARROW_DIR/cpp/examples/minimal_build
export ARROW_BUILD_DIR=`pwd`/arrow-build
export EXAMPLE_BUILD_DIR=`pwd`/example

./run_static.sh
```

On Windows, you can run `run_static.bat` from the command prompt with Visual
Studio's command line tools enabled and CMake and ninja build in the path:

```
call run_static.bat
```