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

# pyarrow_gandiva

This library provides a Python API for functionality provided by the Arrow
Gandiva C++ libraries. Gandiva is a toolset for compiling and evaluating
expressions that project and filter arrow record batches. It uses LLVM for doing
just-in-time compilation of the expressions.

## Installation

pyarrow_gandiva is meant to be installed with pyarrow. Across platforms, you can
install a recent version of pyarrow_gandiva with the conda package manager:

```shell
conda install pyarrow_gandiva -c conda-forge
```

pyarrow will be installed as a dependency, if it is not already installed.

On Linux, macOS, and Windows, you can also install binary wheels from PyPI with
pip:

```shell
pip install pyarrow[gandiva]
```


## Development

To build the Cython components, use the `--target=pyarrow_gandiva` with
setup.py:

```shell
python setup.py build_ext --inplace --target=pyarrow_gandiva
```

To run tests, use pytest with `python -m`. If you run it directly, the main
`pyarrow` won't be on the import path and you will get import errors.

```shell
python -m pytest pyarrow_gandiva
```
