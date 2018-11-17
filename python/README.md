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

## Python library for Apache Arrow

This library provides a Python API for functionality provided by the Arrow C++
libraries, along with tools for Arrow integration and interoperability with
pandas, NumPy, and other software in the Python ecosystem.

## Installing

Across platforms, you can install a recent version of pyarrow with the conda
package manager:

```shell
conda install pyarrow -c conda-forge
```

On Linux/macOS and Windows, you can also install binary wheels from PyPI with pip:

```shell
pip install pyarrow
```

## Development

### Coding Style

We follow a similar PEP8-like coding style to the [pandas project][3].

The code must pass `flake8` (available from pip or conda) or it will fail the
build. Check for style errors before submitting your pull request with:

```
flake8 .
flake8 --config=.flake8.cython .
```

### Building from Source

See the [Development][2] page in the documentation.

### Running the unit tests

We are using [pytest][4] to develop our unit test suite. After building the
project using `setup.py build_ext --inplace`, you can run its unit tests like
so:

```bash
pytest pyarrow
```

The project has a number of custom command line options for its test
suite. Some tests are disabled by default, for example. To see all the options,
run

```bash
pytest pyarrow --help
```

and look for the "custom options" section.

### Building the documentation

```bash
pip install -r doc/requirements.txt
python setup.py build_sphinx -s doc/source
```

[2]: https://github.com/apache/arrow/blob/master/python/doc/source/development.rst
[3]: https://github.com/pandas-dev/pandas
[4]: https://docs.pytest.org/en/latest/