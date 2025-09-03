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

[![pypi](https://img.shields.io/pypi/v/pyarrow.svg)](https://pypi.org/project/pyarrow/) [![conda-forge](https://img.shields.io/conda/vn/conda-forge/pyarrow.svg)](https://anaconda.org/conda-forge/pyarrow)

This library provides a Python API for functionality provided by the Arrow C++
libraries, along with tools for Arrow integration and interoperability with
pandas, NumPy, and other software in the Python ecosystem.

## Installing

Across platforms, you can install a recent version of pyarrow with the conda
package manager:

```shell
conda install pyarrow -c conda-forge
```

On Linux, macOS, and Windows, you can also install binary wheels from PyPI with
pip:

```shell
pip install pyarrow
```

If you encounter any issues importing the pip wheels on Windows, you may need
to install the latest [Visual C++ Redistributable for Visual Studio][6].

## Development

See [Python Development][2] in the documentation subproject.

### Building the documentation

See [documentation build instructions][1] in the documentation subproject.

[1]: https://github.com/apache/arrow/blob/main/docs/source/developers/documentation.rst
[2]: https://github.com/apache/arrow/blob/main/docs/source/developers/python.rst
[3]: https://github.com/pandas-dev/pandas
[5]: https://arrow.apache.org/docs/latest/python/benchmarks.html
[6]: https://learn.microsoft.com/en-us/cpp/windows/latest-supported-vc-redist?view=msvc-170#latest-microsoft-visual-c-redistributable-version
