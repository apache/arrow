<!---
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->

## Python library for Apache Arrow

This library provides a Pythonic API wrapper for the reference Arrow C++
implementation, along with tools for interoperability with pandas, NumPy, and
other traditional Python scientific computing packages.

### Development details

This project is layered in two pieces:

* arrow_python, a library part of the main Arrow C++ project for Python,
  pandas, and NumPy interoperability
* Cython extensions and pure Python code under pyarrow/ which expose Arrow C++
  and pyarrow to pure Python users

#### PyArrow Dependencies:

To build pyarrow, first build and install Arrow C++ with the Python component
enabled using `-DARROW_PYTHON=on`, see
(https://github.com/apache/arrow/blob/master/cpp/README.md) . These components
must be installed either in the default system location (e.g. `/usr/local`) or
in a custom `$ARROW_HOME` location.

```shell
mkdir cpp/build
pushd cpp/build
cmake -DARROW_PYTHON=on -DCMAKE_INSTALL_PREFIX=$ARROW_HOME ..
make -j4
make install
```

If you build with a custom `CMAKE_INSTALL_PREFIX`, during development, you must
set `ARROW_HOME` as an environment variable and add it to your
`LD_LIBRARY_PATH` on Linux and OS X:

```bash
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$ARROW_HOME/lib
```

5. **Python dependencies: numpy, pandas, cython, pytest**

#### Build pyarrow and run the unit tests

```bash
python setup.py build_ext --inplace
py.test pyarrow
```

To change the build type, use the `--build-type` option or set
`$PYARROW_BUILD_TYPE`:

```bash
python setup.py build_ext --build-type=release --inplace
```

To pass through other build options to CMake, set the environment variable
`$PYARROW_CMAKE_OPTIONS`.

#### Build the pyarrow Parquet file extension

To build the integration with [parquet-cpp][1], pass `--with-parquet` to
the `build_ext` option in setup.py:

```
python setup.py build_ext --with-parquet install
```

Alternately, add `-DPYARROW_BUILD_PARQUET=on` to the general CMake options.

```
export PYARROW_CMAKE_OPTIONS=-DPYARROW_BUILD_PARQUET=on
```

#### Build the documentation

```bash
pip install -r doc/requirements.txt
python setup.py build_sphinx -s doc/source
```

[1]: https://github.com/apache/parquet-cpp