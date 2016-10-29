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

* pyarrow, a C++ library for easier interoperability between Arrow C++, NumPy,
  and pandas
* Cython extensions and pure Python code under arrow/ which expose Arrow C++
  and pyarrow to pure Python users

#### PyArrow Dependencies:
These are the various projects that PyArrow depends on.

1. **g++ and gcc Version >= 4.8**
2. **cmake > 2.8.6**
3. **boost**
4. **Arrow-cpp and its dependencies***

The Arrow C++ library must be built with all options enabled and installed with
``ARROW_HOME`` environment variable set to the installation location. Look at
(https://github.com/apache/arrow/blob/master/cpp/README.md) for instructions.

5. **Python dependencies: numpy, pandas, cython, pytest**

#### Build pyarrow and run the unit tests

```bash
python setup.py build_ext --inplace
py.test pyarrow
```

To change the build type, use the `--build-type` option:

```bash
python setup.py build_ext --build-type=release --inplace
```

To pass through other build options to CMake, set the environment variable
`$PYARROW_CMAKE_OPTIONS`.

#### Build the documentation

```bash
pip install -r doc/requirements.txt
python setup.py build_sphinx
```
