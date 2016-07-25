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
4. **Parquet-cpp**

  The preferred way to install parquet-cpp is to use conda.
  You need to set the ``PARQUET_HOME`` environment variable to where parquet-cpp is installed.
  ```bash
  conda install -y --channel apache/channel/dev parquet-cpp
  ```
5. **Arrow-cpp and its dependencies***

   The Arrow C++ library must be built with all options enabled and installed with ``ARROW_HOME`` environment variable set to
   the installation location. Look at (https://github.com/apache/arrow/blob/master/cpp/README.md) for
   instructions. Alternatively you could just install arrow-cpp
   from conda.
  ```bash
  conda install arrow-cpp -c apache/channel/dev
  ```
6. **Python dependencies: numpy, pandas, cython, pytest**

#### Install pyarrow
  ```bash
  python setup.py build_ext --inplace
  ```
