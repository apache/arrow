## Python library for Apache Arrow

This library provides a Pythonic API wrapper for the reference Arrow C++
implementation, along with tools for interoperability with pandas, NumPy, and
other traditional Python scientific computing packages.

#### Development details

This project is layered in two pieces:

* pyarrow, a C++ library for easier interoperability between Arrow C++, NumPy,
  and pandas
* Cython extensions and pure Python code under arrow/ which expose Arrow C++
  and pyarrow to pure Python users