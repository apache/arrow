.. Licensed to the Apache Software Foundation (ASF) under one
.. or more contributor license agreements.  See the NOTICE file
.. distributed with this work for additional information
.. regarding copyright ownership.  The ASF licenses this file
.. to you under the Apache License, Version 2.0 (the
.. "License"); you may not use this file except in compliance
.. with the License.  You may obtain a copy of the License at

..   http://www.apache.org/licenses/LICENSE-2.0

.. Unless required by applicable law or agreed to in writing,
.. software distributed under the License is distributed on an
.. "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
.. KIND, either express or implied.  See the License for the
.. specific language governing permissions and limitations
.. under the License.

Install PyArrow
===============

Conda
-----

To install the latest version of PyArrow from conda-forge using conda:

.. code-block:: bash

    conda install -c conda-forge pyarrow

Pip
---

Install the latest version from PyPI:

.. code-block:: bash

    pip install pyarrow

.. note::
    Currently there are only binary artifcats available for Linux and MacOS.
    Otherwise this will only pull the python sources and assumes an existing
    installation of the C++ part of Arrow.
    To retrieve the binary artifacts, you'll need a recent ``pip`` version that
    supports features like the ``manylinux1`` tag.

Building from source
--------------------

First, clone the master git repository:

.. code-block:: bash

    git clone https://github.com/apache/arrow.git arrow

System requirements
~~~~~~~~~~~~~~~~~~~

Building pyarrow requires:

* A C++11 compiler

  * Linux: gcc >= 4.8 or clang >= 3.5
  * OS X: XCode 6.4 or higher preferred

* `CMake <https://cmake.org/>`_

Python requirements
~~~~~~~~~~~~~~~~~~~

You will need Python (CPython) 2.7, 3.4, or 3.5 installed. Earlier releases and
are not being targeted.

.. note::
    This library targets CPython only due to an emphasis on interoperability with
    pandas and NumPy, which are only available for CPython.

The build requires NumPy, Cython, and a few other Python dependencies:

.. code-block:: bash

    pip install cython
    cd arrow/python
    pip install -r requirements.txt

Installing Arrow C++ library
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

First, you should choose an installation location for Arrow C++. In the future
using the default system install location will work, but for now we are being
explicit:

.. code-block:: bash
    
    export ARROW_HOME=$HOME/local

Now, we build Arrow:

.. code-block:: bash

    cd arrow/cpp
    
    mkdir dev-build
    cd dev-build
    
    cmake -DCMAKE_INSTALL_PREFIX=$ARROW_HOME ..
    
    make
    
    # Use sudo here if $ARROW_HOME requires it
    make install

To get the optional Parquet support, you should also build and install 
`parquet-cpp <https://github.com/apache/parquet-cpp/blob/master/README.md>`_.

Install `pyarrow`
~~~~~~~~~~~~~~~~~


.. code-block:: bash

    cd arrow/python

    # --with-parquet enables the Apache Parquet support in PyArrow
    # --with-jemalloc enables the jemalloc allocator support in PyArrow
    # --build-type=release disables debugging information and turns on
    #       compiler optimizations for native code
    python setup.py build_ext --with-parquet --with-jemalloc --build-type=release install
    python setup.py install

.. warning::
    On XCode 6 and prior there are some known OS X `@rpath` issues. If you are
    unable to import pyarrow, upgrading XCode may be the solution.

.. note::
    In development installations, you will also need to set a correct
    ``LD_LIBRARY_PATH``. This is most probably done with
    ``export LD_LIBRARY_PATH=$ARROW_HOME/lib:$LD_LIBRARY_PATH``.


.. code-block:: python
    
    In [1]: import pyarrow

    In [2]: pyarrow.from_pylist([1,2,3])
    Out[2]:
    <pyarrow.array.Int64Array object at 0x7f899f3e60e8>
    [
      1,
      2,
      3
    ]

