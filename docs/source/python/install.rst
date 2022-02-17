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

Installing PyArrow
==================

System Compatibility
--------------------

PyArrow is regularly built and tested on Windows, macOS and various Linux
distributions (including Ubuntu 16.04, Ubuntu 18.04).  We strongly recommend
using a 64-bit system.

Python Compatibility
--------------------

PyArrow is currently compatible with Python 3.7, 3.8, 3.9 and 3.10.

Using Conda
-----------

Install the latest version of PyArrow from
`conda-forge <https://conda-forge.org/>`_ using `Conda <https://conda.io>`_:

.. code-block:: bash

    conda install -c conda-forge pyarrow

Using Pip
---------

Install the latest version from `PyPI <https://pypi.org/>`_ (Windows, Linux,
and macOS):

.. code-block:: bash

    pip install pyarrow

If you encounter any importing issues of the pip wheels on Windows, you may
need to install the `Visual C++ Redistributable for Visual Studio 2015
<https://www.microsoft.com/en-us/download/details.aspx?id=48145>`_.

.. warning::
   On Linux, you will need pip >= 19.0 to detect the prebuilt binary packages.

Installing from source
----------------------

See :ref:`python-development`.

Installing Nightly Packages
---------------------------

.. warning::
    These packages are not official releases. Use them at your own risk.

PyArrow has nightly wheels and conda packages for testing purposes.

These may be suitable for downstream libraries in their continuous integration
setup to maintain compatibility with the upcoming PyArrow features,
deprecations and/or feature removals.

Install the development version of PyArrow from `arrow-nightlies
<https://anaconda.org/arrow-nightlies/pyarrow>`_ conda channel:

.. code-block:: bash

    conda install -c arrow-nightlies pyarrow

Install the development version from an `alternative PyPI
<https://gemfury.com/arrow-nightlies>`_ index:

.. code-block:: bash

    pip install --extra-index-url https://pypi.fury.io/arrow-nightlies/ \
        --prefer-binary --pre pyarrow
