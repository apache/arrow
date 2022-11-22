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

PyArrow is currently compatible with Python 3.7, 3.8, 3.9, 3.10 and 3.11.

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

Dependencies
------------

Required dependency

* **NumPy 1.16.6** or higher.

Optional dependencies

* **pandas 1.0** or higher,
* **cffi**.

Additional packages PyArrow is compatible with are :ref:`fsspec <filesystem-fsspec>`
and **pytz**, **dateutil** or **tzdata** package for timezones.
