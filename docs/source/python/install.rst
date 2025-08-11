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

PyArrow is regularly built and tested on Windows, macOS and various
Linux distributions. We strongly recommend using a 64-bit system.

Python Compatibility
--------------------

PyArrow is currently compatible with Python 3.9, 3.10, 3.11, 3.12 and 3.13.

Using Conda
-----------

Install the latest version of PyArrow from
`conda-forge <https://conda-forge.org/>`_ using `Conda <https://conda.io>`_:

.. code-block:: bash

    conda install -c conda-forge pyarrow

.. note::

    While the ``pyarrow`` `conda-forge <https://conda-forge.org/>`_ package is
    the right choice for most users, both a minimal and maximal variant of the
    package exist, either of which may be better for your use case. See
    :ref:`python-conda-differences`.

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

Installing nightly packages or from source
------------------------------------------

See :ref:`python-development`.

Dependencies
------------

Optional dependencies

* **NumPy 1.16.6** or higher.
* **pandas 1.0** or higher,
* **cffi**.

Additional packages PyArrow is compatible with are :ref:`fsspec <filesystem-fsspec>`
and **pytz**, **dateutil** or **tzdata** package for timezones.

tzdata on Windows
^^^^^^^^^^^^^^^^^

While Arrow uses the OS-provided timezone database on Linux and macOS, it requires a
user-provided database on Windows. To download and extract the text version of
the IANA timezone database follow the instructions in the C++
:ref:`download-timezone-database` or use pyarrow utility function
``pyarrow.util.download_tzdata_on_windows()`` that does the same.

By default, the timezone database will be detected at ``%USERPROFILE%\Downloads\tzdata``.
If the database has been downloaded in a different location, you will need to set
a custom path to the database from Python:

.. code-block:: python

   >>> import pyarrow as pa
   >>> pa.set_timezone_db_path("custom_path")

You may encounter problems writing datetime data to an ORC file if you install
pyarrow with pip. One possible solution to fix this problem:

   1. Install tzdata with ``pip install tzdata``
   2. Set the environment variable ``TZDIR = path\to\.venv\Lib\site-packages\tzdata\``

You can find where ``tzdata`` is installed with the following python
command:

.. code-block:: python

   >>> import tzdata
   >>> print(tzdata.__file__)
   path\to\.venv\Lib\site-packages\tzdata\__init__.py


.. _python-conda-differences:

Differences between conda-forge packages
----------------------------------------

On `conda-forge <https://conda-forge.org/>`_, PyArrow is published as three
separate packages, each providing varying levels of functionality. This is in
contrast to PyPi, where only a single PyArrow package is provided.

The purpose of this split is to minimize the size of the installed package for
most users (``pyarrow``), provide a smaller, minimal package for specialized use
cases (``pyarrow-core``), while still providing a complete package for users who
require it (``pyarrow-all``). What was historically ``pyarrow`` on
`conda-forge <https://conda-forge.org/>`_ is now ``pyarrow-all``, though most
users can continue using ``pyarrow``.

The ``pyarrow-core`` package includes the following functionality:

- :ref:`data`
- :ref:`compute` (i.e., ``pyarrow.compute``)
- :ref:`io`
- :ref:`ipc` (i.e., ``pyarrow.ipc``)
- :ref:`filesystem` (i.e., ``pyarrow.fs``. Note: It's planned to move cloud fileystems (i.e., :ref:`S3<filesystem-s3>`, :ref:`GCS<filesystem-gcs>`, etc) into ``pyarrow`` in a future release though :ref:`filesystem-localfs` will remain in ``pyarrow-core``.)
- File formats: :ref:`Arrow/Feather<feather>`, :ref:`JSON<json>`, :ref:`CSV<py-csv>`, :ref:`ORC<orc>` (but not Parquet)

The ``pyarrow`` package adds the following:

- Acero (i.e., ``pyarrow.acero``)
- :ref:`dataset` (i.e., ``pyarrow.dataset``)
- :ref:`Parquet<parquet>` (i.e., ``pyarrow.parquet``)
- Substrait (i.e., ``pyarrow.substrait``)

Finally, ``pyarrow-all`` adds:

- :ref:`flight` and Flight SQL (i.e., ``pyarrow.flight``)
- Gandiva  (i.e., ``pyarrow.gandiva``)

The following table lists the functionality provided by each package and may be
useful when deciding to use one package over another or when
:ref:`python-conda-custom-selection`.

+------------+---------------------+--------------+---------+-------------+
| Component  | Package             | pyarrow-core | pyarrow | pyarrow-all |
+------------+---------------------+--------------+---------+-------------+
| Core       | pyarrow-core        | ✓            | ✓       | ✓           |
+------------+---------------------+--------------+---------+-------------+
| Parquet    | libparquet          |              | ✓       | ✓           |
+------------+---------------------+--------------+---------+-------------+
| Dataset    | libarrow-dataset    |              | ✓       | ✓           |
+------------+---------------------+--------------+---------+-------------+
| Acero      | libarrow-acero      |              | ✓       | ✓           |
+------------+---------------------+--------------+---------+-------------+
| Substrait  | libarrow-substrait  |              | ✓       | ✓           |
+------------+---------------------+--------------+---------+-------------+
| Flight     | libarrow-flight     |              |         | ✓           |
+------------+---------------------+--------------+---------+-------------+
| Flight SQL | libarrow-flight-sql |              |         | ✓           |
+------------+---------------------+--------------+---------+-------------+
| Gandiva    | libarrow-gandiva    |              |         | ✓           |
+------------+---------------------+--------------+---------+-------------+

.. _python-conda-custom-selection:

Creating A Custom Selection
^^^^^^^^^^^^^^^^^^^^^^^^^^^

If you know which components you need and want to control what's installed, you
can create a custom selection of packages to include only the extra features you
need. For example, to install ``pyarrow-core`` and add support for reading and
writing Parquet, install ``libparquet`` alongside ``pyarrow-core``:

.. code-block:: shell

    conda install -c conda-forge pyarrow-core libparquet

Or if you wish to use ``pyarrow`` but need support for Flight RPC:

.. code-block:: shell

    conda install -c conda-forge pyarrow libarrow-flight
