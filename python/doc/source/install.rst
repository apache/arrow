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

    Currently there are only binary artifacts available for Linux and MacOS.
    Otherwise this will only pull the python sources and assumes an existing
    installation of the C++ part of Arrow.  To retrieve the binary artifacts,
    you'll need a recent ``pip`` version that supports features like the
    ``manylinux1`` tag.

Installing from source
----------------------

See :ref:`development`.
