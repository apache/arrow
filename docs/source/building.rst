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

.. _building-docs:

Building the Documentation
==========================

Prerequisites
-------------

The documentation build process uses `Doxygen <http://www.doxygen.nl/>`_ and
`Sphinx <http://www.sphinx-doc.org/>`_ along with a few extensions.

If you're using Conda, the required software can be installed in a single line:

.. code-block:: shell

   conda install -c conda-forge --file=ci/conda_env_sphinx.yml

Otherwise, you'll first need to install `Doxygen <http://www.doxygen.nl/>`_
yourself (for example from your distribution's official repositories, if
using Linux).  Then you can install the Python-based requirements with the
following command:

.. code-block:: shell

   pip install -r docs/requirements.txt

Building
--------

.. note::

   If you are building the documentation on Windows, not all sections
   may build properly.

These two steps are mandatory and must be executed in order.

#. Process the C++ API using Doxygen

   .. code-block:: shell

      pushd cpp/apidoc
      doxygen
      popd

#. Build the complete documentation using Sphinx

   .. code-block:: shell

      pushd docs
      make html
      popd

After these steps are completed, the documentation is rendered in HTML
format in ``docs/_build/html``.  In particular, you can point your browser
at ``docs/_build/html/index.html`` to read the docs and review any changes
you made.


.. _building-docker:

Building with Docker
--------------------

You can use Docker to build the documentation:

.. code-block:: shell

  docker-compose build cpp
  docker-compose build python
  docker-compose build docs
  docker-compose run docs

The final output is located under ``docs/_build/html``.
