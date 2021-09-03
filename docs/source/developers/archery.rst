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

.. _archery:

Daily Development using Archery
===============================

To ease some of the daily development tasks, we developed a Python-written
utility called Archery.

Installation
------------

Archery requires Python 3.6 or later. It is recommended to install archery in
*editable* mode with the ``-e`` flag to automatically update the installation
when pulling the Arrow repository.

.. code:: bash

   pip install -e dev/archery

Usage
-----

You can inspect Archery usage by passing the ``--help`` flag:

.. code:: bash

   $ archery --help
   Usage: archery [OPTIONS] COMMAND [ARGS]...

     Apache Arrow developer utilities.

     See sub-commands help with `archery <cmd> --help`.

   Options:
     --debug      Increase logging with debugging output.
     --pdb        Invoke pdb on uncaught exception.
     -q, --quiet  Silence executed commands.
     --help       Show this message and exit.

   Commands:
     benchmark    Arrow benchmarking.
     build        Initialize an Arrow C++ build
     crossbow     Schedule packaging tasks or nightly builds on CI services.
     docker       Interact with docker-compose based builds.
     integration  Execute protocol and Flight integration tests
     linking      Quick and dirty utilities for checking library linkage.
     lint         Check Arrow source tree for errors
     numpydoc     Lint python docstring with NumpyDoc
     release      Release releated commands.
     trigger-bot

Archery exposes independent subcommands, each of which provides dedicated
help output, for example:

.. code:: bash

   $ archery docker --help
   Usage: archery docker [OPTIONS] COMMAND [ARGS]...

     Interact with docker-compose based builds.

   Options:
     --src <arrow_src>  Specify Arrow source directory.
     --help             Show this message and exit.

   Commands:
     images  List the available docker-compose images.
     push    Push the generated docker-compose image.
     run     Execute docker-compose builds.
