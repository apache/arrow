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

.. currentmodule:: pyarrow.flight

Arrow Flight
============

.. ifconfig:: not flight_enabled

   .. error::
      This documentation was built without Flight enabled.  The Flight
      API docs are not available.

.. NOTE We still generate those API docs (with empty docstrings)
.. when Flight is disabled and `pyarrow.flight` mocked (see conf.py).
.. Otherwise we'd get autodoc warnings, see https://github.com/sphinx-doc/sphinx/issues/4770

.. warning:: Flight is currently unstable. APIs are subject to change,
             though we don't expect drastic changes.

.. warning:: Flight is currently not distributed as part of wheels or
             in Conda - it is only available when built from source
             appropriately.

Common Types
------------

.. autosummary::
   :toctree: ../generated/

    Action
    ActionType
    DescriptorType
    FlightDescriptor
    FlightEndpoint
    FlightInfo
    Location
    Ticket
    Result

Flight Client
-------------

.. autosummary::
   :toctree: ../generated/

    FlightCallOptions
    FlightClient

Flight Server
-------------

.. autosummary::
   :toctree: ../generated/

    FlightServerBase
    GeneratorStream
    RecordBatchStream

Authentication
--------------

.. autosummary::
   :toctree: ../generated/

    ClientAuthHandler
    ServerAuthHandler
