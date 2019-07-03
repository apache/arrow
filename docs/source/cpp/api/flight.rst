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

================
Arrow Flight RPC
================

.. warning:: Flight is currently unstable. APIs are subject to change,
             though we don't expect drastic changes.

.. warning:: Flight is currently only available when built from source
             appropriately.

Common Types
============

.. doxygenstruct:: arrow::flight::Action
   :project: arrow_cpp
   :members:

.. doxygenstruct:: arrow::flight::ActionType
   :project: arrow_cpp
   :members:

.. doxygenstruct:: arrow::flight::Criteria
   :project: arrow_cpp
   :members:

.. doxygenstruct:: arrow::flight::FlightDescriptor
   :project: arrow_cpp
   :members:

.. doxygenstruct:: arrow::flight::FlightEndpoint
   :project: arrow_cpp
   :members:

.. doxygenclass:: arrow::flight::FlightInfo
   :project: arrow_cpp
   :members:

.. doxygenstruct:: arrow::flight::FlightPayload
   :project: arrow_cpp
   :members:

.. doxygenclass:: arrow::flight::FlightListing
   :project: arrow_cpp
   :members:

.. doxygenstruct:: arrow::flight::Location
   :project: arrow_cpp
   :members:

.. doxygenstruct:: arrow::flight::PutResult
   :project: arrow_cpp
   :members:

.. doxygenstruct:: arrow::flight::Result
   :project: arrow_cpp
   :members:

.. doxygenclass:: arrow::flight::ResultStream
   :project: arrow_cpp
   :members:

.. doxygenstruct:: arrow::flight::Ticket
   :project: arrow_cpp
   :members:

Clients
=======

.. doxygenclass:: arrow::flight::FlightClient
   :project: arrow_cpp
   :members:

.. doxygenclass:: arrow::flight::FlightCallOptions
   :project: arrow_cpp
   :members:

.. doxygenclass:: arrow::flight::ClientAuthHandler
   :project: arrow_cpp
   :members:

.. doxygentypedef:: arrow::flight::TimeoutDuration
   :project: arrow_cpp

Servers
=======

.. doxygenclass:: arrow::flight::FlightServerBase
   :project: arrow_cpp
   :members:

.. doxygenclass:: arrow::flight::FlightDataStream
   :project: arrow_cpp
   :members:

.. doxygenclass:: arrow::flight::FlightMessageReader
   :project: arrow_cpp
   :members:

.. doxygenclass:: arrow::flight::RecordBatchStream
   :project: arrow_cpp
   :members:

.. doxygenclass:: arrow::flight::ServerAuthHandler
   :project: arrow_cpp
   :members:

.. doxygenclass:: arrow::flight::ServerCallContext
   :project: arrow_cpp
   :members:
