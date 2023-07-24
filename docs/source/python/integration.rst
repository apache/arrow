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

.. _integration:

********************
PyArrow Integrations
********************

Arrow is designed to be both a framework and an interchange format.

Developers can use Arrow to exchange data between various
technologies and languages without incurring in any extra cost of
marshalling/unmarshalling the data. The Arrow bindings and Arrow
native libraries on the various platforms will all understand Arrow data
natively wihout the need to decode it.

This allows to easily integrate PyArrow with other languages and technologies.

.. toctree::
   :maxdepth: 2

   integration/python_r
   integration/python_java
   integration/extending
   integration/cuda
