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

=========================
ADBC Go API Specification
=========================

In Go, ADBC consists of a set of interface definitions in the package
``github.com/apache/arrow-adbc/go/adbc``.

Broadly, the interfaces are organized similarly to the C API
specification, and bindings to the C API can be created easily.

See apache/arrow-adbc commit f044edf5256abfb4c091b0ad2acc73afea2c93c0_
for the definitions.

.. _f044edf5256abfb4c091b0ad2acc73afea2c93c0: https://github.com/apache/arrow-adbc/commit/f044edf5256abfb4c091b0ad2acc73afea2c93c0
