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

.. _format_canonical_extensions:

*************************
Canonical Extension Types
*************************

============
Introduction
============

The Arrow Columnar Format allows defining
:ref:`extension types <format_metadata_extension_types>` so as to extend
standard Arrow data types with custom semantics.  Often these semantics
will be specific to a system or application.  However, it is beneficial
to share the definitions of well-known extension types so as to improve
interoperability between different systems integrating Arrow columnar data.

Standardization
===============

These rules must be followed for the standardization of canonical extension
types:

* Canonical extension types are described and maintained below in this document.

* Each canonical extension type requires a distinct discussion and vote
  on the `Arrow development mailing-list <https://arrow.apache.org/community/>`__.

* The specification text to be added *must* follow these requirements:

  1) It *must* define a well-defined extension name starting with "``arrow.``".

  2) Its parameters, if any, *must* be described in the proposal.

  3) Its serialization *must* be described in the proposal and should
     not require unduly implementation work or unusual software dependencies
     (for example, a trivial custom text format or JSON would be acceptable).

  4) Its expected semantics *should* be described as well and any
     potential ambiguities or pain points addressed or at least mentioned.

* The extension type *should* have one implementation submitted;
  preferably two if non-trivial (for example if parameterized).

Making Modifications
====================

Like standard Arrow data types, canonical extension types should be considered
stable once standardized.  Modifying a canonical extension type (for example
to expand the set of parameters) should be an exceptional event, follow the
same rules as laid out above, and provide backwards compatibility guarantees.


=============
Official List
=============

No canonical extension types have been standardized yet.
