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

=================================
ADBC: Arrow Database Connectivity
=================================

Full Documentation on ADBC can be found at https://arrow.apache.org/adbc/.

ADBC is:

- A set of abstract APIs in different languages (C/C++, Go, and Java, with
  more on the way) for working with databases and Arrow data.

  For example, result sets of queries in ADBC are all returned as streams of
  Arrow data, not row-by-row.
- A set of implementations of that API in different languages (C/C++, C#/.NET,
  Go, Java, Python, and Ruby) that target different databases
  (e.g. PostgreSQL, SQLite, any database supporting Flight SQL).

See the :external+adbc:doc:`ADBC Specification <format/specification>` for
details.

The ADBC specification is currently at version 1.1.0.

Updating this specification
===========================

ADBC is versioned separately from the core Arrow project.  The API
standard and components (driver manager, drivers) are also versioned
separately, but both follow semantic versioning.

For example: components may make backwards-compatible releases as
1.0.0, 1.0.1, 1.1.0, 1.2.0, etc.  They may release
backwards-incompatible versions such as 2.0.0, but which still
implement the API standard version 1.0.0.

Similarly, this documentation describes the ADBC API standard version
1.1.0.  If/when an ABI-compatible revision is made
(e.g. new standard options are defined), the next version would be
1.2.0.  If incompatible changes are made (e.g. new API functions), the
next version would be 2.0.0.
