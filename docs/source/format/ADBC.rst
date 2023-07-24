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

Rationale
=========

The Arrow ecosystem lacks standard database interfaces built around
Arrow data, especially for efficiently fetching large datasets
(i.e. with minimal or no serialization and copying).  Without a common
API, the end result is a mix of custom protocols (e.g. BigQuery,
Snowflake) and adapters (e.g. Turbodbc_) scattered across languages.
Consumers must laboriously wrap individual systems (as `DBI is
contemplating`_ and `Trino does with connectors`_).

ADBC aims to provide a minimal database client API standard, based on
Arrow, for C, Go, and Java (with bindings for other languages).
Applications code to this API standard (in much the same way as they
would with JDBC or ODBC), but fetch result sets in Arrow format
(e.g. via the :doc:`C Data Interface <./CDataInterface>`).  They then
link to an implementation of the standard: either directly to a
vendor-supplied driver for a particular database, or to a driver
manager that abstracts across multiple drivers.  Drivers implement the
standard using a database-specific API, such as Flight SQL.

Goals
-----

- Provide a cross-language, Arrow-based API to standardize how clients
  submit queries to and fetch Arrow data from databases.
- Support both SQL dialects and the emergent `Substrait`_ standard.
- Support explicitly partitioned/distributed result sets to work
  better with contemporary distributed systems.
- Allow for a variety of implementations to maximize reach.

Non-goals
---------

- Replacing JDBC/ODBC in all use cases, particularly `OLTP`_ use
  cases.
- Requiring or enshrining a particular database protocol for the Arrow
  ecosystem.

Example use cases
-----------------

A C or C++ application wishes to retrieve bulk data from a Postgres
database for further analysis.  The application is compiled against
the ADBC header, and executes queries via the ADBC APIs.  The
application is linked against the ADBC libpq driver.  At runtime, the
driver submits queries to the database via the Postgres client
libraries, and retrieves row-oriented results, which it then converts
to Arrow format before returning them to the application.

If the application wishes to retrieve data from a database supporting
Flight SQL instead, it would link against the ADBC Flight SQL driver.
At runtime, the driver would submit queries via Flight SQL and get
back Arrow data, which is then passed unchanged and uncopied to the
application.  (The application may have to edit the SQL queries, as
ADBC does not translate between SQL dialects.)

If the application wishes to work with multiple databases, it would
link against the ADBC driver manager, and specify the desired driver
at runtime.  The driver manager would pass on API calls to the correct
driver, which handles the request.

ADBC API Standard 1.0.0
=======================

ADBC is a language-specific set of interface definitions that can be
implemented directly by a vendor-specific "driver" or a vendor-neutral
"driver manager".

Version 1.0.0 of the standard corresponds to tag adbc-1.0.0 of the
repository ``apache/arrow-adbc``, which is commit
f044edf5256abfb4c091b0ad2acc73afea2c93c0_.  Note that is is separate
from releases of the actual implementations.

See the language-specific pages for details:

.. toctree::
   :maxdepth: 1

   ADBC/C
   ADBC/Go
   ADBC/Java

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
1.0.0.  If/when an ABI-compatible revision is made
(e.g. new standard options are defined), the next version would be
1.1.0.  If incompatible changes are made (e.g. new API functions), the
next version would be 2.0.0.

Related work
============

In the initial proposal, a survey of existing solutions and systems
was included, which is reproduced below for context, though note the
descriptions are only kept up-to-date on a best-effort basis.

Comparison with Arrow Flight SQL
--------------------------------

Flight SQL is a **client-server protocol** oriented at database
developers.  By implementing Flight SQL, a database can support
clients that use ADBC, JDBC, and ODBC.

ADBC is an **API specification** oriented at database clients.  By
coding to ADBC, an application can get Arrow data from a variety of
databases that use different client technologies underneath.

Hence, the two projects complement each other.  While Flight SQL
provides a client that can be used directly, we expect applications
would prefer to use ADBC instead of tying themselves to a particular
database.

Comparison with JDBC/ODBC
-------------------------

JDBC is a row-based API, so bridging JDBC to Arrow is hard to do
efficiently.

ODBC provides support for bulk data with `block cursors`_, and
Turbodbc_ demonstrates that a performant Arrow-based API can be built
on top. However, it is still an awkward fit for Arrow:

- Nulls (‘indicator’ values) are `represented as integers`_, requiring
  conversion.
- `Result buffers are caller-allocated`_. This can force unnecessarily
  copying data. ADBC uses the C Data Interface instead, eliminating
  copies when possible (e.g. if the driver uses Flight SQL).
- Some data types are represented differently, and require
  conversion. `SQL_C_BINARY`_ can sidestep this for drivers and
  applications that cooperate, but then applications would have to
  treat Arrow-based and non-Arrow-based data sources differently.

  - `Strings must be null-terminated`_, which would require a copy
    into an Arrow array, or require that the application handle null
    terminated strings in an array.
  - It is implementation-defined whether strings may have embedded
    nulls, but Arrow specifies UTF-8 strings for which 0x00 is a valid
    byte.
  - Because buffers are caller-allocated, the driver and application
    must cooperate to handle large strings; `the driver must truncate
    the value`_, and the application can try to fetch the value again.
  - ODBC uses length buffers rather than offsets, requiring another
    conversion to/from Arrow string arrays.
  - `Time intervals use different representations`_.

Hence, we think just extending ODBC is insufficient to meet the goals
of ADBC. ODBC will always be valuable for wider database support, and
providing an Arrow-based API on top of ODBC is useful. ADBC would
allow implementing/optimizing this conversion in a common library,
provide a simpler interface for consumers, and would provide an API
that Arrow-native or otherwise columnar systems can implement to
bypass this wrapper.

.. figure:: ./ADBCQuadrants.svg

   ADBC, JDBC, and ODBC are database-agnostic.  They define the
   API that the application uses, but not how that API is implemented,
   instead deferring to drivers to fulfill requests using the protocol
   of their choice.  JDBC and (generally) ODBC offer results in a
   row-oriented format, while ADBC offers columnar Arrow data.

   Protocols/libraries like libpq (Postgres) and TDS (SQL Server) are
   database-specific and row-oriented.  Multiple databases may
   implement the same protocol to try to reuse each other's work,
   e.g. several databases implement the Postgres wire protocol to
   benefit from its driver implementations.  But the protocol itself
   was not designed with multiple databases in mind, nor are they
   generally meant to be used directly by applications.

   Some database-specific protocols are Arrow-native, like those of
   BigQuery and ClickHouse.  Flight SQL additionally is meant to be
   database-agnostic, but it defines both the client-facing API and
   the underlying protocol, so it's hard for applications to use it as
   the API for databases that don't already implement Flight SQL.

Existing database client APIs
-----------------------------

:doc:`Arrow Flight SQL <./FlightSql>`
  A standard building on top of Arrow Flight, defining how to use
  Flight to talk to databases, retrieve metadata, execute queries, and
  so on. Provides a single client in C++ and Java language that talks
  to any database servers implementing the protocol. Models its API
  surface (though not API design) after JDBC and ODBC.

`DBI for R <https://www.r-dbi.org/>`_
  An R package/ecosystem of packages for database access. Provides a
  single interface with "backends" for specific databases.  While
  row-oriented, `integration with Arrow is under consideration`_,
  including a sketch of effectively the same idea as ADBC.

`JDBC <https://jcp.org/en/jsr/detail?id=221>`_
  A Java library for database access, providing row-based
  APIs. Provides a single interface with drivers for specific
  databases.

`ODBC <https://github.com/microsoft/ODBC-Specification>`_
  A language-agnostic standard from the ISO/IEC for database access,
  associated with Microsoft. Feature-wise, it is similar to JDBC (and
  indeed JDBC can wrap ODBC drivers), but it offers columnar data
  support through fetching buffers of column values. (See above for
  caveats.) Provides a single C interface with drivers for specific
  databases.

`PEP 249 <https://www.python.org/dev/peps/pep-0249/>`_ (DBAPI 2.0)
  A Python standard for database access providing row-based APIs. Not
  a singular package, but rather a set of interfaces that packages
  implement.

Existing libraries
------------------

These are libraries which either 1) implement columnar data access for
a particular system; or 2) could be used to implement such access.

:doc:`Arrow Flight <./Flight>`
  An RPC framework optimized for transferring Arrow record batches,
  with application-specific extension points but without any higher
  level semantics.

:doc:`Arrow JDBC <../java/jdbc>`
  A Java submodule, part of Arrow/Java, that uses the JDBC API to
  produce Arrow data. Internally, it can read data only row-at-a-time.

`arrow-odbc <https://github.com/pacman82/arrow-odbc>`_
  A Rust community project that uses the ODBC API to produce Arrow
  data, using ODBC’s buffer-based API to perform bulk copies. (See
  also: Turbodbc.)

`Arrowdantic <https://github.com/jorgecarleitao/arrowdantic/>`_
  Python bindings for an implementation of ODBC<>Arrow in Rust.

`pgeon <https://github.com/0x0L/pgeon>`_
  A client that manually parses the Postgres wire format and produces
  Arrow data, bypassing JDBC/ODBC. While it attempts to optimize this
  case, the Postgres wire protocol is still row-oriented.

`Turbodbc <https://turbodbc.readthedocs.io/en/latest/>`_
  A set of Python ODBC bindings, implementing PEP 249, that also
  provides APIs to fetch data as Arrow batches, optimizing the
  conversion internally.

Papers
------

Raasveldt, Mark, and Hannes Mühleisen. `“Don't Hold My Data Hostage -
A Case for Client Protocol Redesign”`_. In *Proceedings of the VLDB
Endowment*, 1022–1033, 2017.

.. External link definitions follow

.. _f044edf5256abfb4c091b0ad2acc73afea2c93c0: https://github.com/apache/arrow-adbc/commit/f044edf5256abfb4c091b0ad2acc73afea2c93c0
.. _arrow-adbc: https://github.com/apache/arrow-adbc
.. _block cursors: https://docs.microsoft.com/en-us/sql/odbc/reference/develop-app/block-cursors?view=sql-server-ver15
.. _DBI is contemplating: https://r-dbi.github.io/dbi3/articles/dbi3.html
.. _“Don't Hold My Data Hostage - A Case for Client Protocol Redesign”: https://ir.cwi.nl/pub/26415
.. _integration with Arrow is under consideration: https://r-dbi.github.io/dbi3/articles/dbi3.html#using-arrowparquet-as-an-exchange-format
.. _OLTP: https://en.wikipedia.org/wiki/Online_transaction_processing
.. _represented as integers: https://docs.microsoft.com/en-us/sql/odbc/reference/develop-app/using-length-and-indicator-values?view=sql-server-ver15
.. _Result buffers are caller-allocated: https://docs.microsoft.com/en-us/sql/odbc/reference/develop-app/allocating-and-freeing-buffers?view=sql-server-ver15
.. _SQL_C_BINARY: https://docs.microsoft.com/en-us/sql/odbc/reference/appendixes/transferring-data-in-its-binary-form?view=sql-server-ver15
.. _Strings must be null-terminated: https://docs.microsoft.com/en-us/sql/odbc/reference/develop-app/character-data-and-c-strings?view=sql-server-ver15
.. _Substrait: https://substrait.io
.. _the driver must truncate the value: https://docs.microsoft.com/en-us/sql/odbc/reference/develop-app/data-length-buffer-length-and-truncation?view=sql-server-ver15
.. _Time intervals use different representations: https://docs.microsoft.com/en-us/sql/odbc/reference/appendixes/c-interval-structure?view=sql-server-ver15
.. _Trino does with connectors: https://trino.io/docs/current/connector.html
