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

.. _flight-sql:

================
Arrow Flight SQL
================

Arrow Flight SQL is a protocol for interacting with SQL databases
using the Arrow in-memory format and the :doc:`Flight RPC
<./Flight>` framework.

Generally, a database will implement the RPC methods according to the
specification, but does not need to implement a client-side driver. A
database client can use the provided Flight SQL client to interact
with any database that supports the necessary endpoints. Flight SQL
clients wrap the underlying Flight client to provide methods for the
new RPC methods described here.

RPC Methods
===========

Flight SQL reuses the predefined RPC methods in Arrow Flight, and
provides various commands that pair those methods with request/response
messages defined via Protobuf (see below).

SQL Metadata
------------

Flight SQL provides a variety of commands to fetch catalog metadata
about the database server.

All of these commands can be used with the GetFlightInfo and GetSchema
RPC methods. The Protobuf request message should be packed into a
google.protobuf.Any message, then serialized and packed as the ``cmd``
field in a CMD-type FlightDescriptor.

If the command is used with GetFlightInfo, the server will return a
FlightInfo response. The client should then use the Ticket(s) in the
FlightInfo with the DoGet RPC method to fetch a Arrow data containing
the results of the command. In other words, SQL metadata is returned as
Arrow data, just like query results themselves.

The Arrow schema returned by GetSchema or DoGet for a particular
command is fixed according to the specification.

``CommandGetCatalogs``
    List the catalogs available in the database. The definition varies
    by vendor.

``CommandGetCrossReference``
    List the foreign key columns in a given table that reference
    columns in a given parent table.

``CommandGetDbSchemas``
    List the schemas (note: a grouping of tables, *not* an Arrow
    schema) available in the database. The definition varies by
    vendor.

``CommandGetExportedKeys``
    List foreign key columns that reference the primary key columns of
    a given table.

``CommandGetImportedKeys``
    List foreign keys of a given table.

``CommandGetPrimaryKeys``
    List the primary keys of a given table.

``CommandGetSqlInfo``
    Fetch metadata about the database server and its supported SQL
    features.

``CommandGetTables``
    List tables in the database.

``CommandGetTableTypes``
    List table types in the database. The list of types varies by
    vendor.

Query Execution
---------------

Flight SQL also provides commands to execute SQL queries and manage
prepared statements.

Many of these commands are also used with GetFlightInfo/GetSchema and
work identically to the metadata methods above. Some of these commands
can be used with the DoPut RPC method, but the command should still be
encoded in the request FlightDescriptor in the same way.

Commands beginning with "Action" are instead used with the DoAction
RPC method, in which case the command should be packed into a
google.protobuf.Any message, then serialized and packed into the
``body`` of a Flight Action. Also, the action ``type`` should be set
to the command name (i.e. for ``ActionClosePreparedStatementRequest``,
the ``type`` should be ``ClosePreparedStatement``).

Commands that execute updates such as ``CommandStatementUpdate`` and
``CommandStatementIngest`` return a Flight SQL ``DoPutUpdateResult``
after consuming the entire FlightData stream. This message is encoded
in the ``app_metadata`` field of the Flight RPC ``PutResult`` returned.


``ActionClosePreparedStatementRequest``
    Close a previously created prepared statement.

``ActionCreatePreparedStatementRequest``
    Create a new prepared statement for a SQL query.

    The response will contain an opaque handle used to identify the
    prepared statement.  It may also contain two optional schemas: the
    Arrow schema of the result set, and the Arrow schema of the bind
    parameters (if any).  Because the schema of the result set may
    depend on the bind parameters, the schemas may not necessarily be
    provided here as a result, or if provided, they may not be accurate.
    Clients should not assume the schema provided here will be the
    schema of any data actually returned by executing the prepared
    statement.

    Some statements may have bind parameters without any specific type.
    (As a trivial example for SQL, consider ``SELECT ?``.)  It is
    not currently specified how this should be handled in the bind
    parameter schema above.  We suggest either using a union type to
    enumerate the possible types, or using the NA (null) type as a
    wildcard/placeholder.

``CommandPreparedStatementQuery``
    Execute a previously created prepared statement and get the results.

    When used with DoPut: binds parameter values to the prepared statement.
    The server may optionally provide an updated handle in the response.
    Updating the handle allows the client to supply all state required to
    execute the query in an ActionPreparedStatementExecute message.
    For example, stateless servers can encode the bound parameter values into
    the new handle, and the client will send that new handle with parameters
    back to the server.

    Note that a handle returned from a DoPut call with
    CommandPreparedStatementQuery can itself be passed to a subsequent DoPut
    call with CommandPreparedStatementQuery to bind a new set of parameters.
    The subsequent call itself may return an updated handle which again should
    be used for subsequent requests.

    The server is responsible for detecting the case where the client does not
    use the updated handle and should return an error.

    When used with GetFlightInfo: execute the prepared statement. The
    prepared statement can be reused after fetching results.

    When used with GetSchema: get the expected Arrow schema of the
    result set.  If the client has bound parameter values with DoPut
    previously, the server should take those values into account.

``CommandPreparedStatementUpdate``
    Execute a previously created prepared statement that does not
    return results.

    When used with DoPut: execute the query and return the number of
    affected rows. The prepared statement can be reused afterwards.

``CommandStatementQuery``
    Execute an ad-hoc SQL query.

    When used with GetFlightInfo: execute the query (call DoGet to
    fetch results).

    When used with GetSchema: return the schema of the query results.

``CommandStatementUpdate``
    Execute an ad-hoc SQL query that does not return results.

    When used with DoPut: execute the query and return the number of
    affected rows.

``CommandStatementIngest``
    Execute a bulk ingestion.

    When used with DoPut: load the stream of Arrow record batches into
    the specified target table and return the number of rows ingested
    via a `DoPutUpdateResult` message.

Flight Server Session Management
--------------------------------

Flight SQL provides commands to set and update server session variables
which affect the server behaviour in various ways.  Common options may
include (depending on the server implementation) ``catalog`` and
``schema``, indicating the currently-selected catalog and schema for
queries to be run against.

Clients should prefer, where possible, setting options prior to issuing
queries and other commands, as some server implementations may require
these options be set exactly once and prior to any other activity which
may trigger their implicit setting.

For compatibility with Database Connectivity drivers (JDBC, ODBC, and
others), it is strongly recommended that server implementations accept
string representations of all option values which may be provided to the
driver as part of a server connection string and passed through to the
server without further conversion.  For ease of use it is also recommended
to accept and convert other numeric types to the preferred type for an
option value, however this is not required.

Sessions are persisted between the client and server using an
implementation-defined mechanism, which is typically RFC 6265 cookies.
Servers may also combine other connection state opaquely with the
session token:  Consider that the lifespan and semantics of a session
should make sense for any additional uses, e.g. CloseSession would also
invalidate any authentication context persisted via the session context.
A session may be initiated upon a nonempty (or empty) SetSessionOptions
call, or at any other time of the server's choosing.

``SetSessionOptions``
Set server session option(s) by name/value.

``GetSessionOptions``
Get the current server session options, including those set by the client
and any defaulted or implicitly set by the server.

``CloseSession``
Close and invalidate the current session context.

Sequence Diagrams
=================

.. mermaid:: ./FlightSql/CommandGetTables.mmd
  :caption: Listing available tables.

.. mermaid:: ./FlightSql/CommandStatementQuery.mmd
  :caption: Executing an ad-hoc query.

.. mermaid:: ./FlightSql/CommandPreparedStatementQuery.mmd
  :caption: Creating a prepared statement, then executing it.

.. mermaid:: ./FlightSql/CommandStatementIngest.mmd
  :caption: Executing a bulk ingestion.

External Resources
==================

- `Introducing Apache Arrow Flight SQL: Accelerating Database Access
  <https://arrow.apache.org/blog/2022/02/16/introducing-arrow-flight-sql/>`_

Protocol Buffer Definitions
===========================

.. literalinclude:: ../../../format/FlightSql.proto
   :language: protobuf
   :linenos:
