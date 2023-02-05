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

============================
Arrow Flight SQL JDBC Driver
============================

The Flight SQL JDBC driver is a JDBC driver implementation that uses
the :doc:`Flight SQL protocol <../format/FlightSql>` under the hood.
This driver can be used with any database that implements Flight SQL.

.. contents::

Installation and Requirements
=============================

The driver is compatible with JDK 8+.  On JDK 9+, the following JVM
parameter is required:

.. code-block:: shell

   java --add-opens=java.base/java.nio=ALL-UNNAMED ...

To add a dependency via Maven, use a ``pom.xml`` like the following:

.. code-block:: xml

   <?xml version="1.0" encoding="UTF-8"?>
   <project xmlns="http://maven.apache.org/POM/4.0.0"
            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
            xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
     <modelVersion>4.0.0</modelVersion>
     <groupId>org.example</groupId>
     <artifactId>demo</artifactId>
     <version>1.0-SNAPSHOT</version>
     <properties>
       <arrow.version>10.0.0</arrow.version>
     </properties>
     <dependencies>
       <dependency>
         <groupId>org.apache.arrow</groupId>
         <artifactId>flight-sql-jdbc-driver</artifactId>
         <version>${arrow.version}</version>
       </dependency>
     </dependencies>
   </project>

Connecting to a Database
========================

The URI format is as follows::

  jdbc:arrow-flight-sql://HOSTNAME:PORT[/?param1=val1&param2=val2&...]

For example, take this URI::

  jdbc:arrow-flight-sql://localhost:12345/?username=admin&password=pass&useEncryption=1

This will connect to a Flight SQL service running on ``localhost`` on
port 12345.  It will create a secure, encrypted connection, and
authenticate using the username ``admin`` and the password ``pass``.

The components of the URI are as follows.

* The URI scheme must be ``jdbc:arrow-flight-sql://``.
* **HOSTNAME** is the hostname of the Flight SQL service.
* **PORT** is the port of the Flight SQL service.

Additional options can be passed as query parameters.  The supported
parameters are:

.. list-table::
   :header-rows: 1

   * - Parameter
     - Default
     - Description

   * - disableCertificateVerification
     - false
     - When TLS is enabled, whether to verify the server certificate

   * - password
     - null
     - The password for user/password authentication

   * - threadPoolSize
     - 1
     - The size of an internal thread pool

   * - token
     - null
     - The token used for token authentication

   * - trustStore
     - null
     - When TLS is enabled, the path to the certificate store

   * - trustStorePassword
     - null
     - When TLS is enabled, the password for the certificate store

   * - useEncryption
     - false
     - Whether to use TLS (the default is an insecure, plaintext
       connection)

   * - username
     - null
     - The username for user/password authentication

   * - useSystemTrustStore
     - true
     - When TLS is enabled, whether to use the system certificate store

Any URI parameters that are not handled by the driver are passed to
the Flight SQL service as gRPC headers. For example, the following URI ::

  jdbc:arrow-flight-sql://localhost:12345/?useEncryption=0&database=mydb

This will connect without authentication or encryption, to a Flight
SQL service running on ``localhost`` on port 12345. Each request will
also include a `database=mydb` gRPC header.
