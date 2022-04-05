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

Installing Java Modules
=======================

.. contents::

System Compatibility
--------------------

Java modules are regularly built and tested on macOS and Linux distributions.

Java Compatibility
------------------

Java modules are currently compatible with Java 8 / 9 / 10 / 11.

Installing from Maven
---------------------

By default, Maven will download from the central repository: https://repo.maven.apache.org/maven2/org/apache/arrow/

Configure your pom.xml with the Java modules needed, for example:
``arrow-memory-netty``, ``arrow-format``, and ``arrow-vector``.

.. code-block::

    <?xml version="1.0" encoding="UTF-8"?>
    <project xmlns="http://maven.apache.org/POM/4.0.0"
             xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
             xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
        <modelVersion>4.0.0</modelVersion>
        <groupId>org.example</groupId>
        <artifactId>demo</artifactId>
        <version>1.0-SNAPSHOT</version>
        <properties>
            <arrow.version>7.0.0</arrow.version>
        </properties>
        <dependencies>
            <dependency>
                <groupId>org.apache.arrow</groupId>
                <artifactId>arrow-vector</artifactId>
                <version>${arrow.version}</version>
            </dependency>
            <dependency>
                <groupId>org.apache.arrow</groupId>
                <artifactId>arrow-memory-netty</artifactId>
                <version>${arrow.version}</version>
            </dependency>
            <dependency>
                <groupId>org.apache.arrow</groupId>
                <artifactId>arrow-format</artifactId>
                <version>${arrow.version}</version>
            </dependency>
        </dependencies>
    </project>

Installing from Source
----------------------

See :ref:`java-development`.

Installing Nightly Packages
---------------------------

.. warning::
    These packages are not official releases. Use them at your own risk.

Arrow nightly builds are posted on the mailing list at `builds@arrow.apache.org`_.
The artifacts are uploaded to GitHub. For example, for 2022/03/01, they can be found at `Github Nightly`_.

Maven cannot directly use the artifacts from GitHub.
Instead, install them to the local Maven repository:

1. Decide nightly packages repository to use, for example: https://github.com/ursacomputing/crossbow/releases/tag/nightly-2022-03-03-0-github-java-jars
2. Add packages to your pom.xml, for example: ``arrow-vector`` and ``arrow-format``:

.. code-block:: xml

    <properties>
        <maven.compiler.source>8</maven.compiler.source>
        <maven.compiler.target>8</maven.compiler.target>
        <arrow.version>8.0.0.dev165</arrow.version>
    </properties>

    <dependencies>
        <dependency>
            <groupId>org.apache.arrow</groupId>
            <artifactId>arrow-vector</artifactId>
            <version>${arrow.version}</version>
        </dependency>
        <dependency>
            <groupId>org.apache.arrow</groupId>
            <artifactId>arrow-format</artifactId>
            <version>${arrow.version}</version>
        </dependency>
    </dependencies>

3. Download packages needed to a temporary directory:

.. code-block:: shell

    $ mkdir nightly-2022-03-03-0-github-java-jars
    $ cd nightly-2022-03-03-0-github-java-jars
    $ wget https://github.com/ursacomputing/crossbow/releases/download/nightly-2022-03-03-0-github-java-jars/arrow-vector-8.0.0.dev165.jar
    $ wget https://github.com/ursacomputing/crossbow/releases/download/nightly-2022-03-03-0-github-java-jars/arrow-format-8.0.0.dev165.jar
    $ tree
    |__ arrow-format-8.0.0.dev165.jar
    |__ arrow-vector-8.0.0.dev165.jar

4. Install the artifacts to the local Maven repository with ``mvn install:install-file``:

.. code-block:: shell

    $ mvn install:install-file \
        -Dfile="$(pwd)/arrow-format-8.0.0.dev165.jar" \
        -DgroupId=org.apache.arrow \
        -DartifactId=arrow-format \
        -Dversion=8.0.0.dev165 \
        -Dpackaging=jar \
        -DcreateChecksum=true \
        -Dgenerate.pom=true
    [INFO] Installing /nightly-2022-03-03-0-github-java-jars/arrow-format-8.0.0.dev165.jar to /Users/arrow/.m2/repository/org/apache/arrow/arrow-format/8.0.0.dev165/arrow-format-8.0.0.dev165.jar
    $ mvn install:install-file \
        -Dfile="$(pwd)/arrow-vector-8.0.0.dev165.jar" \
        -DgroupId=org.apache.arrow \
        -DartifactId=arrow-vector \
        -Dversion=8.0.0.dev165 \
        -Dpackaging=jar \
        -DcreateChecksum=true \
        -Dgenerate.pom=true
    [INFO] Installing /nightly-2022-03-03-0-github-java-jars/arrow-vector-8.0.0.dev165.jar to /Users/arrow/.m2/repository/org/apache/arrow/arrow-vector/8.0.0.dev165/arrow-vector-8.0.0.dev165.jar

5. Validate that the packages were installed:

.. code-block:: shell

    $ tree /Users/arrow/.m2/repository/org/apache/arrow
    |__ arrow-format
        |__ 8.0.0.dev165
            |__ arrow-format-8.0.0.dev165.jar
            |__ arrow-format-8.0.0.dev165.pom
    |__ arrow-vector
        |__ 8.0.0.dev165
            |__ arrow-vector-8.0.0.dev165.jar
            |__ arrow-vector-8.0.0.dev165.pom

6. Compile your project like usual with ``mvn install``.

.. _builds@arrow.apache.org: https://lists.apache.org/list.html?builds@arrow.apache.org
.. _Github Nightly: https://github.com/ursacomputing/crossbow/releases/tag/nightly-2022-03-01-0-github-java-jars