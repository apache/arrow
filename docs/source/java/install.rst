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

Java modules are currently compatible with JDK 8, 9, 10, 11, 17, and 18, but only JDK 11 is tested in CI.

Installing from Maven
---------------------

By default, Maven will download from the central repository: https://repo.maven.apache.org/maven2/org/apache/arrow/

Configure your pom.xml with the Java modules needed, for example:
arrow-vector, and arrow-memory-netty.

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
            <arrow.version>8.0.0</arrow.version>
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
        </dependencies>
    </project>

To use the Arrow Flight dependencies, also add the ``os-maven-plugin``
plugin. This plugin generates useful platform-dependent properties
such as ``os.detected.name`` and ``os.detected.arch`` needed to resolve
transitive dependencies of Flight.

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
            <arrow.version>8.0.0</arrow.version>
        </properties>
        <dependencies>
            <dependency>
                <groupId>org.apache.arrow</groupId>
                <artifactId>flight-core</artifactId>
                <version>${arrow.version}</version>
            </dependency>
        </dependencies>
        <build>
            <extensions>
                <extension>
                    <groupId>kr.motd.maven</groupId>
                    <artifactId>os-maven-plugin</artifactId>
                    <version>1.7.0</version>
                </extension>
            </extensions>
        </build>
    </project>

When using Java 17 or later, some JDK internals must be exposed by
adding ``--add-opens=java.base/java.nio=ALL-UNNAMED``. Otherwise,
you may see errors like ``module java.base does not "opens
java.nio" to unnamed module``.

For example, when running unit tests through Maven:

.. code-block::

    <build>
        <plugins>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-surefire-plugin</artifactId>
                <version>3.0.0-M6</version>
                <configuration>
                        <argLine>--add-opens=java.base/java.nio=ALL-UNNAMED</argLine>
                </configuration>
            </plugin>
        </plugins>
    </build>

Environment variables: To execute your Arrow Java main code.

.. code-block::

    _JAVA_OPTIONS="--add-opens=java.base/java.nio=ALL-UNNAMED" mvn exec:java -Dexec.mainClass="YourMainCode"

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

1. Decide nightly packages repository to use, for example: https://github.com/ursacomputing/crossbow/releases/tag/nightly-2022-03-19-0-github-java-jars
2. Add packages to your pom.xml, for example: flight-core (it depends on: arrow-format, arrow-vector, arrow-memeory-core and arrow-memory-netty).

.. code-block:: xml

    <properties>
        <maven.compiler.source>8</maven.compiler.source>
        <maven.compiler.target>8</maven.compiler.target>
        <arrow.version>8.0.0.dev254</arrow.version>
    </properties>

    <dependencies>
        <dependency>
            <groupId>org.apache.arrow</groupId>
            <artifactId>flight-core</artifactId>
            <version>${arrow.version}</version>
        </dependency>
    </dependencies>

3. Download the necessary pom and jar files to a temporary directory:

.. code-block:: shell

    $ mkdir nightly-2022-03-19-0-github-java-jars
    $ cd nightly-2022-03-19-0-github-java-jars
    $ wget https://github.com/ursacomputing/crossbow/releases/download/nightly-2022-03-19-0-github-java-jars/arrow-java-root-8.0.0.dev254.pom
    $ wget https://github.com/ursacomputing/crossbow/releases/download/nightly-2022-03-19-0-github-java-jars/arrow-format-8.0.0.dev254.pom
    $ wget https://github.com/ursacomputing/crossbow/releases/download/nightly-2022-03-19-0-github-java-jars/arrow-format-8.0.0.dev254.jar
    $ wget https://github.com/ursacomputing/crossbow/releases/download/nightly-2022-03-19-0-github-java-jars/arrow-vector-8.0.0.dev254.pom
    $ wget https://github.com/ursacomputing/crossbow/releases/download/nightly-2022-03-19-0-github-java-jars/arrow-vector-8.0.0.dev254.jar
    $ wget https://github.com/ursacomputing/crossbow/releases/download/nightly-2022-03-19-0-github-java-jars/arrow-memory-8.0.0.dev254.pom
    $ wget https://github.com/ursacomputing/crossbow/releases/download/nightly-2022-03-19-0-github-java-jars/arrow-memory-core-8.0.0.dev254.pom
    $ wget https://github.com/ursacomputing/crossbow/releases/download/nightly-2022-03-19-0-github-java-jars/arrow-memory-netty-8.0.0.dev254.pom
    $ wget https://github.com/ursacomputing/crossbow/releases/download/nightly-2022-03-19-0-github-java-jars/arrow-memory-core-8.0.0.dev254.jar
    $ wget https://github.com/ursacomputing/crossbow/releases/download/nightly-2022-03-19-0-github-java-jars/arrow-memory-netty-8.0.0.dev254.jar
    $ wget https://github.com/ursacomputing/crossbow/releases/download/nightly-2022-03-19-0-github-java-jars/arrow-flight-8.0.0.dev254.pom
    $ wget https://github.com/ursacomputing/crossbow/releases/download/nightly-2022-03-19-0-github-java-jars/flight-core-8.0.0.dev254.pom
    $ wget https://github.com/ursacomputing/crossbow/releases/download/nightly-2022-03-19-0-github-java-jars/flight-core-8.0.0.dev254.jar
    $ tree
    .
    ├── arrow-flight-8.0.0.dev254.pom
    ├── arrow-format-8.0.0.dev254.jar
    ├── arrow-format-8.0.0.dev254.pom
    ├── arrow-java-root-8.0.0.dev254.pom
    ├── arrow-memory-8.0.0.dev254.pom
    ├── arrow-memory-core-8.0.0.dev254.jar
    ├── arrow-memory-core-8.0.0.dev254.pom
    ├── arrow-memory-netty-8.0.0.dev254.jar
    ├── arrow-memory-netty-8.0.0.dev254.pom
    ├── arrow-vector-8.0.0.dev254.jar
    ├── arrow-vector-8.0.0.dev254.pom
    ├── flight-core-8.0.0.dev254.jar
    └── flight-core-8.0.0.dev254.pom

4. Install the artifacts to the local Maven repository with ``mvn install:install-file``:

.. code-block:: shell

    $ mvn install:install-file -Dfile="$(pwd)/arrow-java-root-8.0.0.dev254.pom" -DgroupId=org.apache.arrow -DartifactId=arrow-java-root -Dversion=8.0.0.dev254 -Dpackaging=pom
    $ mvn install:install-file -Dfile="$(pwd)/arrow-format-8.0.0.dev254.pom" -DgroupId=org.apache.arrow -DartifactId=arrow-format -Dversion=8.0.0.dev254 -Dpackaging=pom
    $ mvn install:install-file -Dfile="$(pwd)/arrow-format-8.0.0.dev254.jar" -DgroupId=org.apache.arrow -DartifactId=arrow-format -Dversion=8.0.0.dev254 -Dpackaging=jar
    $ mvn install:install-file -Dfile="$(pwd)/arrow-vector-8.0.0.dev254.pom" -DgroupId=org.apache.arrow -DartifactId=arrow-vector -Dversion=8.0.0.dev254 -Dpackaging=pom
    $ mvn install:install-file -Dfile="$(pwd)/arrow-vector-8.0.0.dev254.jar" -DgroupId=org.apache.arrow -DartifactId=arrow-vector -Dversion=8.0.0.dev254 -Dpackaging=jar
    $ mvn install:install-file -Dfile="$(pwd)/arrow-memory-8.0.0.dev254.pom" -DgroupId=org.apache.arrow -DartifactId=arrow-memory -Dversion=8.0.0.dev254 -Dpackaging=pom
    $ mvn install:install-file -Dfile="$(pwd)/arrow-memory-core-8.0.0.dev254.pom" -DgroupId=org.apache.arrow -DartifactId=arrow-memory-core -Dversion=8.0.0.dev254 -Dpackaging=pom
    $ mvn install:install-file -Dfile="$(pwd)/arrow-memory-netty-8.0.0.dev254.pom" -DgroupId=org.apache.arrow -DartifactId=arrow-memory-netty -Dversion=8.0.0.dev254 -Dpackaging=pom
    $ mvn install:install-file -Dfile="$(pwd)/arrow-memory-core-8.0.0.dev254.jar" -DgroupId=org.apache.arrow -DartifactId=arrow-memory-core -Dversion=8.0.0.dev254 -Dpackaging=jar
    $ mvn install:install-file -Dfile="$(pwd)/arrow-memory-netty-8.0.0.dev254.jar" -DgroupId=org.apache.arrow -DartifactId=arrow-memory-netty -Dversion=8.0.0.dev254 -Dpackaging=jar
    $ mvn install:install-file -Dfile="$(pwd)/arrow-flight-8.0.0.dev254.pom" -DgroupId=org.apache.arrow -DartifactId=arrow-flight -Dversion=8.0.0.dev254 -Dpackaging=pom
    $ mvn install:install-file -Dfile="$(pwd)/flight-core-8.0.0.dev254.pom" -DgroupId=org.apache.arrow -DartifactId=flight-core -Dversion=8.0.0.dev254 -Dpackaging=pom
    $ mvn install:install-file -Dfile="$(pwd)/flight-core-8.0.0.dev254.jar" -DgroupId=org.apache.arrow -DartifactId=flight-core -Dversion=8.0.0.dev254 -Dpackaging=jar

5. Validate that the packages were installed:

.. code-block:: shell

    $ tree ~/.m2/repository/org/apache/arrow
    .
    ├── arrow-flight
    │   ├── 8.0.0.dev254
    │   │   └── arrow-flight-8.0.0.dev254.pom
    ├── arrow-format
    │   ├── 8.0.0.dev254
    │   │   ├── arrow-format-8.0.0.dev254.jar
    │   │   └── arrow-format-8.0.0.dev254.pom
    ├── arrow-java-root
    │   ├── 8.0.0.dev254
    │   │   └── arrow-java-root-8.0.0.dev254.pom
    ├── arrow-memory
    │   ├── 8.0.0.dev254
    │   │   └── arrow-memory-8.0.0.dev254.pom
    ├── arrow-memory-core
    │   ├── 8.0.0.dev254
    │   │   ├── arrow-memory-core-8.0.0.dev254.jar
    │   │   └── arrow-memory-core-8.0.0.dev254.pom
    ├── arrow-memory-netty
    │   ├── 8.0.0.dev254
    │   │   ├── arrow-memory-netty-8.0.0.dev254.jar
    │   │   └── arrow-memory-netty-8.0.0.dev254.pom
    ├── arrow-vector
    │   ├── 8.0.0.dev254
    │   │   ├── _remote.repositories
    │   │   ├── arrow-vector-8.0.0.dev254.jar
    │   │   └── arrow-vector-8.0.0.dev254.pom
    └── flight-core
        ├── 8.0.0.dev254
        │   ├── flight-core-8.0.0.dev254.jar
        │   └── flight-core-8.0.0.dev254.pom

6. Compile your project like usual with ``mvn clean install``.

.. _builds@arrow.apache.org: https://lists.apache.org/list.html?builds@arrow.apache.org
.. _Github Nightly: https://github.com/ursacomputing/crossbow/releases/tag/nightly-2022-03-19-0-github-java-jars