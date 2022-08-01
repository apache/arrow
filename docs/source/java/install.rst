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

Java modules are currently compatible with JDK 8, 9, 10, 11, 17, and 18.
Currently, JDK 8, 11, 17, and 18 are tested in CI.

When using Java 9 or later, some JDK internals must be exposed by
adding ``--add-opens=java.base/java.nio=ALL-UNNAMED``. Otherwise,
you may see errors like ``module java.base does not "opens
java.nio" to unnamed module``.

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
            <arrow.version>9.0.0</arrow.version>
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
            <arrow.version>9.0.0</arrow.version>
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

The ``--add-opens`` flag can be added when running unit tests through Maven:

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

Or they can be added via environment variable, for example when executing your code:

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

Installing from Apache Nightlies
********************************
1. Look up the nightly version number for the Arrow libraries used.

   For example, for ``arrow-memory``, visit  https://nightlies.apache.org/arrow/java/org/apache/arrow/arrow-memory/ and see what versions are available (e.g. 9.0.0.dev501).
2. Add Apache Nightlies Repository to the Maven/Gradle project.

.. code-block:: xml

    <properties>
        <arrow.version>9.0.0.dev501</arrow.version>
    </properties>
    ...
    <repositories>
        <repository>
            <id>arrow-apache-nightlies</id>
            <url>https://nightlies.apache.org/arrow/java</url>
        </repository>
    </repositories>
    ...
    <dependencies>
        <dependency>
            <groupId>org.apache.arrow</groupId>
            <artifactId>arrow-vector</artifactId>
            <version>${arrow.version}</version>
        </dependency>
    </dependencies>
    ...

Installing Manually
*******************

1. Decide nightly packages repository to use, for example: https://github.com/ursacomputing/crossbow/releases/tag/nightly-packaging-2022-07-30-0-github-java-jars
2. Add packages to your pom.xml, for example: flight-core (it depends on: arrow-format, arrow-vector, arrow-memeory-core and arrow-memory-netty).

.. code-block:: xml

    <properties>
        <maven.compiler.source>8</maven.compiler.source>
        <maven.compiler.target>8</maven.compiler.target>
        <arrow.version>9.0.0.dev501</arrow.version>
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

    $ mkdir nightly-packaging-2022-07-30-0-github-java-jars
    $ cd nightly-packaging-2022-07-30-0-github-java-jars
    $ wget https://github.com/ursacomputing/crossbow/releases/download/nightly-packaging-2022-07-30-0-github-java-jars/arrow-java-root-9.0.0.dev501.pom
    $ wget https://github.com/ursacomputing/crossbow/releases/download/nightly-packaging-2022-07-30-0-github-java-jars/arrow-format-9.0.0.dev501.pom
    $ wget https://github.com/ursacomputing/crossbow/releases/download/nightly-packaging-2022-07-30-0-github-java-jars/arrow-format-9.0.0.dev501.jar
    $ wget https://github.com/ursacomputing/crossbow/releases/download/nightly-packaging-2022-07-30-0-github-java-jars/arrow-vector-9.0.0.dev501.pom
    $ wget https://github.com/ursacomputing/crossbow/releases/download/nightly-packaging-2022-07-30-0-github-java-jars/arrow-vector-9.0.0.dev501.jar
    $ wget https://github.com/ursacomputing/crossbow/releases/download/nightly-packaging-2022-07-30-0-github-java-jars/arrow-memory-9.0.0.dev501.pom
    $ wget https://github.com/ursacomputing/crossbow/releases/download/nightly-packaging-2022-07-30-0-github-java-jars/arrow-memory-core-9.0.0.dev501.pom
    $ wget https://github.com/ursacomputing/crossbow/releases/download/nightly-packaging-2022-07-30-0-github-java-jars/arrow-memory-netty-9.0.0.dev501.pom
    $ wget https://github.com/ursacomputing/crossbow/releases/download/nightly-packaging-2022-07-30-0-github-java-jars/arrow-memory-core-9.0.0.dev501.jar
    $ wget https://github.com/ursacomputing/crossbow/releases/download/nightly-packaging-2022-07-30-0-github-java-jars/arrow-memory-netty-9.0.0.dev501.jar
    $ wget https://github.com/ursacomputing/crossbow/releases/download/nightly-packaging-2022-07-30-0-github-java-jars/arrow-flight-9.0.0.dev501.pom
    $ wget https://github.com/ursacomputing/crossbow/releases/download/nightly-packaging-2022-07-30-0-github-java-jars/flight-core-9.0.0.dev501.pom
    $ wget https://github.com/ursacomputing/crossbow/releases/download/nightly-packaging-2022-07-30-0-github-java-jars/flight-core-9.0.0.dev501.jar
    $ tree
    .
    ├── arrow-flight-9.0.0.dev501.pom
    ├── arrow-format-9.0.0.dev501.jar
    ├── arrow-format-9.0.0.dev501.pom
    ├── arrow-java-root-9.0.0.dev501.pom
    ├── arrow-memory-9.0.0.dev501.pom
    ├── arrow-memory-core-9.0.0.dev501.jar
    ├── arrow-memory-core-9.0.0.dev501.pom
    ├── arrow-memory-netty-9.0.0.dev501.jar
    ├── arrow-memory-netty-9.0.0.dev501.pom
    ├── arrow-vector-9.0.0.dev501.jar
    ├── arrow-vector-9.0.0.dev501.pom
    ├── flight-core-9.0.0.dev501.jar
    └── flight-core-9.0.0.dev501.pom

4. Install the artifacts to the local Maven repository with ``mvn install:install-file``:

.. code-block:: shell

    $ mvn install:install-file -Dfile="$(pwd)/arrow-java-root-9.0.0.dev501.pom" -DgroupId=org.apache.arrow -DartifactId=arrow-java-root -Dversion=9.0.0.dev501 -Dpackaging=pom
    $ mvn install:install-file -Dfile="$(pwd)/arrow-format-9.0.0.dev501.pom" -DgroupId=org.apache.arrow -DartifactId=arrow-format -Dversion=9.0.0.dev501 -Dpackaging=pom
    $ mvn install:install-file -Dfile="$(pwd)/arrow-format-9.0.0.dev501.jar" -DgroupId=org.apache.arrow -DartifactId=arrow-format -Dversion=9.0.0.dev501 -Dpackaging=jar
    $ mvn install:install-file -Dfile="$(pwd)/arrow-vector-9.0.0.dev501.pom" -DgroupId=org.apache.arrow -DartifactId=arrow-vector -Dversion=9.0.0.dev501 -Dpackaging=pom
    $ mvn install:install-file -Dfile="$(pwd)/arrow-vector-9.0.0.dev501.jar" -DgroupId=org.apache.arrow -DartifactId=arrow-vector -Dversion=9.0.0.dev501 -Dpackaging=jar
    $ mvn install:install-file -Dfile="$(pwd)/arrow-memory-9.0.0.dev501.pom" -DgroupId=org.apache.arrow -DartifactId=arrow-memory -Dversion=9.0.0.dev501 -Dpackaging=pom
    $ mvn install:install-file -Dfile="$(pwd)/arrow-memory-core-9.0.0.dev501.pom" -DgroupId=org.apache.arrow -DartifactId=arrow-memory-core -Dversion=9.0.0.dev501 -Dpackaging=pom
    $ mvn install:install-file -Dfile="$(pwd)/arrow-memory-netty-9.0.0.dev501.pom" -DgroupId=org.apache.arrow -DartifactId=arrow-memory-netty -Dversion=9.0.0.dev501 -Dpackaging=pom
    $ mvn install:install-file -Dfile="$(pwd)/arrow-memory-core-9.0.0.dev501.jar" -DgroupId=org.apache.arrow -DartifactId=arrow-memory-core -Dversion=9.0.0.dev501 -Dpackaging=jar
    $ mvn install:install-file -Dfile="$(pwd)/arrow-memory-netty-9.0.0.dev501.jar" -DgroupId=org.apache.arrow -DartifactId=arrow-memory-netty -Dversion=9.0.0.dev501 -Dpackaging=jar
    $ mvn install:install-file -Dfile="$(pwd)/arrow-flight-9.0.0.dev501.pom" -DgroupId=org.apache.arrow -DartifactId=arrow-flight -Dversion=9.0.0.dev501 -Dpackaging=pom
    $ mvn install:install-file -Dfile="$(pwd)/flight-core-9.0.0.dev501.pom" -DgroupId=org.apache.arrow -DartifactId=flight-core -Dversion=9.0.0.dev501 -Dpackaging=pom
    $ mvn install:install-file -Dfile="$(pwd)/flight-core-9.0.0.dev501.jar" -DgroupId=org.apache.arrow -DartifactId=flight-core -Dversion=9.0.0.dev501 -Dpackaging=jar

5. Validate that the packages were installed:

.. code-block:: shell

    $ tree ~/.m2/repository/org/apache/arrow
    .
    ├── arrow-flight
    │   ├── 9.0.0.dev501
    │   │   └── arrow-flight-9.0.0.dev501.pom
    ├── arrow-format
    │   ├── 9.0.0.dev501
    │   │   ├── arrow-format-9.0.0.dev501.jar
    │   │   └── arrow-format-9.0.0.dev501.pom
    ├── arrow-java-root
    │   ├── 9.0.0.dev501
    │   │   └── arrow-java-root-9.0.0.dev501.pom
    ├── arrow-memory
    │   ├── 9.0.0.dev501
    │   │   └── arrow-memory-9.0.0.dev501.pom
    ├── arrow-memory-core
    │   ├── 9.0.0.dev501
    │   │   ├── arrow-memory-core-9.0.0.dev501.jar
    │   │   └── arrow-memory-core-9.0.0.dev501.pom
    ├── arrow-memory-netty
    │   ├── 9.0.0.dev501
    │   │   ├── arrow-memory-netty-9.0.0.dev501.jar
    │   │   └── arrow-memory-netty-9.0.0.dev501.pom
    ├── arrow-vector
    │   ├── 9.0.0.dev501
    │   │   ├── _remote.repositories
    │   │   ├── arrow-vector-9.0.0.dev501.jar
    │   │   └── arrow-vector-9.0.0.dev501.pom
    └── flight-core
        ├── 9.0.0.dev501
        │   ├── flight-core-9.0.0.dev501.jar
        │   └── flight-core-9.0.0.dev501.pom

6. Compile your project like usual with ``mvn clean install``.

.. _builds@arrow.apache.org: https://lists.apache.org/list.html?builds@arrow.apache.org
.. _Github Nightly: https://github.com/ursacomputing/crossbow/releases/tag/nightly-packaging-2022-07-30-0-github-java-jars