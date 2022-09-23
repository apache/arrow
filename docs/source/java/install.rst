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

=======================
Installing Java Modules
=======================

.. contents::

System Compatibility
====================

Java modules are regularly built and tested on macOS and Linux distributions.

Java Compatibility
==================

Java modules are compatible with JDK 8 and above.
Currently, JDK 8, 11, 17, and 18 are tested in CI.

When using Java 9 or later, some JDK internals must be exposed by
adding ``--add-opens=java.base/java.nio=ALL-UNNAMED`` to the ``java`` command:

.. code-block:: shell

   # Directly on the command line
   $ java --add-opens=java.base/java.nio=ALL-UNNAMED -jar ...
   # Indirectly via environment variables
   $ env _JAVA_OPTIONS="--add-opens=java.base/java.nio=ALL-UNNAMED" java -jar ...

Otherwise, you may see errors like ``module java.base does not "opens
java.nio" to unnamed module``.

If using Maven and Surefire for unit testing, :ref:`this argument must
be added to Surefire as well <java-install-maven-testing>`.

Installing from Maven
=====================

By default, Maven will download from the central repository: https://repo.maven.apache.org/maven2/org/apache/arrow/

Configure your pom.xml with the Java modules needed, for example:
arrow-vector, and arrow-memory-netty.

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

.. _java-install-maven-testing:

The ``--add-opens`` flag must be added when running unit tests through Maven:

.. code-block:: xml

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
======================

See :ref:`java-development`.

IDE Configuration
=================

Generally, no additional configuration should be needed.  However,
ensure your Maven or other build configuration has the ``--add-opens``
flag as described above, so that the IDE picks it up and runs tests
with that flag as well.
