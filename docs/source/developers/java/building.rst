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

.. highlight:: console

.. _building-arrow-java:

===================
Building Arrow Java
===================

.. contents::

System Setup
============

Arrow Java uses the `Maven <https://maven.apache.org/>`_ build system.

Building requires:

* JDK 8, 9, 10, 11, 17, or 18, but only JDK 8, 11 and 17 are tested in CI.
* Maven 3+

Building
========

All the instructions below assume that you have cloned the Arrow git
repository:

.. code-block::

    $ git clone https://github.com/apache/arrow.git
    $ cd arrow
    $ git submodule update --init --recursive

These are the options available to compile Arrow Java modules with:

* Maven build tool.
* Docker Compose.
* Archery.

Building Java Modules
---------------------

To build the default modules, go to the project root and execute:

Maven
~~~~~

.. code-block::

    $ cd arrow/java
    $ export JAVA_HOME=<absolute path to your java home>
    $ java --version
    $ mvn clean install

Docker compose
~~~~~~~~~~~~~~

.. code-block::

    $ cd arrow/java
    $ export JAVA_HOME=<absolute path to your java home>
    $ java --version
    $ docker-compose run debian-java

Archery
~~~~~~~

.. code-block::

    $ cd arrow/java
    $ export JAVA_HOME=<absolute path to your java home>
    $ java --version
    $ archery docker run debian-java

Building JNI Libraries (\*.dylib / \*.so / \*.dll)
--------------------------------------------------

First, we need to build the `C++ shared libraries`_ that the JNI bindings will use.
We can build these manually or we can use `Archery`_ to build them using a Docker container
(This will require installing Docker, Docker Compose, and Archery).

.. note::
   If you are building on Apple Silicon, be sure to use a JDK version that was compiled
   for that architecture. See, for example, the `Azul JDK <https://www.azul.com/downloads/?os=macos&architecture=arm-64-bit&package=jdk>`_.

   If you are building on Windows OS, see :ref:`Developing on Windows <developers-cpp-windows>`.

Maven
~~~~~

- To build only the JNI C Data Interface library (MacOS / Linux):

  .. code-block:: text

      $ cd arrow/java
      $ export JAVA_HOME=<absolute path to your java home>
      $ java --version
      $ mvn generate-resources -Pgenerate-libs-cdata-all-os -N
      $ ls -latr ../java-dist/lib/<your system's architecture>
      |__ libarrow_cdata_jni.dylib
      |__ libarrow_cdata_jni.so

- To build only the JNI C Data Interface library (Windows):

  .. code-block::

      $ cd arrow/java
      $ mvn generate-resources -Pgenerate-libs-cdata-all-os -N
      $ dir "../java-dist/bin/x86_64"
      |__ arrow_cdata_jni.dll

- To build all JNI libraries (MacOS / Linux) except the JNI C Data Interface library:

  .. code-block:: text

      $ cd arrow/java
      $ export JAVA_HOME=<absolute path to your java home>
      $ java --version
      $ mvn generate-resources \
          -Pgenerate-libs-jni-macos-linux \
          -DARROW_GANDIVA=ON \
          -DARROW_JAVA_JNI_ENABLE_GANDIVA=ON \
          -N
      $ ls -latr java-dist/lib/<your system's architecture>/*_{jni,java}.*
      |__ libarrow_dataset_jni.dylib
      |__ libarrow_orc_jni.dylib
      |__ libgandiva_jni.dylib

- To build all JNI libraries (Windows) except the JNI C Data Interface library:

  .. code-block::

      $ cd arrow/java
      $ mvn generate-resources -Pgenerate-libs-jni-windows -N
      $ dir "../java-dist/bin/x86_64"
      |__ arrow_dataset_jni.dll

CMake
~~~~~

- To build only the JNI C Data Interface library (MacOS / Linux):

  .. code-block:: text

      $ cd arrow
      $ mkdir -p java-dist java-cdata
      $ cmake \
          -S java \
          -B java-cdata \
          -DARROW_JAVA_JNI_ENABLE_C=ON \
          -DARROW_JAVA_JNI_ENABLE_DEFAULT=OFF \
          -DBUILD_TESTING=OFF \
          -DCMAKE_BUILD_TYPE=Release \
          -DCMAKE_INSTALL_LIBDIR=lib/<your system's architecture> \
          -DCMAKE_INSTALL_PREFIX=java-dist
      $ cmake --build java-cdata --target install --config Release
      $ ls -latr java-dist/lib
      |__ libarrow_cdata_jni.dylib
      |__ libarrow_cdata_jni.so

- To build only the JNI C Data Interface library (Windows):

  .. code-block::

      $ cd arrow
      $ mkdir java-dist, java-cdata
      $ cmake ^
          -S java ^
          -B java-cdata ^
          -DARROW_JAVA_JNI_ENABLE_C=ON ^
          -DARROW_JAVA_JNI_ENABLE_DEFAULT=OFF ^
          -DBUILD_TESTING=OFF ^
          -DCMAKE_BUILD_TYPE=Release ^
          -DCMAKE_INSTALL_LIBDIR=lib/x86_64 ^
          -DCMAKE_INSTALL_PREFIX=java-dist
      $ cmake --build java-cdata --target install --config Release
      $ dir "java-dist/bin"
      |__ arrow_cdata_jni.dll

- To build all JNI libraries (MacOS / Linux) except the JNI C Data Interface library:

  .. code-block::

      $ cd arrow
      $ brew bundle --file=cpp/Brewfile
      Homebrew Bundle complete! 25 Brewfile dependencies now installed.
      $ brew uninstall aws-sdk-cpp
      (We can't use aws-sdk-cpp installed by Homebrew because it has
      an issue: https://github.com/aws/aws-sdk-cpp/issues/1809 )
      $ export JAVA_HOME=<absolute path to your java home>
      $ mkdir -p java-dist cpp-jni
      $ cmake \
          -S cpp \
          -B cpp-jni \
          -DARROW_BUILD_SHARED=OFF \
          -DARROW_CSV=ON \
          -DARROW_DATASET=ON \
          -DARROW_DEPENDENCY_SOURCE=BUNDLED \
          -DARROW_DEPENDENCY_USE_SHARED=OFF \
          -DARROW_FILESYSTEM=ON \
          -DARROW_GANDIVA=ON \
          -DARROW_GANDIVA_STATIC_LIBSTDCPP=ON \
          -DARROW_ORC=ON \
          -DARROW_PARQUET=ON \
          -DARROW_S3=ON \
          -DARROW_USE_CCACHE=ON \
          -DCMAKE_BUILD_TYPE=Release \
          -DCMAKE_INSTALL_LIBDIR=lib/<your system's architecture> \
          -DCMAKE_INSTALL_PREFIX=java-dist \
          -DCMAKE_UNITY_BUILD=ON
      $ cmake --build cpp-jni --target install --config Release
      $ cmake \
          -S java \
          -B java-jni \
          -DARROW_JAVA_JNI_ENABLE_C=OFF \
          -DARROW_JAVA_JNI_ENABLE_DEFAULT=ON \
          -DBUILD_TESTING=OFF \
          -DCMAKE_BUILD_TYPE=Release \
          -DCMAKE_INSTALL_LIBDIR=lib/<your system's architecture> \
          -DCMAKE_INSTALL_PREFIX=java-dist \
          -DCMAKE_PREFIX_PATH=$PWD/java-dist
      $ cmake --build java-jni --target install --config Release
      $ ls -latr java-dist/lib/<your system's architecture>/*_{jni,java}.*
      |__ libarrow_dataset_jni.dylib
      |__ libarrow_orc_jni.dylib
      |__ libgandiva_jni.dylib

- To build all JNI libraries (Windows) except the JNI C Data Interface library:

  .. code-block::

      $ cd arrow
      $ mkdir java-dist, cpp-jni
      $ cmake ^
          -S cpp ^
          -B cpp-jni ^
          -DARROW_BUILD_SHARED=OFF ^
          -DARROW_CSV=ON ^
          -DARROW_DATASET=ON ^
          -DARROW_DEPENDENCY_USE_SHARED=OFF ^
          -DARROW_FILESYSTEM=ON ^
          -DARROW_ORC=OFF ^
          -DARROW_PARQUET=ON ^
          -DARROW_S3=ON ^
          -DARROW_USE_CCACHE=ON ^
          -DARROW_WITH_BROTLI=ON ^
          -DARROW_WITH_LZ4=ON ^
          -DARROW_WITH_SNAPPY=ON ^
          -DARROW_WITH_ZLIB=ON ^
          -DARROW_WITH_ZSTD=ON ^
          -DCMAKE_BUILD_TYPE=Release ^
          -DCMAKE_INSTALL_LIBDIR=lib/x86_64 ^
          -DCMAKE_INSTALL_PREFIX=java-dist ^
          -DCMAKE_UNITY_BUILD=ON ^
          -GNinja
      $ cd cpp-jni
      $ ninja install
      $ cd ../
      $ cmake ^
          -S java ^
          -B java-jni ^
          -DARROW_JAVA_JNI_ENABLE_C=OFF ^
          -DARROW_JAVA_JNI_ENABLE_DEFAULT=ON ^
          -DARROW_JAVA_JNI_ENABLE_GANDIVA=OFF ^
          -DARROW_JAVA_JNI_ENABLE_ORC=OFF ^
          -DBUILD_TESTING=OFF ^
          -DCMAKE_BUILD_TYPE=Release ^
          -DCMAKE_INSTALL_LIBDIR=lib/x86_64 ^
          -DCMAKE_INSTALL_PREFIX=java-dist ^
          -DCMAKE_PREFIX_PATH=$PWD/java-dist
      $ cmake --build java-jni --target install --config Release
      $ dir "java-dist/bin"
      |__ arrow_dataset_jni.dll

Archery
~~~~~~~

.. code-block:: text

    $ cd arrow
    $ archery docker run java-jni-manylinux-2014
    $ ls -latr java-dist/<your system's architecture>/
    |__ libarrow_cdata_jni.so
    |__ libarrow_dataset_jni.so
    |__ libarrow_orc_jni.so
    |__ libgandiva_jni.so

Building Java JNI Modules
-------------------------

- To compile the JNI bindings, use the ``arrow-c-data`` Maven profile:

  .. code-block::

      $ cd arrow/java
      $ mvn -Darrow.c.jni.dist.dir=<absolute path to your arrow folder>/java-dist/lib -Parrow-c-data clean install

- To compile the JNI bindings for ORC / Gandiva / Dataset, use the ``arrow-jni`` Maven profile:

  .. code-block::

      $ cd arrow/java
      $ mvn \
          -Darrow.cpp.build.dir=<absolute path to your arrow folder>/java-dist/lib/ \
          -Darrow.c.jni.dist.dir=<absolute path to your arrow folder>/java-dist/lib/ \
          -Parrow-jni clean install

IDE Configuration
=================

IntelliJ
--------

To start working on Arrow in IntelliJ: build the project once from the command
line using ``mvn clean install``. Then open the ``java/`` subdirectory of the
Arrow repository, and update the following settings:

* In the Files tool window, find the path ``vector/target/generated-sources``,
  right click the directory, and select Mark Directory as > Generated Sources
  Root. There is no need to mark other generated sources directories, as only
  the ``vector`` module generates sources.
* For JDK 8, disable the ``error-prone`` profile to build the project successfully.
* For JDK 11, due to an `IntelliJ bug
  <https://youtrack.jetbrains.com/issue/IDEA-201168>`__, you must go into
  Settings > Build, Execution, Deployment > Compiler > Java Compiler and disable
  "Use '--release' option for cross-compilation (Java 9 and later)". Otherwise
  you will get an error like "package sun.misc does not exist".
* You may want to disable error-prone entirely if it gives spurious
  warnings (disable both error-prone profiles in the Maven tool window
  and "Reload All Maven Projects").
* If using IntelliJ's Maven integration to build, you may need to change
  ``<fork>`` to ``false`` in the pom.xml files due to an `IntelliJ bug
  <https://youtrack.jetbrains.com/issue/IDEA-278903>`__.

You may not need to update all of these settings if you build/test with the
IntelliJ Maven integration instead of with IntelliJ directly.

Common Errors
=============

* When working with the JNI code: if the C++ build cannot find dependencies, with errors like these:

  .. code-block::

     Could NOT find Boost (missing: Boost_INCLUDE_DIR system filesystem)
     Could NOT find Lz4 (missing: LZ4_LIB)
     Could NOT find zstd (missing: ZSTD_LIB)

  Specify that the dependencies should be downloaded at build time (more details at `Dependency Resolution`_):

  .. code-block::

     -Dre2_SOURCE=BUNDLED \
     -DBoost_SOURCE=BUNDLED \
     -Dutf8proc_SOURCE=BUNDLED \
     -DSnappy_SOURCE=BUNDLED \
     -DORC_SOURCE=BUNDLED \
     -DZLIB_SOURCE=BUNDLED

.. _Archery: https://github.com/apache/arrow/blob/main/dev/archery/README.md
.. _Dependency Resolution: https://arrow.apache.org/docs/developers/cpp/building.html#individual-dependency-resolution
.. _C++ shared libraries: https://arrow.apache.org/docs/cpp/build_system.html


Installing Nightly Packages
===========================

.. warning::
    These packages are not official releases. Use them at your own risk.

Arrow nightly builds are posted on the mailing list at `builds@arrow.apache.org`_.
The artifacts are uploaded to GitHub. For example, for 2022/07/30, they can be found at `Github Nightly`_.


Installing from Apache Nightlies
--------------------------------
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
-------------------

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
