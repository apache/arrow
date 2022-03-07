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

Arrow nightly builds are uploaded to GitHub. For example, for 2022/03/01, they can be found at `Github Nightly`_.

There is not a recipe to configure maven settings.xml/pom.xml to integrate local project with github nightly packages
in a easily/transparent way. For that reason if you need to test nightly packages one way is:

* Define what nightly directory are you needed, example: nightly-2022-03-03-0-github-java-jars
* Then, define packages needed: ALL or only needed (memory)
* Then, run a shell that download ALL or only needed (memory) jar|pom files to a temporary directory
* Then, the same shell install packages downloaded locally using maven command `mvn install:install-file`
* Then, go to your project add nightly packages needed
* Then, compile your project with `mvn clean install`

These are detailed steps:

Create a filename `arrow_java_nightly.sh` that contains the following lines ofcode:

.. code-block:: shell

    $ cat arrow_java_nightly.sh
    #!/bin/bash

    # Shell variables
    ARROW_JAVA_NIGHTLY_VERSION=${1:-'nightly-2022-03-03-0-github-java-jars'}
    DEPENDENCY_TO_INSTALL=${2:-'arrow'}

    # Local Variables
    TMP_FOLDER=arrow_java_$(date +"%d-%m-%Y")
    PATTERN_TO_GET_LIB_AND_VERSION='([a-z].+)-([0-9].[0-9].[0-9].dev[0-9]+).([a-z]+)'

    # Aplication logic
    echo $DEPENDENCY_TO_INSTALL
    mkdir -p $TMP_FOLDER
    pushd $TMP_FOLDER
    echo "**************** 1 - Download arrow-java $1 dependencies ****************"
    wget $( \
        wget \
            -qO- https://api.github.com/repos/ursacomputing/crossbow/releases/tags/$ARROW_JAVA_NIGHTLY_VERSION \
            | jq -r '.assets[] | select((.name | endswith(".pom")) or (.name | endswith(".jar"))) | .browser_download_url' \
            | grep $DEPENDENCY_TO_INSTALL )


    echo "**************** 2 - Install arrow java libraries to local repository ****************"
    for LIBRARY in $(ls | grep -E '.jar' | grep dev); do
        [[ $LIBRARY =~ $PATTERN_TO_GET_LIB_AND_VERSION ]]
        FILE=$PWD/${BASH_REMATCH[0]}
        if [[ ( ${BASH_REMATCH[0]} == *"$DEPENDENCY_TO_INSTALL"* ) ]];then
            if [ -f "$FILE" ]; then
                FILE=$FILE
            else
                if [ -f "$FILE.jar" ]; then # Out of regex: -javadoc.jar / -sources.jar
                    FILE=$FILE.jar
                else
                    if [ -f "$FILE-with-dependencies.jar" ]; then # Out of regex: -with-dependencies.jar
                        FILE=$FILE-with-dependencies.jar
                    else
                        echo "Please! Review $FILE, it was not intalled on m2 locally."
                    fi
                fi
            fi
            echo "$FILE"
            mvn install:install-file \
                -Dfile="$FILE" \
                -DgroupId=org.apache.arrow \
                -DartifactId=${BASH_REMATCH[1]} \
                -Dversion=${BASH_REMATCH[2]} \
                -Dpackaging=${BASH_REMATCH[3]} \
                -DcreateChecksum=true \
                -Dgenerate.pom=true
        fi
    done
    popd
    # rm -rf $TMP_FOLDER
    echo "Go to your project and execute: mvn clean install"

Run the shell file just created with the following parameters:

.. code-block:: shell

    # Download all dependencies
    $ sh arrow_java_nightly.sh nightly-2022-03-03-0-github-java-jars

    # Download needed library, for example: memory
    $ sh arrow_java_nightly.sh nightly-2022-03-03-0-github-java-jars memory

Run maven project and execute:

.. code-block:: shell

    $ mvn clean install

Arrow nightly builds are posted on the mailing list at `builds@arrow.apache.org`_.

.. _builds@arrow.apache.org: https://lists.apache.org/list.html?builds@arrow.apache.org
.. _Github Nightly: https://github.com/ursacomputing/crossbow/releases/tag/nightly-2022-03-01-0-github-java-jars