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

======================
Development Guidelines
======================

.. contents::

Unit Testing
============

.. code-block::

    $ cd arrow/java
    $ mvn \
        -Darrow.cpp.build.dir=$GITHUB_WORKSPACE/java-dist/lib -Parrow-jni \
        -Darrow.c.jni.dist.dir=$GITHUB_WORKSPACE/java-dist/lib -Parrow-c-data \
        clean install
    [INFO] Reactor Summary for Apache Arrow Java Root POM 7.0.0:
    [INFO]
    [INFO] Apache Arrow Java Root POM ......................... SUCCESS [  7.308 s]
    [INFO] Arrow Format ....................................... SUCCESS [  4.899 s]
    [INFO] Arrow Memory ....................................... SUCCESS [  2.065 s]
    [INFO] Arrow Memory - Core ................................ SUCCESS [ 11.417 s]
    [INFO] Arrow Memory - Unsafe .............................. SUCCESS [  8.132 s]
    [INFO] Arrow Memory - Netty ............................... SUCCESS [ 10.211 s]
    [INFO] Arrow Vectors ...................................... SUCCESS [01:48 min]
    [INFO] Arrow Compression .................................. SUCCESS [  7.282 s]
    [INFO] Arrow Tools ........................................ SUCCESS [ 12.612 s]
    [INFO] Arrow JDBC Adapter ................................. SUCCESS [ 15.558 s]
    [INFO] Arrow Plasma Client ................................ SUCCESS [  6.303 s]
    [INFO] Arrow Flight ....................................... SUCCESS [  2.326 s]
    [INFO] Arrow Flight Core .................................. SUCCESS [ 50.632 s]
    [INFO] Arrow Flight GRPC .................................. SUCCESS [  9.932 s]
    [INFO] Arrow Flight SQL ................................... SUCCESS [ 22.559 s]
    [INFO] Arrow Flight Integration Tests ..................... SUCCESS [  9.649 s]
    [INFO] Arrow AVRO Adapter ................................. SUCCESS [ 17.145 s]
    [INFO] Arrow Algorithms ................................... SUCCESS [ 44.571 s]
    [INFO] Arrow Performance Benchmarks ....................... SUCCESS [  9.202 s]
    [INFO] Arrow Java C Data Interface ........................ SUCCESS [ 15.794 s]
    [INFO] Arrow Orc Adapter .................................. SUCCESS [ 14.205 s]
    [INFO] Arrow Gandiva ...................................... SUCCESS [02:06 min]
    [INFO] Arrow Java Dataset ................................. SUCCESS [ 15.261 s]
    [INFO] ------------------------------------------------------------------------
    [INFO] BUILD SUCCESS
    [INFO] ------------------------------------------------------------------------

Performance Testing
===================

Java module that contains performance tests is: `performance`

Let's configure our performance environment to run performance tests:

- Install `benchmark`_
- Install `archery`_
- Install `conbench`_

Lets execute benchmark tests:

.. code-block::

    $ cd benchmarks
    $ conbench java-micro --help
    $ conbench java-micro
        --iterations=1
        --commit=e90472e35b40f58b17d408438bb8de1641bfe6ef
        --java-home=<absolute path to your java home>
        --src=<absolute path to your arrow project>
        --benchmark-filter=org.apache.arrow.adapter.AvroAdapterBenchmarks.testAvroToArrow
    Benchmark                              Mode  Cnt       Score   Error  Units
    AvroAdapterBenchmarks.testAvroToArrow  avgt       725545.783          ns/op
    Time to POST http://localhost:5000/api/login/ 0.14911699295043945
    Time to POST http://localhost:5000/api/benchmarks/ 0.06116318702697754

Then go to: http://127.0.0.1:5000/ to see reports:

UI Home:

.. image:: img/conbench_ui.png

UI Runs:

.. image:: img/conbench_runs.png

UI Benchmark:

.. image:: img/conbench_benchmark.png

Code Style
==========

Code style are configured at `java/dev/checkstyle`

.. code-block::

    <plugin>
        <groupId>org.apache.maven.plugins</groupId>
        <artifactId>maven-checkstyle-plugin</artifactId>
        <version>3.1.0</version>
        <executions>
            <execution>
                <id>validate</id>
                <phase>validate</phase>
                <goals>
                    <goal>check</goal>
                </goals>
            </execution>
        </executions>
        <configuration>
            <configLocation>dev/checkstyle/checkstyle.xml</configLocation>
            <headerLocation>dev/checkstyle/checkstyle.license</headerLocation>
            <suppressionsLocation>dev/checkstyle/suppressions.xml</suppressionsLocation>
            <includeTestSourceDirectory>true</includeTestSourceDirectory>
            ...
        </configuration>
    </plugin>

Validate check style configuration:

.. code-block::

    $ cd arrow/java
    $ mvn validate
    [INFO] ------------------------------------------------------------------------
    [INFO] Reactor Summary for Apache Arrow Java Root POM 7.0.0:
    [INFO]
    [INFO] Apache Arrow Java Root POM ......................... SUCCESS [  1.053 s]
    [INFO] Arrow Format ....................................... SUCCESS [  0.006 s]
    [INFO] Arrow Memory ....................................... SUCCESS [  0.088 s]
    [INFO] Arrow Memory - Core ................................ SUCCESS [  0.077 s]
    [INFO] Arrow Memory - Unsafe .............................. SUCCESS [  0.136 s]
    [INFO] Arrow Memory - Netty ............................... SUCCESS [  0.075 s]
    [INFO] Arrow Vectors ...................................... SUCCESS [  0.603 s]
    [INFO] Arrow Compression .................................. SUCCESS [  0.073 s]
    [INFO] Arrow Tools ........................................ SUCCESS [  0.068 s]
    [INFO] Arrow JDBC Adapter ................................. SUCCESS [  0.059 s]
    [INFO] Arrow Plasma Client ................................ SUCCESS [  0.052 s]
    [INFO] Arrow Flight ....................................... SUCCESS [  0.046 s]
    [INFO] Arrow Flight Core .................................. SUCCESS [  0.065 s]
    [INFO] Arrow Flight GRPC .................................. SUCCESS [  0.050 s]
    [INFO] Arrow Flight SQL ................................... SUCCESS [  0.054 s]
    [INFO] Arrow Flight Integration Tests ..................... SUCCESS [  0.046 s]
    [INFO] Arrow AVRO Adapter ................................. SUCCESS [  0.051 s]
    [INFO] Arrow Algorithms ................................... SUCCESS [  0.063 s]
    [INFO] Arrow Performance Benchmarks ....................... SUCCESS [  0.060 s]
    [INFO] ------------------------------------------------------------------------
    [INFO] BUILD SUCCESS
    [INFO] ------------------------------------------------------------------------

.. _benchmark: https://github.com/ursacomputing/benchmarks
.. _archery: https://github.com/apache/arrow/blob/master/dev/conbench_envs/README.md#L188
.. _conbench: https://github.com/conbench/conbench
