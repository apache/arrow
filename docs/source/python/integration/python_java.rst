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

Integrating PyArrow with Java
=============================

Arrow supports exchanging data within the same process through the
:ref:`c-data-interface`.

This can be used to exchange data between Python and Java functions and
methods so that the two languages can interact without any cost of
marshaling and unmarshaling data.

.. note::

    The article takes for granted that you have a ``Python`` environment
    with ``pyarrow`` correctly installed and an ``Java`` environment with
    ``arrow`` library correctly installed. 
    The ``Java`` version must have been compiled with ``mvn -Parrow-c-data`` to
    ensure CData exchange support is enabled.
    See `Python Install Instructions <https://arrow.apache.org/docs/python/install.html>`_
    and `Java Documentation <https://arrow.apache.org/docs/java/>`_
    for further details.

Invoking Java methods from Python
---------------------------------

Suppose we have a simple Java class providing a number as its output:

.. code-block:: Java

    public class Simple {
        public static int getNumber() {
            return 4;
        }
    }

We would save such class in the ``Simple.java`` file and proceed with
compiling it to ``Simple.class`` using ``javac Simple.java``.

Once the ``Simple.class`` file is created we can use the class
from Python using the 
`JPype <https://jpype.readthedocs.io/>`_ library which
enables a Java runtime within the Python interpreter.

``jpype1`` can be installed using ``pip`` like most Python libraries

.. code-block:: bash

    $ pip install jpype1

The most basic thing we can do with our ``Simple`` class is to
use the ``Simple.getNumber`` method from Python and see 
if it will return the result.

To do so we can create an ``simple.py`` file which uses ``jpype`` to
import the ``Simple`` class from ``Simple.class`` file and invoke 
the ``Simple.getNumber`` method:

.. code-block:: python

    import jpype
    from jpype.types import *

    jpype.startJVM(classpath=["./"])

    Simple = JClass('Simple')

    print(Simple.getNumber())

Running the ``simple.py`` file will show how our Python code is able
to access the ``Java`` method and print the expected result:

.. code-block:: bash

    $ python simple.py 
    4

Java to Python using pyarrow.jvm
--------------------------------

PyArrow provides a ``pyarrow.jvm`` module that makes easier to
interact with Java classes and convert the Java objects to actual
Python objects.

To showcase ``pyarrow.jvm`` we could create a more complex
class, named ``FillTen.java``

.. code-block:: java

    import org.apache.arrow.memory.RootAllocator;
    import org.apache.arrow.vector.BigIntVector;
    import org.apache.arrow.vector.ValueVector;


    public class FillTen {
        static RootAllocator allocator = new RootAllocator();

        public static ValueVector createArray() {
            BigIntVector intVector = new BigIntVector("ints", allocator);
            intVector.allocateNew(10);
            intVector.setValueCount(10);
            FillTen.fillValueVector(intVector);
            return intVector;
        }

        private static void fillValueVector(ValueVector v) {
            BigIntVector iv = (BigIntVector)v;
            iv.setSafe(0, 1);
            iv.setSafe(1, 2);
            iv.setSafe(2, 3);
            iv.setSafe(3, 4);
            iv.setSafe(4, 5);
            iv.setSafe(5, 6);
            iv.setSafe(6, 7);
            iv.setSafe(7, 8);
            iv.setSafe(8, 9);
            iv.setSafe(9, 10);
        }
    }

This class provides a public ``createArray`` method that anyone can invoke
to get back an array containing numbers from 1 to 10. 

Given that this class now has a dependency on a bunch of packages,
compiling it with ``javac`` is not enough anymore. We need to create
a dedicated ``pom.xml`` file were we can collect the dependencies:

.. code-block:: xml

    <project>
        <modelVersion>4.0.0</modelVersion>
        
        <groupId>org.apache.arrow.py2java</groupId>
        <artifactId>FillTen</artifactId>
        <version>1</version>

        <properties>
            <maven.compiler.source>1.7</maven.compiler.source>
            <maven.compiler.target>1.7</maven.compiler.target>
        </properties> 

        <dependencies>
            <dependency>
            <groupId>org.apache.arrow</groupId>
            <artifactId>arrow-memory</artifactId>
            <version>8.0.0-SNAPSHOT</version>
            <type>pom</type>
            </dependency>
            <dependency>
            <groupId>org.apache.arrow</groupId>
            <artifactId>arrow-memory-netty</artifactId>
            <version>8.0.0-SNAPSHOT</version>
            <type>jar</type>
            </dependency>
            <dependency>
            <groupId>org.apache.arrow</groupId>
            <artifactId>arrow-vector</artifactId>
            <version>8.0.0-SNAPSHOT</version>
            <type>pom</type>
            </dependency> 
            <dependency>
            <groupId>org.apache.arrow</groupId>
            <artifactId>arrow-c-data</artifactId>
            <version>8.0.0-SNAPSHOT</version>
            <type>jar</type>
            </dependency>
        </dependencies>
    </project>

Once the ``FillTen.java`` file with the class is created
as ``src/main/java/FillTen.java`` we can use ``maven`` to
compile the project with ``mvn package`` and get it 
available in the ``target`` directory.

.. code-block:: bash

    $ mvn package
    [INFO] Scanning for projects...
    [INFO] 
    [INFO] ------------------< org.apache.arrow.py2java:FillTen >------------------
    [INFO] Building FillTen 1
    [INFO] --------------------------------[ jar ]---------------------------------
    [INFO] 
    [INFO] --- maven-compiler-plugin:3.1:compile (default-compile) @ FillTen ---
    [INFO] Changes detected - recompiling the module!
    [INFO] Compiling 1 source file to /experiments/java2py/target/classes
    [INFO] 
    [INFO] --- maven-jar-plugin:2.4:jar (default-jar) @ FillTen ---
    [INFO] Building jar: /experiments/java2py/target/FillTen-1.jar
    [INFO] ------------------------------------------------------------------------
    [INFO] BUILD SUCCESS
    [INFO] ------------------------------------------------------------------------

Now that we have the package built, we can make it available to Python.
To do so, we need to make sure that not only the package itself is available,
but that also its dependencies are too.

We can use ``maven`` to collect all dependencies and make them available in a single place
(the ``dependencies`` directory) so that we can more easily load them from Python:

.. code-block:: bash

    $ mvn org.apache.maven.plugins:maven-dependency-plugin:2.7:copy-dependencies -DoutputDirectory=dependencies
    [INFO] Scanning for projects...
    [INFO] 
    [INFO] ------------------< org.apache.arrow.py2java:FillTen >------------------
    [INFO] Building FillTen 1
    [INFO] --------------------------------[ jar ]---------------------------------
    [INFO] 
    [INFO] --- maven-dependency-plugin:2.7:copy-dependencies (default-cli) @ FillTen ---
    [INFO] Copying jsr305-3.0.2.jar to /experiments/java2py/dependencies/jsr305-3.0.2.jar
    [INFO] Copying netty-common-4.1.72.Final.jar to /experiments/java2py/dependencies/netty-common-4.1.72.Final.jar
    [INFO] Copying arrow-memory-core-8.0.0-SNAPSHOT.jar to /experiments/java2py/dependencies/arrow-memory-core-8.0.0-SNAPSHOT.jar
    [INFO] Copying arrow-vector-8.0.0-SNAPSHOT.jar to /experiments/java2py/dependencies/arrow-vector-8.0.0-SNAPSHOT.jar
    [INFO] Copying arrow-c-data-8.0.0-SNAPSHOT.jar to /experiments/java2py/dependencies/arrow-c-data-8.0.0-SNAPSHOT.jar
    [INFO] Copying arrow-vector-8.0.0-SNAPSHOT.pom to /experiments/java2py/dependencies/arrow-vector-8.0.0-SNAPSHOT.pom
    [INFO] Copying jackson-core-2.11.4.jar to /experiments/java2py/dependencies/jackson-core-2.11.4.jar
    [INFO] Copying jackson-annotations-2.11.4.jar to /experiments/java2py/dependencies/jackson-annotations-2.11.4.jar
    [INFO] Copying slf4j-api-1.7.25.jar to /experiments/java2py/dependencies/slf4j-api-1.7.25.jar
    [INFO] Copying arrow-memory-netty-8.0.0-SNAPSHOT.jar to /experiments/java2py/dependencies/arrow-memory-netty-8.0.0-SNAPSHOT.jar
    [INFO] Copying arrow-format-8.0.0-SNAPSHOT.jar to /experiments/java2py/dependencies/arrow-format-8.0.0-SNAPSHOT.jar
    [INFO] Copying flatbuffers-java-1.12.0.jar to /experiments/java2py/dependencies/flatbuffers-java-1.12.0.jar
    [INFO] Copying arrow-memory-8.0.0-SNAPSHOT.pom to /experiments/java2py/dependencies/arrow-memory-8.0.0-SNAPSHOT.pom
    [INFO] Copying netty-buffer-4.1.72.Final.jar to /experiments/java2py/dependencies/netty-buffer-4.1.72.Final.jar
    [INFO] Copying jackson-databind-2.11.4.jar to /experiments/java2py/dependencies/jackson-databind-2.11.4.jar
    [INFO] Copying commons-codec-1.10.jar to /experiments/java2py/dependencies/commons-codec-1.10.jar
    [INFO] ------------------------------------------------------------------------
    [INFO] BUILD SUCCESS
    [INFO] ------------------------------------------------------------------------

Once our package and all its depdendencies are available, 
we can invoke it from ``fillten_pyarrowjvm.py`` script that will
import the ``FillTen`` class and print out the result of invoking ``FillTen.createArray`` 

.. code-block:: python

    import jpype
    import jpype.imports
    from jpype.types import *

    # Start a JVM making available all dependencies we collected
    # and our class from target/FillTen-1.jar
    jpype.startJVM(classpath=["./dependencies/*", "./target/*"])

    FillTen = JClass('FillTen')

    array = FillTen.createArray()
    print("ARRAY", type(array), array)

    # Convert the proxied ValueVector to an actual pyarrow array
    import pyarrow.jvm
    pyarray = pyarrow.jvm.array(array)
    print("ARRAY", type(pyarray), pyarray)
    del pyarray

Running the python script will lead to two lines getting printed:

.. code-block::

    ARRAY <java class 'org.apache.arrow.vector.BigIntVector'> [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    ARRAY <class 'pyarrow.lib.Int64Array'> [
        1,
        2,
        3,
        4,
        5,
        6,
        7,
        8,
        9,
        10
    ]

The first line is the raw result of invoking the ``FillTen.createArray`` method.
The resulting object is a proxy to the actual Java object, so it's not really a pyarrow
Array, it will lack most of its capabilities and methods. 
That's why we subsequently use ``pyarrow.jvm.array`` to convert it to an actual
``pyarrow`` array. That allows us to treat it like any other ``pyarrow`` array.
The result is the second line in the output where the array is correctly reported
as being of type ``pyarrow.lib.Int64Array`` and is printed using the ``pyarrow`` style.

Java to Python communication using the C Data Interface
-------------------------------------------------------


