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
    with ``pyarrow`` correctly installed and a ``Java`` environment with
    ``arrow`` library correctly installed.
    The ``Arrow Java`` version must have been compiled with ``mvn -Parrow-c-data`` to
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

.. code-block:: console

    $ pip install jpype1

The most basic thing we can do with our ``Simple`` class is to
use the ``Simple.getNumber`` method from Python and see
if it will return the result.

To do so, we can create a ``simple.py`` file which uses ``jpype`` to
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

.. code-block:: console

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


    public class FillTen {
        static RootAllocator allocator = new RootAllocator();

        public static BigIntVector createArray() {
            BigIntVector intVector = new BigIntVector("ints", allocator);
            intVector.allocateNew(10);
            intVector.setValueCount(10);
            FillTen.fillVector(intVector);
            return intVector;
        }

        private static void fillVector(BigIntVector iv) {
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
a dedicated ``pom.xml`` file where we can collect the dependencies:

.. code-block:: xml

    <project>
        <modelVersion>4.0.0</modelVersion>

        <groupId>org.apache.arrow.py2java</groupId>
        <artifactId>FillTen</artifactId>
        <version>1</version>

        <properties>
            <maven.compiler.source>8</maven.compiler.source>
            <maven.compiler.target>8</maven.compiler.target>
        </properties>

        <dependencies>
            <dependency>
            <groupId>org.apache.arrow</groupId>
            <artifactId>arrow-memory</artifactId>
            <version>8.0.0</version>
            <type>pom</type>
            </dependency>
            <dependency>
            <groupId>org.apache.arrow</groupId>
            <artifactId>arrow-memory-netty</artifactId>
            <version>8.0.0</version>
            <type>jar</type>
            </dependency>
            <dependency>
            <groupId>org.apache.arrow</groupId>
            <artifactId>arrow-vector</artifactId>
            <version>8.0.0</version>
            <type>pom</type>
            </dependency>
            <dependency>
            <groupId>org.apache.arrow</groupId>
            <artifactId>arrow-c-data</artifactId>
            <version>8.0.0</version>
            <type>jar</type>
            </dependency>
        </dependencies>
    </project>

Once the ``FillTen.java`` file with the class is created
as ``src/main/java/FillTen.java`` we can use ``maven`` to
compile the project with ``mvn package`` and get it
available in the ``target`` directory.

.. code-block:: console

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
but that also its dependencies are.

We can use ``maven`` to collect all dependencies and make them available in a single place
(the ``dependencies`` directory) so that we can more easily load them from Python:

.. code-block:: console

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

.. note::

    Instead of manually collecting dependencies, you could also rely on the
    ``maven-assembly-plugin`` to build a single ``jar`` with all dependencies.

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

    # Convert the proxied BigIntVector to an actual pyarrow array
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

.. note::

    At the moment the ``pyarrow.jvm`` module is fairly limited in capabilities,
    nested types like structs are not supported and it only works on a JVM running
    within the same process like JPype.

Java to Python communication using the C Data Interface
-------------------------------------------------------

The C Data Interface is a protocol implemented in Arrow to exchange data within different
environments without the cost of marshaling and copying data.

This allows to expose data coming from Python or Java to functions that are implemented
in the other language.

.. note::

    In the future the ``pyarrow.jvm`` will be implemented to leverage the C Data
    interface, at the moment is instead specifically written for JPype

To showcase how C Data works, we are going to tweak a bit both our ``FillTen`` Java
class and our ``fillten.py`` Python script. Given a PyArrow array, we are going to
expose a function in Java that sets its content to by the numbers from 1 to 10.

Using C Data interface in ``pyarrow`` at the moment requires installing ``cffi``
explicitly, like most Python distributions it can be installed with

.. code-block:: console

    $ pip install cffi

The first thing we would have to do is to tweak the Python script so that it
sends to Java the exported references to the Array and its Schema according to the
C Data interface:

.. code-block:: python

    import jpype
    import jpype.imports
    from jpype.types import *

    # Init the JVM and make FillTen class available to Python.
    jpype.startJVM(classpath=["./dependencies/*", "./target/*"])
    FillTen = JClass('FillTen')

    # Create a Python array of 10 elements
    import pyarrow as pa
    array = pa.array([0]*10)

    from pyarrow.cffi import ffi as arrow_c

    # Export the Python array through C Data
    c_array = arrow_c.new("struct ArrowArray*")
    c_array_ptr = int(arrow_c.cast("uintptr_t", c_array))
    array._export_to_c(c_array_ptr)

    # Export the Schema of the Array through C Data
    c_schema = arrow_c.new("struct ArrowSchema*")
    c_schema_ptr = int(arrow_c.cast("uintptr_t", c_schema))
    array.type._export_to_c(c_schema_ptr)

    # Send Array and its Schema to the Java function
    # that will populate the array with numbers from 1 to 10
    FillTen.fillCArray(c_array_ptr, c_schema_ptr)

    # See how the content of our Python array was changed from Java
    # while it remained of the Python type.
    print("ARRAY", type(array), array)

.. note::

    Changing content of arrays is not a safe operation, it was done
    for the purpose of creating this example, and it mostly works only
    because the array hasn't changed size, type or nulls.

In the FillTen Java class, we already have the ``fillVector``
method, but that method is private and even if we made it public it
would only accept a ``BigIntVector`` object and not the C Data array
and schema references.

So we have to expand our ``FillTen`` class adding a ``fillCArray``
method that is able to perform the work of ``fillVector`` but
on the C Data exchanged entities instead of the ``BigIntVector`` one:

.. code-block:: java

    import org.apache.arrow.c.ArrowArray;
    import org.apache.arrow.c.ArrowSchema;
    import org.apache.arrow.c.Data;
    import org.apache.arrow.memory.RootAllocator;
    import org.apache.arrow.vector.FieldVector;
    import org.apache.arrow.vector.BigIntVector;


    public class FillTen {
        static RootAllocator allocator = new RootAllocator();

        public static void fillCArray(long c_array_ptr, long c_schema_ptr) {
            ArrowArray arrow_array = ArrowArray.wrap(c_array_ptr);
            ArrowSchema arrow_schema = ArrowSchema.wrap(c_schema_ptr);

            FieldVector v = Data.importVector(allocator, arrow_array, arrow_schema, null);
            FillTen.fillVector((BigIntVector)v);
        }

        private static void fillVector(BigIntVector iv) {
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

The goal of the ``fillCArray`` method is to get the Array and Schema received in
C Data exchange format and turn them back to an object of type ``FieldVector``
so that Arrow Java knows how to deal with it.

If we run again ``mvn package``, update the maven dependencies
and then our Python script, we should be able to see how the
values printed by the Python script have been properly changed by the Java code:

.. code-block:: console

    $ mvn package
    $ mvn org.apache.maven.plugins:maven-dependency-plugin:2.7:copy-dependencies -DoutputDirectory=dependencies
    $ python fillten.py
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

We can also use the C Stream Interface to exchange
:py:class:`pyarrow.RecordBatchReader` between Java and Python.  We'll
use this Java class as a demo, which lets you read an Arrow IPC file
via Java's implementation, or write data to a JSON file:

.. code-block:: java

   import java.io.File;
   import java.nio.file.Files;
   import java.nio.file.Paths;

   import org.apache.arrow.c.ArrowArrayStream;
   import org.apache.arrow.c.Data;
   import org.apache.arrow.memory.BufferAllocator;
   import org.apache.arrow.memory.RootAllocator;
   import org.apache.arrow.vector.ipc.ArrowFileReader;
   import org.apache.arrow.vector.ipc.ArrowReader;
   import org.apache.arrow.vector.ipc.JsonFileWriter;

   public class PythonInteropDemo implements AutoCloseable {
     private final BufferAllocator allocator;

     public PythonInteropDemo() {
       this.allocator = new RootAllocator();
     }

     public void exportStream(String path, long cStreamPointer) throws Exception {
       try (final ArrowArrayStream stream = ArrowArrayStream.wrap(cStreamPointer)) {
         ArrowFileReader reader = new ArrowFileReader(Files.newByteChannel(Paths.get(path)), allocator);
         Data.exportArrayStream(allocator, reader, stream);
       }
     }

     public void importStream(String path, long cStreamPointer) throws Exception {
       try (final ArrowArrayStream stream = ArrowArrayStream.wrap(cStreamPointer);
            final ArrowReader input = Data.importArrayStream(allocator, stream);
            JsonFileWriter writer = new JsonFileWriter(new File(path))) {
         writer.start(input.getVectorSchemaRoot().getSchema(), input);
         while (input.loadNextBatch()) {
           writer.write(input.getVectorSchemaRoot());
         }
       }
     }

     @Override
     public void close() throws Exception {
       allocator.close();
     }
   }

On the Python side, we'll use JPype as before, except this time we'll
send RecordBatchReaders back and forth:

.. code-block:: python

   import tempfile

   import jpype
   import jpype.imports
   from jpype.types import *

   # Init the JVM and make demo class available to Python.
   jpype.startJVM(classpath=["./dependencies/*", "./target/*"])
   PythonInteropDemo = JClass("PythonInteropDemo")
   demo = PythonInteropDemo()

   # Create a Python record batch reader
   import pyarrow as pa
   schema = pa.schema([
       ("ints", pa.int64()),
       ("strs", pa.string())
   ])
   batches = [
       pa.record_batch([
           [0, 2, 4, 8],
           ["a", "b", "c", None],
       ], schema=schema),
       pa.record_batch([
           [None, 32, 64, None],
           ["e", None, None, "h"],
       ], schema=schema),
   ]
   reader = pa.RecordBatchReader.from_batches(schema, batches)

   from pyarrow.cffi import ffi as arrow_c

   # Export the Python reader through C Data
   c_stream = arrow_c.new("struct ArrowArrayStream*")
   c_stream_ptr = int(arrow_c.cast("uintptr_t", c_stream))
   reader._export_to_c(c_stream_ptr)

   # Send reader to the Java function that writes a JSON file
   with tempfile.NamedTemporaryFile() as temp:
       demo.importStream(temp.name, c_stream_ptr)

       # Read the JSON file back
       with open(temp.name) as source:
           print("JSON file written by Java:")
           print(source.read())


   # Write an Arrow IPC file for Java to read
   with tempfile.NamedTemporaryFile() as temp:
       with pa.ipc.new_file(temp.name, schema) as sink:
           for batch in batches:
               sink.write_batch(batch)

       demo.exportStream(temp.name, c_stream_ptr)
       with pa.RecordBatchReader._import_from_c(c_stream_ptr) as source:
           print("IPC file read by Java:")
           print(source.read_all())

.. code-block:: console

   $ mvn package
   $ mvn org.apache.maven.plugins:maven-dependency-plugin:2.7:copy-dependencies -DoutputDirectory=dependencies
   $ python demo.py
   JSON file written by Java:
   {"schema":{"fields":[{"name":"ints","nullable":true,"type":{"name":"int","bitWidth":64,"isSigned":true},"children":[]},{"name":"strs","nullable":true,"type":{"name":"utf8"},"children":[]}]},"batches":[{"count":4,"columns":[{"name":"ints","count":4,"VALIDITY":[1,1,1,1],"DATA":["0","2","4","8"]},{"name":"strs","count":4,"VALIDITY":[1,1,1,0],"OFFSET":[0,1,2,3,3],"DATA":["a","b","c",""]}]},{"count":4,"columns":[{"name":"ints","count":4,"VALIDITY":[0,1,1,0],"DATA":["0","32","64","0"]},{"name":"strs","count":4,"VALIDITY":[1,0,0,1],"OFFSET":[0,1,1,1,2],"DATA":["e","","","h"]}]}]}
   IPC file read by Java:
   pyarrow.Table
   ints: int64
   strs: string
   ----
   ints: [[0,2,4,8],[null,32,64,null]]
   strs: [["a","b","c",null],["e",null,null,"h"]]
