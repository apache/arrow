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

================
C Data Interface
================

.. contents::

Arrow supports exchanging data without copying or serialization within the same process
through the :ref:`c-data-interface`, even between different language runtimes.

Java to Python
--------------

Use this guide to implement :doc:`Java to Python <../python/integration/python_java.rst>`
communication using the C Data Interface.

Java to C++
-----------

Example: Share an Int64 array from C++ to Java:

**C++ Side**

Use this guide to :doc:`compile arrow <../developers/cpp/building.rst>` library:

.. code-block:: shell

    $ git clone https://github.com/apache/arrow.git
    $ cd arrow/cpp
    $ mkdir build   # from inside the `cpp` subdirectory
    $ cd build
    $ cmake .. --preset ninja-debug-minimal
    $ cmake --build .
    $ tree debug/
    debug/
    ├── libarrow.800.0.0.dylib
    ├── libarrow.800.dylib -> libarrow.800.0.0.dylib
    └── libarrow.dylib -> libarrow.800.dylib

Implement a function in CDataCppBridge.h that exports an array via the C Data Interface:

.. code-block:: cpp

    #include <iostream>
    #include <arrow/api.h>
    #include <arrow/c/bridge.h>

    void FillInt64Array(const uintptr_t c_schema_ptr, const uintptr_t c_array_ptr) {
        arrow::Int64Builder builder;
        builder.Append(1);
        builder.Append(2);
        builder.Append(3);
        builder.AppendNull();
        builder.Append(5);
        builder.Append(6);
        builder.Append(7);
        builder.Append(8);
        builder.Append(9);
        builder.Append(10);
        std::shared_ptr<arrow::Array> array = *builder.Finish();

        struct ArrowSchema* c_schema = reinterpret_cast<struct ArrowSchema*>(c_schema_ptr);
        auto c_schema_status = arrow::ExportType(*array->type(), c_schema);
        if (!c_schema_status.ok()) c_schema_status.Abort();

        struct ArrowArray* c_array = reinterpret_cast<struct ArrowArray*>(c_array_ptr);
        auto c_array_status = arrow::ExportArray(*array, c_array);
        if (!c_array_status.ok()) c_array_status.Abort();
    }

**Java Side**

For this example, we will use `JavaCPP`_ to call our C++ function from Java,
without writing JNI bindings ourselves.

.. code-block:: xml

    <?xml version="1.0" encoding="UTF-8"?>
    <project xmlns="http://maven.apache.org/POM/4.0.0"
             xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
             xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
        <modelVersion>4.0.0</modelVersion>

        <groupId>org.example</groupId>
        <artifactId>java-cdata-example</artifactId>
        <version>1.0-SNAPSHOT</version>

        <properties>
            <maven.compiler.source>8</maven.compiler.source>
            <maven.compiler.target>8</maven.compiler.target>
            <arrow.version>8.0.0</arrow.version>
        </properties>
        <dependencies>
            <dependency>
                <groupId>org.bytedeco</groupId>
                <artifactId>javacpp</artifactId>
                <version>1.5.7</version>
            </dependency>
            <dependency>
                <groupId>org.apache.arrow</groupId>
                <artifactId>arrow-c-data</artifactId>
                <version>${arrow.version}</version>
            </dependency>
            <dependency>
                <groupId>org.apache.arrow</groupId>
                <artifactId>arrow-vector</artifactId>
                <version>${arrow.version}</version>
            </dependency>
            <dependency>
                <groupId>org.apache.arrow</groupId>
                <artifactId>arrow-memory-core</artifactId>
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

.. code-block:: java

    import org.bytedeco.javacpp.annotation.Platform;
    import org.bytedeco.javacpp.annotation.Properties;
    import org.bytedeco.javacpp.tools.InfoMap;
    import org.bytedeco.javacpp.tools.InfoMapper;

    @Properties(
            target = "CDataJavaToCppExample",
            value = @Platform(
                    include = {
                            "CDataCppBridge.h"
                    },
                    compiler = {"cpp11"},
                    linkpath = {"/arrow/cpp/build/debug/"},
                    link = {"arrow"}
            )
    )
    public class CDataJavaConfig implements InfoMapper {

        @Override
        public void map(InfoMap infoMap) {
        }
    }

.. code-block:: shell

    # Compile our Java code
    $ javac -cp javacpp-1.5.7.jar CDataJavaConfig.java

    # Generate CDataInterfaceLibrary
    $ java -jar javacpp-1.5.7.jar CDataJavaConfig.java

    # Generate libjniCDataInterfaceLibrary.dylib
    $ java -jar javacpp-1.5.7.jar CDataJavaToCppExample.java

    # Validate libjniCDataInterfaceLibrary.dylib created
    $ otool -L macosx-x86_64/libjniCDataJavaToCppExample.dylib
    macosx-x86_64/libjniCDataJavaToCppExample.dylib:
        libjniCDataJavaToCppExample.dylib (compatibility version 0.0.0, current version 0.0.0)
        @rpath/libarrow.800.dylib (compatibility version 800.0.0, current version 800.0.0)
        /usr/lib/libc++.1.dylib (compatibility version 1.0.0, current version 1200.3.0)
        /usr/lib/libSystem.B.dylib (compatibility version 1.0.0, current version 1311.0.0)

**Java Test**

Let's create a Java class to test our bridge:

.. code-block:: java

    import org.apache.arrow.c.ArrowArray;
    import org.apache.arrow.c.ArrowSchema;
    import org.apache.arrow.c.Data;
    import org.apache.arrow.memory.BufferAllocator;
    import org.apache.arrow.memory.RootAllocator;
    import org.apache.arrow.vector.BigIntVector;

    public class TestCDataInterface {
        public static void main(String[] args) {
            try(
                BufferAllocator allocator = new RootAllocator();
                ArrowSchema arrowSchema = ArrowSchema.allocateNew(allocator);
                ArrowArray arrowArray = ArrowArray.allocateNew(allocator)
            ){
                CDataJavaToCppExample.FillInt64Array(
                        arrowSchema.memoryAddress(), arrowArray.memoryAddress());
                try(
                    BigIntVector bigIntVector = (BigIntVector) Data.importVector(
                            allocator, arrowArray, arrowSchema, null)
                ){
                    System.out.println("C++-allocated array: " + bigIntVector);
                }
            }
        }
    }

.. code-block:: shell

    C++-allocated array: [1, 2, 3, null, 5, 6, 7, 8, 9, 10]

.. _`JavaCPP`: https://github.com/bytedeco/javacpp