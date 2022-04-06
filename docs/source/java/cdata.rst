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

Arrow supports exchanging data without copying or serialization within the same process through the :ref:`c-data-interface`, even between different language runtimes.

Java to Python
--------------

Use this guide to implement :doc:`Java to Python <../python/integration/python_java.rst>`
communication using the C Data Interface.

Java to C++
-----------

The C Data Interface is a protocol implemented in Arrow to exchange data within different
environments without the cost of marshaling and copying data.

Example: share an Int32 array from C++ to Java:

**C++ Side**

Compile arrow minimal library:

.. code-block:: shell

    git clone https://github.com/apache/arrow.git
    cd arrow/cpp
    cmake --preset -N ninja-debug-minimal
    mkdir build   # from inside the `cpp` subdirectory
    cd build
    cmake .. --preset ninja-debug-minimal
    cmake --build .
    tree debug/
    debug/
    ├── libarrow.800.0.0.dylib
    ├── libarrow.800.dylib -> libarrow.800.0.0.dylib
    └── libarrow.dylib -> libarrow.800.dylib

Define c++ code CDataInterfaceLibrary.h that export functions for third party like Java consumer:

.. code-block:: cpp

    #include <iostream>
    #include <arrow/api.h>

    #define ARROW_FLAG_DICTIONARY_ORDERED 1
    #define ARROW_FLAG_NULLABLE 2
    #define ARROW_FLAG_MAP_KEYS_SORTED 4

    using arrow::Int64Builder;

    struct ArrowSchema {
        // Array type description
        const char* format;
        const char* name;
        const char* metadata;
        int64_t flags;
        int64_t n_children;
        struct ArrowSchema** children;
        struct ArrowSchema* dictionary;

        // Release callback
        void (*release)(struct ArrowSchema*);
        // Opaque producer-specific data
        void* private_data;
    };

    struct ArrowArray {
        // Array data description
        int64_t length;
        int64_t null_count;
        int64_t offset;
        int64_t n_buffers;
        int64_t n_children;
        const void** buffers;
        struct ArrowArray** children;
        struct ArrowArray* dictionary;

        // Release callback
        void (*release)(struct ArrowArray*);
        // Opaque producer-specific data
        void* private_data;
    };

    static void release_int32_type(struct ArrowSchema* schema) {
        // Mark released
        schema->release = NULL;
    }

    void export_int32_type(struct ArrowSchema* schema) {
        *schema = (struct ArrowSchema) {
                // Type description
                .format = "l",
                .name = "",
                .metadata = NULL,
                .flags = 0,
                .n_children = 0,
                .children = NULL,
                .dictionary = NULL,
                // Bookkeeping
                .release = &release_int32_type
        };
        std::cout << "C Data - Schema Pointer = " << schema << std::endl;
    }

    static void release_int32_array(struct ArrowArray* array) {
        assert(array->n_buffers == 2);
        // Free the buffers and the buffers array
        free((void *) array->buffers[1]);
        free(array->buffers);
        // Mark released
        array->release = NULL;
    }

    void export_int32_array(struct ArrowArray* array) {
        arrow::Int64Builder builder;
        builder.Append(1);
        builder.Append(2);
        builder.Append(3);
        builder.AppendNull();
        builder.Append(5);
        builder.Append(6);
        builder.Append(7);
        builder.Append(8);

        auto maybe_array = builder.Finish();
        std::shared_ptr<arrow::Array> array_arrow = *maybe_array;
        auto int64_array = std::static_pointer_cast<arrow::Int64Array>(array_arrow);
        const int64_t* data = int64_array->raw_values();
        std::cout << "Data To Exchange Pointer = " << data << std::endl;
        for (int j = 0; j < int64_array->length(); j++){
            std::cout << "Data To Exchange Value[" << j << "] = " << data[j] << std::endl;
        }

        *array = (struct ArrowArray) {
                // Data description
                .length = int64_array->length(),
                .offset = 0,
                .null_count = 0,
                .n_buffers = 2,
                .n_children = 0,
                .children = NULL,
                .dictionary = NULL,
                // Bookkeeping
                .release = &release_int32_array
        };

        // Allocate list of buffers
        array->buffers = (const void**) malloc(sizeof(void*) * array->n_buffers);
        assert(array->buffers != NULL);
        array->buffers[0] = NULL;  // no nulls, null bitmap can be omitted
        array->buffers[1] = data;

        std::cout << "C Data - Array Pointer = " << array << std::endl;
        std::cout << "C Data - Array Data Pointer Buffer array->buffers[1] = " << array->buffers[1] << std::endl;
    }

**Java Side**

Define Java code CDataInterfaceLibraryConfig.java that consume by JNI C++ functions exported through
C Data Interface:

.. code-block:: xml

    <?xml version="1.0" encoding="UTF-8"?>
    <project xmlns="http://maven.apache.org/POM/4.0.0"
             xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
             xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
        <modelVersion>4.0.0</modelVersion>
        <groupId>org.example</groupId>
        <artifactId>cpp-java-cdata</artifactId>
        <version>1.0-SNAPSHOT</version>
        <properties>
            <maven.compiler.source>8</maven.compiler.source>
            <maven.compiler.target>8</maven.compiler.target>
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
                <version>7.0.0</version>
            </dependency>
            <dependency>
                <groupId>org.apache.arrow</groupId>
                <artifactId>arrow-memory-netty</artifactId>
                <version>7.0.0</version>
            </dependency>
        </dependencies>
    </project>

.. code-block:: java

    import org.bytedeco.javacpp.annotation.Platform;
    import org.bytedeco.javacpp.annotation.Properties;
    import org.bytedeco.javacpp.tools.InfoMap;
    import org.bytedeco.javacpp.tools.InfoMapper;

    @Properties(
            target = "CDataInterfaceLibrary",
            value = @Platform(
                    include = {"CDataInterfaceLibrary.h"},
                    compiler = {"cpp11"},
                    linkpath = {"/arrow/cpp/build/debug/"},
                    link = {"arrow"}
            )
    )
    public class CDataInterfaceLibraryConfig implements InfoMapper {
        @Override
        public void map(InfoMap infoMap) {
        }
    }

.. code-block:: shell

    // Compile our Java code
    javac -cp javacpp-1.5.7.jar CDataInterfaceLibraryConfig.java

    // Generate CDataInterfaceLibrary
    java -jar javacpp-1.5.7.jar CDataInterfaceLibraryConfig.java

    // Generate libjniCDataInterfaceLibrary.dylib
    java -jar javacpp-1.5.7.jar CDataInterfaceLibrary.java

    // Validate libjniCDataInterfaceLibrary.dylib created
    otool -L macosx-x86_64/libjniCDataInterfaceLibrary.dylib
    macosx-x86_64/libjniCDataInterfaceLibrary.dylib:
            libjniCDataInterfaceLibrary.dylib (compatibility version 0.0.0, current version 0.0.0)
            @rpath/libarrow.800.dylib (compatibility version 800.0.0, current version 800.0.0)
            /usr/lib/libc++.1.dylib (compatibility version 1.0.0, current version 1200.3.0)
            /usr/lib/libSystem.B.dylib (compatibility version 1.0.0, current version 1311.0.0)

**Java Test**

Let's create a Java class to Test C Data Interface from Java to C++:

.. code-block:: java

    import org.apache.arrow.c.ArrowArray;
    import org.apache.arrow.c.ArrowSchema;
    import org.apache.arrow.c.Data;
    import org.apache.arrow.memory.BufferAllocator;
    import org.apache.arrow.memory.RootAllocator;
    import org.apache.arrow.vector.BigIntVector;

    public class TestCDataInterfaceV5 {

        public static void main(String[] args) {
            CDataInterfaceLibrary.ArrowSchema arrowSchema = new CDataInterfaceLibrary.ArrowSchema();
            CDataInterfaceLibrary.export_int32_type(arrowSchema);

            ArrowSchema arrow_schema = ArrowSchema.wrap(arrowSchema.address());
            System.out.println("Java C Data - Schema Pointer = = " + Long.toHexString(arrowSchema.address()));

            CDataInterfaceLibrary.ArrowArray arrowArray = new CDataInterfaceLibrary.ArrowArray();
            CDataInterfaceLibrary.export_int32_array(arrowArray);

            ArrowArray arrow_array = ArrowArray.wrap(arrowArray.address());
            System.out.println("Java C Data - Array Pointer = " + Long.toHexString(arrowArray.address()));
            System.out.println("Java C Data - Array Data Pointer Buffer array->buffers[1] = " + Long.toHexString(arrowArray.buffers(1).address()));

            BufferAllocator allocator = new RootAllocator();
            BigIntVector bigIntVector = (BigIntVector) Data.importVector(allocator, arrow_array, arrow_schema, null);
            System.out.println("Java C Data - BigIntVector: " + bigIntVector);

            CDataInterfaceLibrary.release_int32_type(arrowSchema);
            CDataInterfaceLibrary.release_int32_array(arrowArray);
        }
    }

.. code-block:: shell

    C Data - Schema Pointer = 0x7fba97f36070
    Java C Data - Schema Pointer = = 7fba97f36070
    Data To Exchange Pointer = 0x10ec0d040
    Data To Exchange Value[0] = 1
    Data To Exchange Value[1] = 2
    Data To Exchange Value[2] = 3
    Data To Exchange Value[3] = 0
    Data To Exchange Value[4] = 5
    Data To Exchange Value[5] = 6
    Data To Exchange Value[6] = 7
    Data To Exchange Value[7] = 8
    C Data - Array Pointer = 0x7fba67d0d380
    C Data - Array Data Pointer Buffer array->buffers[1] = 0x10ec0d040
    Java C Data - Array Pointer = 7fba67d0d380
    Java C Data - Array Data Pointer Buffer array->buffers[1] = 10ec0d040
    Java C Data - BigIntVector: [6510615555426900570, 6510615555426900570, 6510615555426900570, 6510615555426900570, 6510615555426900570, 6510615555426900570, 6510615555426900570, 6510615555426900570]

