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

Arrow supports exchanging data within the same process through the :ref:`c-data-interface`.

Java to Python
--------------

Use this guide to implement :doc:`Java to Python <../python/integration/python_java.rst>`
communication using the C Data Interface.

Java to C++
-----------

The C Data Interface is a protocol implemented in Arrow to exchange data within different
environments without the cost of marshaling and copying data.

Example:
How to expose an int32 array using C Data Interface?

Let's review all steps needed to expose an int32 array using C Data Interface:

1st: Define c++ code CDataInterfaceLibrary.h that export functions for third party like Java consumer:

.. code-block:: cpp

    #include <iostream>
    #include <arrow/api.h>

    #define ARROW_FLAG_DICTIONARY_ORDERED 1
    #define ARROW_FLAG_NULLABLE 2
    #define ARROW_FLAG_MAP_KEYS_SORTED 4

    using arrow::Int64Builder;
    using arrow::Int32Builder;

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
                .format = "i",
                .name = "",
                .metadata = NULL,
                .flags = 0,
                .n_children = 0,
                .children = NULL,
                .dictionary = NULL,
                // Bookkeeping
                .release = &release_int32_type
        };
    }

    static void release_int32_array(struct ArrowArray* array) {
        assert(array->n_buffers == 2);
        // Free the buffers and the buffers array
        free((void *) array->buffers[1]);
        free(array->buffers);
        // Mark released
        array->release = NULL;
    }

    void export_int32_array(const int32_t* data, int64_t nitems,
                            struct ArrowArray* array) {
        // Initialize primitive fields
        *array = (struct ArrowArray) {
                // Data description
                .length = nitems,
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
    }

    int main() {
        std::cout << "Hello, World" << std::endl;
        std::cout << "Create int32*" << std::endl;
        arrow::Int32Builder builder;
        builder.Append(1);
        builder.Append(2);
        builder.Append(3);
        builder.Append(4);
        builder.Append(5);
        builder.Append(6);
        builder.Append(7);
        builder.Append(8);
        builder.Append(9);
        builder.Append(10);
        auto maybe_array = builder.Finish();
        std::shared_ptr<arrow::Array> arrayArrow = *maybe_array;
        auto int32_array = std::static_pointer_cast<arrow::Int32Array>(arrayArrow);
        const int32_t* data = reinterpret_cast<const int32_t *>(int32_array->raw_values());
        std::cout << "int32* data pointer = " << data << std::endl;


        // This is an example about what are the steps that is going to be executed by java program

        struct ArrowSchema* schema;
        struct ArrowArray* array;

        export_int32_type(schema);
        std::cout << "Schema pointer = " << schema << std::endl;

        export_int32_array(data, 10, array);
        std::cout << "Array pointer = " << array << std::endl;
        std::cout << "Array pointer int32* data = " << array->buffers[1] << std::endl;

        release_int32_type(schema);
        release_int32_array(array);

        // recreate array with: IntVector fieldVector = (IntVector) Data.importVector(allocator, arrow_array, arrow_schema, null);

        return 0;
    }

2nd: Define Java code CDataInterfaceLibraryConfig.java that consume by JNI C++ functions exported through
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
                    linkpath = {"/arrow/cpp/build/"},
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

3rd: Let's create a Java class to Test C Data Interface from Java to C++:

.. code-block:: java

    import org.apache.arrow.c.ArrowArray;
    import org.apache.arrow.c.ArrowSchema;
    import org.apache.arrow.c.Data;
    import org.apache.arrow.memory.BufferAllocator;
    import org.apache.arrow.memory.RootAllocator;
    import org.apache.arrow.vector.IntVector;

    public class TestCDataInterfaceJavaToCpp {

        public static void main(String[] args) {
            CDataInterfaceLibrary.NativeClass nativeCData = new CDataInterfaceLibrary.NativeClass();
            CDataInterfaceLibrary.NativeClass.ArrowSchema arrowSchema = new CDataInterfaceLibrary.NativeClass.ArrowSchema();
            nativeCData.export_int32_type(arrowSchema);
            ArrowSchema arrow_schema = ArrowSchema.wrap(arrowSchema.address());
            CDataInterfaceLibrary.NativeClass.ArrowArray arrowArray = new CDataInterfaceLibrary.NativeClass.ArrowArray();
            nativeCData.export_int32_array(arrowArray);
            ArrowArray arrow_array = ArrowArray.wrap(arrowArray.address());
            BufferAllocator allocator = new RootAllocator();
            IntVector fieldVector = (IntVector) Data.importVector(allocator, arrow_array, arrow_schema, null);
            System.out.println("Int 32 Vector: " + fieldVector);
        }
    }

.. code-block:: shell

    Data pointer = 0x108f8b040
    Value Index 2 = 3
    Int 32 Vector: [1515870810, 1515870810, 1515870810, 1515870810, 1515870810, 1515870810, 1515870810, 1515870810, 1515870810, 1515870810]

