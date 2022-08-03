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

See :doc:`../python/integration/python_java` to implement Java to
Python communication using the C Data Interface.

Java to C++
-----------

Share an Int64 array from C++ to Java
=====================================

**C++ Side**

See :doc:`../developers/cpp/building` to build the Arrow C++ libraries:

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

Share an Int32 array from Java to C++
=====================================

**Java Side**

For this example, we will build a JAR with all dependencies bundled.

.. code-block:: xml

    <?xml version="1.0" encoding="UTF-8"?>
    <project xmlns="http://maven.apache.org/POM/4.0.0"
             xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
             xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
        <modelVersion>4.0.0</modelVersion>
        <groupId>org.example</groupId>
        <artifactId>cpptojava</artifactId>
        <version>1.0-SNAPSHOT</version>
        <properties>
            <maven.compiler.source>11</maven.compiler.source>
            <maven.compiler.target>11</maven.compiler.target>
            <arrow.version>8.0.0</arrow.version>
        </properties>
        <repositories>
            <repository>
                <id>arrow-nightly</id>
                <url>https://nightlies.apache.org/arrow/java</url>
            </repository>
        </repositories>
        <dependencies>
            <dependency>
                <groupId>org.apache.arrow</groupId>
                <artifactId>arrow-c-data</artifactId>
                <version>${arrow.version}</version>
            </dependency>
            <dependency>
                <groupId>org.apache.arrow</groupId>
                <artifactId>arrow-memory-netty</artifactId>
                <version>${arrow.version}</version>
            </dependency>
        </dependencies>
        <build>
            <plugins>
                <plugin>
                    <groupId>org.apache.maven.plugins</groupId>
                    <artifactId>maven-assembly-plugin</artifactId>
                    <executions>
                        <execution>
                            <phase>package</phase>
                            <goals>
                                <goal>single</goal>
                            </goals>
                            <configuration>
                                <archive>
                                    <manifest>
                                        <mainClass>
                                            ToBeCalledByCpp
                                        </mainClass>
                                    </manifest>
                                </archive>
                                <descriptorRefs>
                                    <descriptorRef>jar-with-dependencies</descriptorRef>
                                </descriptorRefs>
                            </configuration>
                        </execution>
                    </executions>
                </plugin>
            </plugins>
        </build>
    </project>

.. code-block:: java

    import org.apache.arrow.c.ArrowArray;
    import org.apache.arrow.c.ArrowSchema;
    import org.apache.arrow.c.Data;
    import org.apache.arrow.memory.BufferAllocator;
    import org.apache.arrow.memory.RootAllocator;
    import org.apache.arrow.vector.FieldVector;
    import org.apache.arrow.vector.IntVector;
    import org.apache.arrow.vector.VectorSchemaRoot;

    import java.util.Arrays;

    public class ToBeCalledByCpp {
        final static BufferAllocator allocator = new RootAllocator();

        public static void fillfieldvectorfromjavatocpp(long schema_id, long array_id){
            try (ArrowArray arrow_array = ArrowArray.wrap(array_id);
                 ArrowSchema arrow_schema = ArrowSchema.wrap(schema_id) ) {
                Data.exportVector(allocator, populateFieldVectorToExport(), null, arrow_array, arrow_schema);
            }
        }

        public static FieldVector populateFieldVectorToExport(){
            IntVector intVector = new IntVector("int-to-export", allocator);
            intVector.allocateNew(3);
            intVector.setSafe(0, 1);
            intVector.setSafe(1, 2);
            intVector.setSafe(2, 3);
            intVector.setValueCount(3);
            System.out.println("[Java] FieldVector: \n" + intVector);
            return intVector;
        }

        public static void fillvectorschemarootfromjavatocpp(long schema_id, long array_id){
            try (ArrowArray arrow_array = ArrowArray.wrap(array_id);
                 ArrowSchema arrow_schema = ArrowSchema.wrap(schema_id) ) {
                Data.exportVectorSchemaRoot(allocator, populateVectorSchemaRootToExport(), null, arrow_array, arrow_schema);
            }
        }

        public static VectorSchemaRoot populateVectorSchemaRootToExport(){
            IntVector intVector = new IntVector("age-to-export", allocator);
            intVector.setSafe(0, 10);
            intVector.setSafe(1, 20);
            intVector.setSafe(2, 30);
            VectorSchemaRoot root = new VectorSchemaRoot(Arrays.asList(intVector));
            root.setRowCount(3);
            System.out.println("[Java - side] VectorSchemaRoot: \n" + root.contentToTSVString());
            return root;
        }

        public static void main(String[] args) {
            populateFieldVectorToExport();
            populateVectorSchemaRootToExport();
        }
    }

Build the JAR and copy it to the C++ project.

.. code-block:: shell

    $ mvn clean install
    $ cp target/cpptojava-1.0-SNAPSHOT-jar-with-dependencies.jar <c++ project path>/cpptojava.jar

**C++ Side**

This application uses JNI to call Java code, but transfers data (zero-copy) via the C Data Interface instead.

.. code-block:: cpp

    #include <iostream>
    #include <arrow/api.h>
    #include <arrow/c/bridge.h>
    #include <jni.h>

    JNIEnv* CreateVM(JavaVM** jvm) {
        JNIEnv *env;
        JavaVMInitArgs vm_args;
        JavaVMOption options[2];
        options[0].optionString = "-Djava.class.path=cpptojava.jar"; // java jar name
        options[1].optionString = "-DXcheck:jni:pedantic";
        vm_args.version = JNI_VERSION_1_8;
        vm_args.nOptions = 2;
        vm_args.options = options;
        int status = JNI_CreateJavaVM(jvm, (void**) &env, &vm_args);
        if (status < 0) printf("\n<<<<< Unable to Launch JVM >>>>>\n");
        return env;
    }

    int main() {
        JNIEnv* env;
        JavaVM* jvm;
        env = create_vm(&jvm);
        if (env == nullptr) return 1;
        jclass javaClassToBeCalledByCpp = NULL;
        javaClassToBeCalledByCpp = env ->FindClass("ToBeCalledByCpp");
        if ( javaClassToBeCalledByCpp != NULL ) {
            jmethodID fillfieldvectorfromjavatocpp = NULL;
            fillfieldvectorfromjavatocpp = env->GetStaticMethodID(javaClassToBeCalledByCpp, "fillfieldvectorfromjavatocpp", "(JJ)V");
            if ( fillfieldvectorfromjavatocpp != NULL ){
                struct ArrowSchema arrowSchema;
                struct ArrowArray arrowArray;
                printf("\n<<<<< C++ to Java for Arrays >>>>>\n");
                env->CallStaticVoidMethod(javaClassToBeCalledByCpp, fillfieldvectorfromjavatocpp, reinterpret_cast<uintptr_t>(&arrowSchema), reinterpret_cast<uintptr_t>(&arrowArray));
                auto resultImportArray = arrow::ImportArray(&arrowArray, &arrowSchema);
                std::shared_ptr<arrow::Array> array = resultImportArray.ValueOrDie();
                auto int32_array = std::static_pointer_cast<arrow::Int32Array>(array);
                const int32_t* data = int32_array->raw_values();
                for (int j = 0; j < int32_array->length(); j++){
                    printf("[C++ - side]: Data ImportArray - array[%d] = %d\n", j, data[j]);
                }
            } else {
                printf("Problem to read method fillfieldvectorfromjavatocpp\n");
            }
            jmethodID fillvectorschemarootfromjavatocpp = NULL;
            fillvectorschemarootfromjavatocpp = env->GetStaticMethodID(javaClassToBeCalledByCpp, "fillvectorschemarootfromjavatocpp", "(JJ)V");
            if ( fillvectorschemarootfromjavatocpp != NULL ){
                struct ArrowSchema arrowSchema;
                struct ArrowArray arrowArray;
                printf("\n<<<<< C++ to Java for RecordBatch >>>>>\n");
                env->CallStaticVoidMethod(javaClassToBeCalledByCpp, fillvectorschemarootfromjavatocpp, static_cast<long>(reinterpret_cast<uintptr_t>(&arrowSchema)), static_cast<long>(reinterpret_cast<uintptr_t>(&arrowArray)));
                auto resultImportVectorSchemaRoot = arrow::ImportRecordBatch(&arrowArray, &arrowSchema);
                std::shared_ptr<arrow::RecordBatch> recordBatch = resultImportVectorSchemaRoot.ValueOrDie();
                for (std::shared_ptr<arrow::Array> array : recordBatch->columns()) {
                    auto int32_array = std::static_pointer_cast<arrow::Int32Array>(array);
                    const int32_t* data = int32_array->raw_values();
                    for (int j = 0; j < int32_array->length(); j++){
                        printf("[C++ - side]: Data ImportArray - array[%d] = %d\n", j, data[j]);
                    }
                }
            } else {
                printf("Problem to read method fillvectorschemarootfromjavatocpp\n");
            }
        } else {
            printf("Problem to read class ToBeCalledByCpp\n");
        }
        jvm ->DestroyJavaVM();
        return 0;
    }

CMakeLists.txt definition file:

.. code-block:: xml

    cmake_minimum_required(VERSION 3.19)
    project(firstarrowcpp)
    find_package(JNI REQUIRED)
    find_package(Arrow REQUIRED)
    message(STATUS "Arrow version: ${ARROW_VERSION}")
    message(${ARROW_FULL_SO_VERSION})
    include_directories(${JNI_INCLUDE_DIRS})
    set(CMAKE_CXX_STANDARD 11)
    add_executable(${PROJECT_NAME} main.cpp)
    target_link_libraries(firstarrowcpp PRIVATE arrow_shared)
    target_link_libraries(firstarrowcpp PRIVATE ${JNI_LIBRARIES})

**Result**

.. code-block:: shell

    <<<<< C++ to Java for Arrays >>>>>
    [Java - side]: FieldVector:
    [1, 2, 3]
    [C++ - side]: Data ImportArray - array[0] = 1
    [C++ - side]: Data ImportArray - array[1] = 2
    [C++ - side]: Data ImportArray - array[2] = 3

    <<<<< C++ to Java for RecordBatch >>>>>
    [Java - side] VectorSchemaRoot:
    age-to-export
    10
    20
    30

    [C++ - side]: Data ImportArray - array[0] = 10
    [C++ - side]: Data ImportArray - array[1] = 20
    [C++ - side]: Data ImportArray - array[2] = 30

.. _`JavaCPP`: https://github.com/bytedeco/javacpp
