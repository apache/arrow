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

=====
Table
=====

**NOTE**: The Table API is experimental and subject to change. See the list of limitations below.

`Table`_ is an immutable tabular data structure based on `FieldVector`_. Like `VectorSchemaRoot`_, ``Table`` is a columnar data structure backed by Arrow arrays, or more specifically, by ``FieldVector`` objects. It differs from ``VectorSchemaRoot`` mainly in that it is fully immutable and lacks support for batch operations. Anyone processing batches of tabular data in a pipeline should continue to use ``VectorSchemaRoot``. Finally, the ``Table`` API is mainly row-oriented, so in some ways it's more like the JDBC API than the ``VectorSchemaRoot`` API, but you can still use ``FieldReaders`` to work with data in a columnar fashion.

Mutation in Table and VectorSchemaRoot
======================================

``VectorSchemaRoot`` provides a thin wrapper on the vectors that hold its data. Individual vectors can be retrieved from a vector schema root. These vectors have *setters* for modifying their elements, making ``VectorSchemaRoot`` immutable only by convention. The protocol for mutating a vector is documented in the `ValueVector`_ interface:

- values need to be written in order (e.g. index 0, 1, 2, 5)
- null vectors start with all values as null before writing anything
- for variable width types, the offset vector should be all zeros before writing
- you must call setValueCount before a vector can be read
- you should never write to a vector once it has been read.

The rules aren't enforced by the API so the programmer is responsible for ensuring that they are followed. Failure to do so could lead to runtime exceptions.

``Table``, on the other hand, is immutable. The underlying vectors are not exposed. When a table is created from existing vectors, their memory is transferred to new vectors, so subsequent changes to the original vectors can't impact the new table's values.

Features and limitations
======================================

A basic set of table functionality is currently available:

- Create a table from vectors or ``VectorSchemaRoot``
- Iterate tables by row, or set the current row index directly
- Access vector values as primitives, objects, and/or nullable `ValueHolder`_ instances (depending on type)
- Get a ``FieldReader`` for any vector
- Add and remove vectors, creating new tables
- Encode and decode a table's vectors using dictionary encoding
- Export table data for use by native code
- Print representative data to TSV strings
- Get a table's schema
- Slice tables
- Convert table to ``VectorSchemaRoot``

Limitations in the 11.0.0 release:

- No support ``ChunkedArray`` or any form of row-group. Support for chunked arrays or row groups will be considered for a future release.
- No support for the C-Stream API. Support for the streaming API is contingent on chunked array support
- No support for creating tables directly from Java POJOs. All data held by a table must be imported via a ``VectorSchemaRoot``, or from collections or arrays of vectors.

The Table API
=============

Like ``VectorSchemaRoot``, a table contains a `Schema`_ and an ordered collection of ``FieldVector`` objects, but it is designed to be accessed via a row-oriented interface.

Creating a Table from a VectorSchemaRoot
****************************************

Tables are created from a ``VectorSchemaRoot`` as shown below. The memory buffers holding the data are transferred from the vector schema root to new vectors in the new table, clearing the source vectors in the process. This ensures that the data in your new table is never changed. Since the buffers are transferred rather than copied, this is a very low overhead operation.

.. code-block:: Java

    Table t = new Table(someVectorSchemaRoot);

If you now update the vectors held by the ``VectorSchemaRoot`` (using some version of  `ValueVector#setSafe()`), it would reflect those changes, but the values in table *t* are unchanged.

Creating a Table from FieldVectors
**********************************

Tables can be created from ``FieldVectors`` as shown below, using 'var-arg' array arguments:

.. code-block:: Java

    IntVector myVector = createMyIntVector();
    VectorSchemaRoot vsr1 = new VectorSchemaRoot(myVector);

or by passing a collection:

.. code-block:: Java

    IntVector myVector = createMyIntVector();
    List<FieldVector> fvList = List.of(myVector);
    VectorSchemaRoot vsr1 = new VectorSchemaRoot(fvList);

It is rarely a good idea to share vectors between multiple vector schema roots, and it would not be a good idea to share them between vector schema roots and tables. Creating a ``VectorSchemaRoot`` from a list of vectors does not cause the reference counts for the vectors to be incremented. Unless you manage the counts manually, the code below would lead to more references than reference counts, and that could lead to trouble. There is an implicit assumption that the vectors were created for use by *one* ``VectorSchemaRoot`` that this code violates.

*Don't do this:*

.. code-block:: Java

    IntVector myVector = createMyIntVector();  // Reference count for myVector = 1
    VectorSchemaRoot vsr1 = new VectorSchemaRoot(myVector); // Still one reference
    VectorSchemaRoot vsr2 = new VectorSchemaRoot(myVector);
    // Ref count is still one, but there are two VSRs with a reference to myVector
    vsr2.clear(); // Reference count for myVector is 0.

What is happening is that the reference counter works at a lower level than the ``VectorSchemaRoot`` interface. A reference counter counts references to `ArrowBuf`_ instances that control memory buffers. It doesn't count references to the vectors that hold those ArrowBufs. In the example above, each ``ArrowBuf`` is held by one vector, so there is only one reference. This distinction is blurred when you call the ``VectorSchemaRoot``'s clear() method, which frees the memory held by each of the vectors it references even though another instance references the same vectors.

When you create tables from vectors, it's assumed that there are no external references to those vectors. To be certain, the buffers underlying these vectors are transferred to new vectors in the new table, and the original vectors are cleared.

*Don't do this either, but note the difference from above:*

.. code-block:: Java

    IntVector myVector = createMyIntVector(); // Reference count for myVector = 1
    Table t1 = new Table(myVector);
    // myVector is cleared; Table t1 has a new hidden vector with the data from myVector
    Table t2 = new Table(myVector);
    // t2 has no rows because myVector was just cleared
    // t1 continues to have the data from the original vector
    t2.clear();
    // no change because t2 is already empty and t1 is independent

With tables, memory is explicitly transferred on instantiation so the buffers held by a table are held by *only* that table.

Creating Tables with dictionary-encoded vectors
***********************************************

Another point of difference is that ``VectorSchemaRoot`` is uninformed about any dictionary-encoding of its vectors, while tables hold an optional `DictionaryProvider`_ instance. If any vectors in the source data are encoded, a DictionaryProvider must be set to un-encode the values.

.. code-block:: Java

    VectorSchemaRoot vsr = myVsr();
    DictionaryProvider provider = myProvider();
    Table t = new Table(vsr, provider);

In ``Table``, dictionaries are used like they are with vectors. To decode a vector, the user provides the name of the vector to decode and the dictionary id:

.. code-block:: Java

    Table t = new Table(vsr, provider);
    ValueVector decodedName = t.decode("name", 1L);

To encode a vector from a table, a similar approach is used:

.. code-block:: Java

    Table t = new Table(vsr, provider);
    ValueVector encodedName = t.encode("name", 1L);

Freeing memory explicitly
*************************

Tables use off-heap memory that must be freed when it is no longer needed. ``Table`` implements ``AutoCloseable`` so the best way to create one is in a try-with-resources block:

.. code-block:: Java

    try (VectorSchemaRoot vsr = myMethodForGettingVsrs();
        Table t = new Table(vsr)) {
        // do useful things.
    }

If you don't use a try-with-resources block, you must close the table manually:

.. code-block:: Java

    try {
        VectorSchemaRoot vsr = myMethodForGettingVsrs();
        Table t = new Table(vsr);
        // do useful things.
    } finally {
        vsr.close();
        t.close();
    }

Manual closing should be performed in a finally block.

Getting the schema
******************

You get the table's schema just as you would with a vector schema root:

.. code-block:: Java

    Schema s = table.getSchema();

Adding and removing vectors
***************************

``Table`` provides facilities for adding and removing vectors modeled on the same functionality in ``VectorSchemaRoot``. These operations return new instances rather than modifying the original instance in-place.

.. code-block:: Java

    try (Table t = new Table(vectorList)) {
        IntVector v3 = new IntVector("3", intFieldType, allocator);
        Table t2 = t.addVector(2, v3);
        Table t3 = t2.removeVector(1);
        // don't forget to close t2 and t3
    }

Slicing tables
**************

``Table`` supports *slice()* operations, where a slice of a source table is a second Table that refers to a single, contiguous range of rows in the source.

.. code-block:: Java

    try (Table t = new Table(vectorList)) {
        Table t2 = t.slice(100, 200); // creates a slice referencing the values in range (100, 200]
        ...
    }

This raises the question: If you create a slice with *all* the values in the source table (as shown below), how would that differ from a new Table constructed with the same vectors as the source?

.. code-block:: Java

    try (Table t = new Table(vectorList)) {
        Table t2 = t.slice(0, t.getRowCount()); // creates a slice referencing all the values in t
        // ...
    }

The difference is that when you *construct* a new table, the buffers are transferred from the source vectors to new vectors in the destination. With a slice, both tables share the same underlying vectors. That's OK, though, since both tables are immutable.

Using FieldReaders
******************

You can get a `FieldReader`_ for any vector in the Table passing either the `Field`_, vector index, or vector name as an argument. The signatures are the same as in ``VectorSchemaRoot``.

.. code-block:: Java

    FieldReader nameReader = table.getReader("user_name");

Row operations
**************

Row-based access is supported by the `Row`_ object. ``Row`` provides *get()* methods by both vector name and vector position, but no *set()* operations.

It is important to recognize that rows are NOT reified as objects, but rather operate like a cursor where the data from numerous logical rows in the table can be viewed (one at a time) using the same ``Row`` instance. See "Moving from row-to-row" below for information about navigating through the table.

Getting a row
*************

Calling `immutableRow()` on any table instance returns a new ``Row`` instance.

.. code-block:: Java

    Row r = table.immutableRow();

Moving from row-to-row
**********************

Since rows are iterable, you can traverse a table using a standard while loop:

.. code-block:: Java

    Row r = table.immutableRow();
    while (r.hasNext()) {
      r.next();
      // do something useful here
    }

``Table`` implements `Iterable<Row>` so you can access rows directly from a table in an enhanced *for* loop:

.. code-block:: Java

    for (Row row: table) {
      int age = row.getInt("age");
      boolean nameIsNull = row.isNull("name");
      ...
    }

Finally, while rows are usually iterated in the order of the underlying data vectors, but they are also positionable using the `Row#setPosition()` method, so you can skip to a specific row. Row numbers are 0-based.

.. code-block:: Java

    Row r = table.immutableRow();
    int age101 = r.setPosition(101); // change position directly to 101

Any changes to position are applied to all the columns in the table.

Note that you must call `next()`, or `setPosition()` before accessing values via a row. Failure to do so results in a runtime exception.

Read operations using rows
**************************

Methods are available for getting values by vector name and vector index, where index is the 0-based position of the vector in the table. For example, assuming 'age' is the 13th vector in 'table', the following two gets are equivalent:

.. code-block:: Java

    Row r = table.immutableRow();
    r.next(); // position the row at the first value
    int age1 = r.get("age"); // gets the value of vector named 'age' in the table at row 0
    int age2 = r.get(12);    // gets the value of the 13th vector in the table at row 0

You can also get value using a nullable ``ValueHolder``. For example:

.. code-block:: Java

    NullableIntHolder holder = new NullableIntHolder();
    int b = row.getInt("age", holder);

This can be used to retrieve values without creating a new Object for each.

In addition to getting values, you can check if a value is null using `isNull()`. This is important if the vector contains any nulls, as asking for a value from a vector can cause NullPointerExceptions in some cases.

.. code-block:: Java

    boolean name0isNull = row.isNull("name");

You can also get the current row number:

.. code-block:: Java

    int row = row.getRowNumber();

Reading values as Objects
*************************

For any given vector type, the basic *get()* method returns a primitive value wherever possible. For example, *getTimeStampMicro()* returns a long value that encodes the timestamp. To get the LocalDateTime object representing that timestamp in Java, another method with 'Obj' appended to the name is provided.  For example:

.. code-block:: Java

    long ts = row.getTimeStampMicro();
    LocalDateTime tsObject = row.getTimeStampMicroObj();

The exception to this naming scheme is for complex vector types (List, Map, Schema, Union, DenseUnion, and ExtensionType). These always return objects rather than primitives so no "Obj" extension is required.  It is expected that some users may subclass ``Row`` to add getters that are more specific to their needs.

Reading VarChars and LargeVarChars
**********************************

Strings in arrow are represented as byte arrays encoded with the UTF-8 charset. You can get either a String result or the actual byte array.

.. code-block:: Java

    byte[] b = row.getVarChar("first_name");
    String s = row.getVarCharObj("first_name");       // uses the default encoding (UTF-8)

Converting a Table to a VectorSchemaRoot
****************************************

Tables can be converted to vector schema roots using the *toVectorSchemaRoot()* method. Buffers are transferred to the vector schema root and the source table is cleared.

.. code-block:: Java

    VectorSchemaRoot root = myTable.toVectorSchemaRoot();

Working with the C-Data interface
*********************************

The ability to work with native code is required for many Arrow features. This section describes how tables can be be exported for use with native code

Exporting works by converting the data to a ``VectorSchemaRoot`` instance and using the existing facilities to transfer the data. You could do it yourself, but that isn't ideal because conversion to a vector schema root breaks the immutability guarantees. Using the `exportTable()` methods in the `Data`_ class avoids this concern.

.. code-block:: Java

    Data.exportTable(bufferAllocator, table, dictionaryProvider, outArrowArray);

If the table contains dictionary-encoded vectors and was constructed with a ``DictionaryProvider``, the provider argument to `exportTable()` can be omitted and the table's provider attribute will be used:

.. code-block:: Java

    Data.exportTable(bufferAllocator, table, outArrowArray);

.. _`ArrowBuf`: https://arrow.apache.org/docs/java/reference/org/apache/arrow/memory/ArrowBuf.html
.. _`Data`: https://arrow.apache.org/docs/java/reference/org/apache/arrow/c/Data.html
.. _`DictionaryProvider`: https://arrow.apache.org/docs/java/reference/org/apache/arrow/vector/dictionary/DictionaryProvider.html
.. _`Field`: https://arrow.apache.org/docs/java/reference/org/apache/arrow/vector/types/pojo/Field.html
.. _`FieldReader`: https://arrow.apache.org/docs/java/reference/org/apache/arrow/vector/complex/reader/FieldReader.html
.. _`FieldVector`: https://arrow.apache.org/docs/java/reference/org/apache/arrow/vector/FieldVector.html
.. _`Row`: https://arrow.apache.org/docs/java/reference/org/apache/arrow/vector/table/Row.html
.. _`Schema`: https://arrow.apache.org/docs/java/reference/org/apache/arrow/vector/types/pojo/Schema.html
.. _`Table`: https://arrow.apache.org/docs/java/reference/org/apache/arrow/vector/table/Table.html
.. _`ValueHolder`: https://arrow.apache.org/docs/java/reference/org/apache/arrow/vector/holders/ValueHolder.html
.. _`ValueVector`: https://arrow.apache.org/docs/java/reference/org/apache/arrow/vector/ValueVector.html
.. _`VectorSchemaRoot`: https://arrow.apache.org/docs/java/reference/org/apache/arrow/vector/VectorSchemaRoot.html
