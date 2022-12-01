<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Table

**NOTE**: The API is experimental and subject to change. See the list of limitations below.

*Table* is a new immutable tabular data structure based on FieldVectors. A mutable version (*MutableTable*) is expected in a subsequent release. This document describes the Table API.

---

Like VectorSchemaRoot, *Table* is a columnar data structure backed by Arrow arrays, or more specifically, by FieldVector objects. It differs from VectorSchemaRoot mainly in that it is fully immutable and lacks support for batch operations. Anyone processing batches of tabular data in a pipeline should continue to use VectorSchemaRoot.

## Mutation in Table and VectorSchemaRoot

VectorSchemaRoot provides a thin wrapper on the FieldVectors that hold its data. Individual FieldVectors can be retrieved from the VectorSchemaRoot. These FieldVectors have *setters* for modifying their elements, so VectorSchemaRoot is immutable only by convention. The protocol for mutating a vector is documented in the ValueVector class:

- values need to be written in order (e.g. index 0, 1, 2, 5)
- null vectors start with all values as null before writing anything
- for variable width types, the offset vector should be all zeros before writing
- you must call setValueCount before a vector can be read
- you should never write to a vector once it has been read.

The rules aren't enforced by the API so the programmer is responsible for ensuring that they are followed. Failure to do so could lead to runtime exceptions.

_Table_, on the other hand, is immutable. The underlying vectors are not exposed. When a Table is created from existing vectors, their memory is transferred to new vectors, so subsequent changes to the original vectors can't impact the new table's values.

## Features and limitations

### Features
A basic set of table functionality is included in this release:

- Create a Table from FieldVectors or VectorSchemaRoot
- Iterate tables by row or set the current row index directly
- Access Vector values as primitives, objects, and/or NullableHolders (depending on type)
- Get FieldReader for any vector
- Add and remove FieldVectors
- Encode and decode a table's vectors using DictionaryEncoding
- Export Table memory for use by native code
- Print first rows to Strings
- Get table schema
- Slice tables
- Convert table to VectorSchemaRoot

### Limitations
The following are the major limitations of v. 10.0.0 release:

1. **Creating Tables with data imported using the C-Data API will result in a runtime exception**. Support for ths feature is gated on PR#13248 (https://github.com/apache/arrow/pull/13248).
2. No support ChunkedArrays or any form of row-group. Support for ChunkedArrows or row groups will be considered for a future release.
3. No support for native interface using the C-Stream API. Support for the streaming API will be delivered with or after  Item 1.
4. No support for creating tables directly from Java POJOs. All data held by a table must be imported via a VectorSchemaRoot, or from collections or arrays of FieldVectors.
5. No support for mutable tables.

## What's in a Table?

Like VectorSchemaRoot, Table consists of a `Schema` and an ordered collection of `FieldVector` objects, but it is designed to be accessed via a row-oriented interface.

## Table API: Creating Tables

### Creating a Table from a VectorSchemaRoot

Tables are created from a VectorSchemaRoot as shown below. The memory buffers holding the data are transferred from the VectorSchemaRoot to new vectors in the new Table, clearing the original VectorSchemaRoot in the process. This ensures that the data in your new Table is never changed. Since the buffers are transferred rather than copied, this is a very low overhead operation.

```java
    VectorSchemaRoot vsr = getMyVsr();
    Table t = new Table(vsr);
```

If you now update the FieldVectors used to create the VectorSchemaRoot (using some variation of  `ValueVector#setSafe()`), the VectorSchemaRoot *vsr* would reflect those changes, but the values in Table *t* are unchanged.

***Current Limitation:*** Due to an unresolved limitation in `CDataReferenceManager`, you cannot currently create a Table from a VectorSchemaRoot that was created in native code and transferred to Java via the C-Data Interface.

### Creating a Table from ValueVectors

Tables can be created from ValueVectors as shown below.

```java
    IntVector myVector = createMyIntVector();
    VectorSchemaRoot vsr1 = new VectorSchemaRoot(myVector); 
```

or

```java
    IntVector myVector = createMyIntVector();
    List<FieldVector> fvList = List.of(myVector);
    VectorSchemaRoot vsr1 = new VectorSchemaRoot(fvList); 
```

It is rarely a good idea to share vectors between multiple VectorSchemaRoots, and it would not be a good idea to share them between VectorSchemaRoots and tables. Creating a VectorSchemaRoot from a list of vectors does not cause the reference counts for the vectors to be incremented. Unless you manage the counts manually, the code shown below would lead to more references to the vectors than reference counts, and that could lead to trouble. There is an implicit assumption that the vectors were created for use by *one* VectorSchemaRoot that this code violates.

*Don't do this:*

```Java
    IntVector myVector = createMyIntVector();  // Reference count for myVector = 1
    VectorSchemaRoot vsr1 = new VectorSchemaRoot(myVector); // Still one reference
    VectorSchemaRoot vsr2 = new VectorSchemaRoot(myVector);
    // Ref count is still one, but there are two VSRs with a reference to myVector
    vsr2.clear(); // Reference count for myVector is 0.
```

What is happening is that the reference counter works at a lower level than the VectorSchemaRoot interface. A reference counter counts references to ArrowBuf instances that control memory buffers. It doesn't count references to the ValueVectors that hold *them*. In the example above, each ArrowBuf is held by one ValueVector, so there is only one reference. This distinction is blurred though, when you call the VectorSchemaRoot's clear() method, which frees the memory held by each of the vectors it references even though another instance might refer to the same vectors.

When you create Tables from vectors, it's assumed that there are no external references to those vectors. But, just to be on the safe side, the buffers underlying these vectors are transferred to new ValueVectors in the new Table, and the original vectors are cleared.

*Don't do this either, but note the difference from above:*

```Java
    IntVector myVector = createMyIntVector(); // Reference count for myVector = 1
    Table t1 = new Table(myVector);  // myVector is cleared; Table t1 has a new hidden vector with
    // the data from myVector
    Table t2 = new Table(myVector);  // t2 has no rows because the myVector was just cleared
    // t1 continues to have the data from the original vector
    t2.clear();                      // no change because t2 is already empty and t1 is independent
```

With Tables, memory is explicitly transferred on instantiatlon so the buffers are held by that table are held by *only* that table.

#### Creating Tables with dictionary-encoded vectors

***Note: this section is highly speculative***

Another point of difference is that dictionary-encoding is managed separately from VectorSchemaRoot, while Tables hold an optional DictionaryProvider instance. If any vectors in the source data are encoded, a DictionaryProvider must be set to un-encode the values.

```java
    VectorSchemaRoot vsr = myVsr();
    DictionaryProvider provider = myProvider();
    Table t = new Table(vsr, provider);
```

In the immutable Table case, dictionaries are used in a way that's similar to the approach used with ValueVectors. To decode a vector, the user provides the dictionary id and the name of the vector to decode:

```Java
    Table t = new Table(vsr, provider);
    ValueVector decodedName = t.decode("name", 1L);
```

To encode a vector from a table, a similar approach is used:

```Java
    Table t = new Table(vsr, provider);
    ValueVector encodedName = t.encode("name", 1L);
```

```java
    String output = myTable.contentToTSVString(true);
```

### Freeing memory explicitly

Tables use off-heap memory that must be freed when it is no longer needed. Table implements AutoCloseable so the best way to create one is in a try-with-resources block:

```java
    try (VectorSchemaRoot vsr = myMethodForGettingVsrs();
        Table t = new Table(vsr)) {
        // do useful things.
    }
```

If you don't use a try-with-resources block, you must close the Table manually:

````java
    try {
        VectorSchemaRoot vsr = myMethodForGettingVsrs();
        Table t = new Table(vsr);
        // do useful things.
    } finally {
        vsr.close();
        t.close();
    }
````

Manually closing should be performed in a finally block.

## Table API: getting the schema

You get the table's schema the same way you do with a VectorSchemaRoot:

```java
    Schema s = table.getSchema(); 
```

## Table API: Adding and removing vectors

Table provides facilities for adding and removing FieldVectors modeled on the same functionality in VectorSchemaRoot. As with VectorSchemaRoot, these operations return new instances rather than modifiying the original instance in-place.

```java
    try (Table t = new Table(vectorList)) {
        IntVector v3 = new IntVector("3", intFieldType, allocator);
        Table t2 = t.addVector(2, v3);
        Table t3 = t2.removeVector(1);
        // don't forget to close t2 and t3
    }
```

## Table API: Slicing tables

Table supports *slice()* operations, where a slice of a source table is a second Table that refers to a single, contiguous range of rows in the source.

```Java
    try (Table t = new Table(vectorList)) {
        Table t2 = t.slice(100, 200); // creates a slice referencing the values in range (100, 200]
        ...
    }
```

If you created a slice with *all* the values in the source table (as shown below), how would that differ from a new Table constructed with the same vectors as the source?

```Java
    try (Table t = new Table(vectorList)) {
        Table t2 = t.slice(0, t.getRowCount()); // creates a slice referencing all the values in t
        // ...
    }
```

The difference is that when you *construct* a new table, the buffers are transferred from the source vectors to new vectors in the destination. With a slice, both tables share the same underlying vectors. That's OK, though, since both Tables are immutable.

Slices will not be supported in MutableTables.

## Table API: Using FieldReaders

You can get a FieldReader for any vector in the Table using either the Field, vector index, or vector name. The signatures are the same as in VectorSchemaRoot.

```java
    FieldReader nameReader = table.getReader("user_name");
```

## Table API: Row operations

Row-based access is supported using a Row object. Row provides *get()* methods by both vector name and vector position, but no *set()* operations. It is important to recognize that it's NOT a reified row, but rather operates like a cursor where the data from numerous logical rows in the Table can be viewed (one row at a time) using the same Row instance. See "Getting around" below for information about how to navigate through the table.

**Note**: A mutable row implementation is expected with the release of MutableTable, which will support both mutable and immutable Rows.

### Getting a row

Calling `immutableRow()` on any table instance returns a row supporting read operations.

```java
    Row r = table.immutableRow(); 
```

### Getting around

Since rows are iterable, you can traverse a table using a standard while loop:

```java
    Row r = table.immutableRow();
    while (r.hasNext()) {
      r.next();
      // do something useful here
    }
```

Table implements `Iterable<Row>` so you can access rows directly from Table in an enhanced *for* loop:

```java
    for (Row row: table) {
      int age = row.getInt("age");
      boolean nameIsNull = row.isNull("name");
      ...
    }
```

Finally, while rows are usually iterated in the order of the underlying data vectors, but they are also positionable using the `Row#setPosition()` method, so you can skip to a specific row. Row numbers are 0-based.

```java
    Row r = table.immutableRow();
    int age101 = r.setPosition(101); // change position directly to 101
```

Any changes to position are of course applied to all the columns in the table.

Note that you must call `next()`, or `setPosition()` before accessing values via a row. Failure to do so results in a runtime exception.

### Read operations using rows

Methods are available for getting values by vector name and vector index, where index is the 0-based position of the vector in the table. For example, assuming 'age' is the 13th vector in 'table', the following two gets are equivalent:

```java
    Row r = table.immutableRow();
    r.next(); // position the row at the first value
    int age1 = r.get("age"); // gets the value of vector named 'age' in the table at row 0
    int age2 = r.get(12);    // gets the value of the 13th vecto in the table at row 0
```

You can also get value using a NullableHolder. For example:

```Java

    NullableIntHolder holder = new NullableIntHolder();
    int b = row.getInt("age", holder);
```

This can be used to retrieve values without creating a new Object for each.

In addition to getting values, you can check if a value is null using `isNull()` and you can get the current row number:

```java
    boolean name0isNull = row.isNull("name");
    int row = row.getRowNumber(); 
```

Note that while there are getters for most vector types (e.g. *getInt()* for use with IntVector) and a generic *isNull()* method, there is no *getNull()* method for use with the NullVector type or *getZero()* for use with ZeroVector (a zero-length vector of any type).

#### Reading values as Objects

For any given vector type, the basic *get()* method returns a primitive value wherever possible. For example, *getTimeStampMicro()* returns a long value that encodes the timestamp. To get the LocalDateTime object representing that timestamp in Java, another method with 'Obj' appended to the name is provided.  For example:

```java
    long ts = row.getTimeStampMicro();
    LocalDateTime tsObject = row.getTimeStampMicroObj();
```

The exception to this naming scheme is for complex vector types (List, Map, Schema, Union, DenseUnion, and ExtensionType). These always return objects rather than primitives so no "Obj" extension is required.  It is expected that some users may subclass Row to add getters that are more specific to their needs.

#### Reading VarChars and LargeVarChars

Strings in arrow are represented as byte arrays, encoded with the UTF-8 charset as this is the only character set supported in the Arrow format. You can get either a String result or the actual byte array.

```Java
    byte[] b = row.getVarChar("first_name");
    String s = row.getVarCharObj("first_name");       // uses the default encoding (UTF-8)
```

## Table API: Converting a Table to a VectorSchemaRoot

Tables can be converted to VectorSchemaRoot objects using the *toVectorSchemaRoot()* method.

```java
    VectorSchemaRoot root = myTable.toVectorSchemaRoot();
```

Buffers are transferred to the VectorSchemaRoot and the Table is cleared.

## Table API: Working with the C-Data interface

The ability to work with native code is required for many Arrow features. This section describes how tables can be be exported and imported.

### Exporting Tables to native code

This works by converting the data to a VectorSchemaRoot and using the existing facilities for transferring the data. This would not generally be ideal because conversion to a VectorSchemaRoot breaks the immutability guarantees. Using the static utility methods defined in the class org.apache.arrow.c.Data` avoids this concern because the vector schema root used is not expored.  See the example code below:

```java
    Data.exportTable(bufferAllocator, table, dictionaryProvider, outArrowArray);
```

If the table contains dictionary-encoded vectors, it should have been created with a dictionary provider to support encode and decode operations. In that case, the provider argument can be ommitted and the table's provider attribute will be used:

```java
    Data.exportTable(bufferAllocator, table, outArrowArray);
```

### Importing Tables from native code

***Current limitation: Data imported from native code using the C-Data-interface cannot be used in a table, because the current implementation of CDataReferenceManager does not support the transfer operation.***

## Table API: Working with the C-Stream interface

***Current limitation: Streaming API is not currently supported. Support is planned for a future release.***
