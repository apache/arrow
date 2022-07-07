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

==================
Arrow JDBC Adapter
==================

The Arrow JDBC Adapter assists with working with JDBC and Arrow
data. Currently, it supports reading JDBC ResultSets into Arrow
VectorSchemaRoots.

ResultSet to VectorSchemaRoot Conversion
========================================

This can be accessed via the JdbcToArrow class. The resulting
ArrowVectorIterator will convert a ResultSet to Arrow data in batches
of rows.

.. code-block:: java

   try (ArrowVectorIterator it = JdbcToArrow.sqlToArrowVectorIterator(resultSet, allocator)) {
     while (it.hasNext()) {
       VectorSchemaRoot root = it.next();
       // Consume the root…
     }
   }

The batch size and type mapping can both be customized:

.. code-block:: java

   JdbcToArrowConfig config = new JdbcToArrowConfigBuilder(allocator, /*calendar=*/null)
       .setReuseVectorSchemaRoot(reuseVectorSchemaRoot)
       .setJdbcToArrowTypeConverter((jdbcFieldInfo -> {
         switch (jdbcFieldInfo.getJdbcType()) {
           case Types.BIGINT:
             // Assume actual value range is SMALLINT
             return new ArrowType.Int(16, true);
           default:
             return null;
         }
       }))
       .build();
   try (ArrowVectorIterator iter = JdbcToArrow.sqlToArrowVectorIterator(rs, config)) {
     while (iter.hasNext()) {
       VectorSchemaRoot root = iter.next();
       // Consume the root…
     }
   }

The JDBC type can be explicitly specified, which is useful since JDBC
drivers can give spurious type information. For example, the Postgres
driver has been observed to use Decimal types with scale and precision
0; these cases can be handled by specifying the type explicitly before
reading. Also, some JDBC drivers may return BigDecimal values with
inconsistent scale. A RoundingMode can be set to handle these cases:

.. code-block:: java

   Map<Integer, JdbcFieldInfo> mapping = new HashMap<>();
   mapping.put(1, new JdbcFieldInfo(Types.DECIMAL, 20, 7));
   JdbcToArrowConfig config = new JdbcToArrowConfigBuilder(allocator, /*calendar=*/null)
       .setBigDecimalRoundingMode(RoundingMode.UNNECESSARY)
       .setExplicitTypesByColumnIndex(mapping)
       .build();
   try (ArrowVectorIterator iter = JdbcToArrow.sqlToArrowVectorIterator(rs, config)) {
     while (iter.hasNext()) {
       VectorSchemaRoot root = iter.next();
       // Consume the root…
     }
   }

Currently, it is not possible to define a custom type conversion for a
supported or unsupported type.

Type Mapping
------------

The JDBC to Arrow type mapping can be obtained at runtime from
`JdbcToArrowUtils.getArrowTypeFromJdbcType`_.

.. _JdbcToArrowUtils.getArrowTypeFromJdbcType: https://arrow.apache.org/docs/java/reference/org/apache/arrow/adapter/jdbc/JdbcToArrowUtils.html#getArrowTypeFromJdbcType-org.apache.arrow.adapter.jdbc.JdbcFieldInfo-java.util.Calendar-

+--------------------+--------------------+-------+
| JDBC Type          | Arrow Type         | Notes |
+====================+====================+=======+
| ARRAY              | List               | \(1)  |
+--------------------+--------------------+-------+
| BIGINT             | Int64              |       |
+--------------------+--------------------+-------+
| BINARY             | Binary             |       |
+--------------------+--------------------+-------+
| BIT                | Bool               |       |
+--------------------+--------------------+-------+
| BLOB               | Binary             |       |
+--------------------+--------------------+-------+
| BOOLEAN            | Bool               |       |
+--------------------+--------------------+-------+
| CHAR               | Utf8               |       |
+--------------------+--------------------+-------+
| CLOB               | Utf8               |       |
+--------------------+--------------------+-------+
| DATE               | Date32             |       |
+--------------------+--------------------+-------+
| DECIMAL            | Decimal128         | \(2)  |
+--------------------+--------------------+-------+
| DOUBLE             | Double             |       |
+--------------------+--------------------+-------+
| FLOAT              | Float              |       |
+--------------------+--------------------+-------+
| INTEGER            | Int32              |       |
+--------------------+--------------------+-------+
| LONGVARBINARY      | Binary             |       |
+--------------------+--------------------+-------+
| LONGNVARCHAR       | Utf8               |       |
+--------------------+--------------------+-------+
| LONGVARCHAR        | Utf8               |       |
+--------------------+--------------------+-------+
| NCHAR              | Utf8               |       |
+--------------------+--------------------+-------+
| NULL               | Null               |       |
+--------------------+--------------------+-------+
| NUMERIC            | Decimal128         |       |
+--------------------+--------------------+-------+
| NVARCHAR           | Utf8               |       |
+--------------------+--------------------+-------+
| REAL               | Float              |       |
+--------------------+--------------------+-------+
| SMALLINT           | Int16              |       |
+--------------------+--------------------+-------+
| STRUCT             | Struct             | \(3)  |
+--------------------+--------------------+-------+
| TIME               | Time32[ms]         |       |
+--------------------+--------------------+-------+
| TIMESTAMP          | Timestamp[ms]      | \(4)  |
+--------------------+--------------------+-------+
| TINYINT            | Int8               |       |
+--------------------+--------------------+-------+
| VARBINARY          | Binary             |       |
+--------------------+--------------------+-------+
| VARCHAR            | Utf8               |       |
+--------------------+--------------------+-------+

* \(1) The list value type must be explicitly configured and cannot be
  inferred. Use `setArraySubTypeByColumnIndexMap`_ or
  `setArraySubTypeByColumnNameMap`_.
* \(2) By default, the scale of decimal values must match the scale in
  the type exactly; precision is allowed to be any value greater or
  equal to the type precision.  If there is a mismatch, by default, an
  exception will be thrown.  This can be configured by setting a
  different RoundingMode with setBigDecimalRoundingMode.
* \(3) Not fully supported: while the type conversion is defined, the
  value conversion is not. See ARROW-17006_.
* \(4) If a Calendar is provided, then the timestamp will have the
  timezone of the calendar, else it will be a timestamp without
  timezone.

.. _setArraySubTypeByColumnIndexMap: https://arrow.apache.org/docs/java/reference/org/apache/arrow/adapter/jdbc/JdbcToArrowConfigBuilder.html#setArraySubTypeByColumnIndexMap-java.util.Map-
.. _setArraySubTypeByColumnNameMap: https://arrow.apache.org/docs/java/reference/org/apache/arrow/adapter/jdbc/JdbcToArrowConfigBuilder.html#setArraySubTypeByColumnNameMap-java.util.Map-
.. _ARROW-17006: https://issues.apache.org/jira/browse/ARROW-17006
