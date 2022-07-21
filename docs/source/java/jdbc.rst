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

The mapping from JDBC type to Arrow type can be overridden via the
``JdbcToArrowConfig``, but it is not possible to customize the
conversion from JDBC value to Arrow value itself, nor is it possible
to define a conversion for an unsupported type.

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
| FLOAT              | Float32            |       |
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
| REAL               | Float32            |       |
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

VectorSchemaRoot to PreparedStatement Parameter Conversion
==========================================================

The adapter can bind rows of Arrow data from a VectorSchemaRoot to
parameters of a JDBC PreparedStatement.  This can be accessed via the
JdbcParameterBinder class.  Each call to next() will bind parameters
from the next row of data, and then the application can execute the
statement, call addBatch(), etc. as desired.  Null values will lead to
a setNull call with an appropriate JDBC type code (listed below).

.. code-block:: java

   final JdbcParameterBinder binder =
       JdbcParameterBinder.builder(statement, root).bindAll().build();
   while (binder.next()) {
       statement.executeUpdate();
   }
   // Use a VectorLoader to update the root
   binder.reset();
   while (binder.next()) {
       statement.executeUpdate();
   }

The mapping of vectors to parameters, the JDBC type code used by the
converters, and the type conversions themselves can all be customized:

.. code-block:: java

   final JdbcParameterBinder binder =
       JdbcParameterBinder.builder(statement, root)
           .bind(/*parameterIndex*/2, /*columnIndex*/0)
           .bind(/*parameterIndex*/1, customColumnBinderInstance)
           .build();

Type Mapping
------------

The Arrow to JDBC type mapping can be obtained at runtime via
a method on ColumnBinder.

+----------------------------+----------------------------+-------+
| Arrow Type                 | JDBC Type                  | Notes |
+============================+============================+=======+
| Binary                     | VARBINARY (setBytes)       |       |
+----------------------------+----------------------------+-------+
| Bool                       | BOOLEAN (setBoolean)       |       |
+----------------------------+----------------------------+-------+
| Date32                     | DATE (setDate)             |       |
+----------------------------+----------------------------+-------+
| Date64                     | DATE (setDate)             |       |
+----------------------------+----------------------------+-------+
| Decimal128                 | DECIMAL (setBigDecimal)    |       |
+----------------------------+----------------------------+-------+
| Decimal256                 | DECIMAL (setBigDecimal)    |       |
+----------------------------+----------------------------+-------+
| FixedSizeBinary            | BINARY (setBytes)          |       |
+----------------------------+----------------------------+-------+
| Float32                    | REAL (setFloat)            |       |
+----------------------------+----------------------------+-------+
| Int8                       | TINYINT (setByte)          |       |
+----------------------------+----------------------------+-------+
| Int16                      | SMALLINT (setShort)        |       |
+----------------------------+----------------------------+-------+
| Int32                      | INTEGER (setInt)           |       |
+----------------------------+----------------------------+-------+
| Int64                      | BIGINT (setLong)           |       |
+----------------------------+----------------------------+-------+
| LargeBinary                | LONGVARBINARY (setBytes)   |       |
+----------------------------+----------------------------+-------+
| LargeUtf8                  | LONGVARCHAR (setString)    | \(1)  |
+----------------------------+----------------------------+-------+
| Time[s]                    | TIME (setTime)             |       |
+----------------------------+----------------------------+-------+
| Time[ms]                   | TIME (setTime)             |       |
+----------------------------+----------------------------+-------+
| Time[us]                   | TIME (setTime)             |       |
+----------------------------+----------------------------+-------+
| Time[ns]                   | TIME (setTime)             |       |
+----------------------------+----------------------------+-------+
| Timestamp[s]               | TIMESTAMP (setTimestamp)   | \(2)  |
+----------------------------+----------------------------+-------+
| Timestamp[ms]              | TIMESTAMP (setTimestamp)   | \(2)  |
+----------------------------+----------------------------+-------+
| Timestamp[us]              | TIMESTAMP (setTimestamp)   | \(2)  |
+----------------------------+----------------------------+-------+
| Timestamp[ns]              | TIMESTAMP (setTimestamp)   | \(2)  |
+----------------------------+----------------------------+-------+
| Utf8                       | VARCHAR (setString)        |       |
+----------------------------+----------------------------+-------+

* \(1) Strings longer than Integer.MAX_VALUE bytes (the maximum length
  of a Java ``byte[]``) will cause a runtime exception.
* \(2) If the timestamp has a timezone, the JDBC type defaults to
  TIMESTAMP_WITH_TIMEZONE.  If the timestamp has no timezone,
  technically there is not a correct conversion from Arrow value to
  JDBC value, because a JDBC Timestamp is in UTC, and we have no
  timezone information.  In this case, the default binder will call
  `setTimestamp(int, Timestamp)
  <https://docs.oracle.com/en/java/javase/11/docs/api/java.sql/java/sql/PreparedStatement.html#setTimestamp(int,java.sql.Timestamp)>`_,
  which will lead to the driver using the "default timezone" (that of
  the Java VM).
