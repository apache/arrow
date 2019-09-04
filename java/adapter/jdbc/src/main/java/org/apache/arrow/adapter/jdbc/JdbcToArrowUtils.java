/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.arrow.adapter.jdbc;

import static org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE;
import static org.apache.arrow.vector.types.FloatingPointPrecision.SINGLE;

import java.io.IOException;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;

import org.apache.arrow.adapter.jdbc.consumer.ArrayConsumer;
import org.apache.arrow.adapter.jdbc.consumer.BigIntConsumer;
import org.apache.arrow.adapter.jdbc.consumer.BinaryConsumer;
import org.apache.arrow.adapter.jdbc.consumer.BitConsumer;
import org.apache.arrow.adapter.jdbc.consumer.BlobConsumer;
import org.apache.arrow.adapter.jdbc.consumer.ClobConsumer;
import org.apache.arrow.adapter.jdbc.consumer.CompositeJdbcConsumer;
import org.apache.arrow.adapter.jdbc.consumer.DateConsumer;
import org.apache.arrow.adapter.jdbc.consumer.DecimalConsumer;
import org.apache.arrow.adapter.jdbc.consumer.DoubleConsumer;
import org.apache.arrow.adapter.jdbc.consumer.FloatConsumer;
import org.apache.arrow.adapter.jdbc.consumer.IntConsumer;
import org.apache.arrow.adapter.jdbc.consumer.JdbcConsumer;
import org.apache.arrow.adapter.jdbc.consumer.SmallIntConsumer;
import org.apache.arrow.adapter.jdbc.consumer.TimeConsumer;
import org.apache.arrow.adapter.jdbc.consumer.TimestampConsumer;
import org.apache.arrow.adapter.jdbc.consumer.TinyIntConsumer;
import org.apache.arrow.adapter.jdbc.consumer.VarCharConsumer;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.BaseFixedWidthVector;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeStampMilliTZVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * Class that does most of the work to convert JDBC ResultSet data into Arrow columnar format Vector objects.
 *
 * @since 0.10.0
 */
public class JdbcToArrowUtils {

  public static final int DEFAULT_BUFFER_SIZE = 256;

  private static final int JDBC_ARRAY_VALUE_COLUMN = 2;

  /**
   * Returns the instance of a {java.util.Calendar} with the UTC time zone and root locale.
   */
  public static Calendar getUtcCalendar() {
    return Calendar.getInstance(TimeZone.getTimeZone("UTC"), Locale.ROOT);
  }

  /**
   * Create Arrow {@link Schema} object for the given JDBC {@link ResultSetMetaData}.
   *
   * @param rsmd The ResultSetMetaData containing the results, to read the JDBC metadata from.
   * @param calendar The calendar to use the time zone field of, to construct Timestamp fields from.
   * @return {@link Schema}
   * @throws SQLException on error
   */
  public static Schema jdbcToArrowSchema(ResultSetMetaData rsmd, Calendar calendar) throws SQLException {
    Preconditions.checkNotNull(calendar, "Calendar object can't be null");

    return jdbcToArrowSchema(rsmd, new JdbcToArrowConfig(new RootAllocator(0), calendar));
  }

  /**
   * Create Arrow {@link Schema} object for the given JDBC {@link java.sql.ResultSetMetaData}.
   * <p>
   * The {@link JdbcToArrowUtils#getArrowTypeForJdbcField(JdbcFieldInfo, Calendar)} method is used to construct a
   * {@link org.apache.arrow.vector.types.pojo.ArrowType} for each field in the {@link java.sql.ResultSetMetaData}.
   * </p>
   * <p>
   * If {@link JdbcToArrowConfig#shouldIncludeMetadata()} returns <code>true</code>, the following fields
   * will be added to the {@link FieldType#getMetadata()}:
   * <ul>
   *  <li>{@link Constants#SQL_CATALOG_NAME_KEY} representing {@link ResultSetMetaData#getCatalogName(int)}</li>
   *  <li>{@link Constants#SQL_TABLE_NAME_KEY} representing {@link ResultSetMetaData#getTableName(int)}</li>
   *  <li>{@link Constants#SQL_COLUMN_NAME_KEY} representing {@link ResultSetMetaData#getColumnName(int)}</li>
   *  <li>{@link Constants#SQL_TYPE_KEY} representing {@link ResultSetMetaData#getColumnTypeName(int)}</li>
   * </ul>
   * </p>
   * <p>
   * If any columns are of type {@link java.sql.Types#ARRAY}, the configuration object will be used to look up
   * the array sub-type field.  The {@link JdbcToArrowConfig#getArraySubTypeByColumnIndex(int)} method will be
   * checked first, followed by the {@link JdbcToArrowConfig#getArraySubTypeByColumnName(String)} method.
   * </p>
   *
   * @param rsmd The ResultSetMetaData containing the results, to read the JDBC metadata from.
   * @param config The configuration to use when constructing the schema.
   * @return {@link Schema}
   * @throws SQLException on error
   * @throws IllegalArgumentException if <code>rsmd</code> contains an {@link java.sql.Types#ARRAY} but the
   *                                  <code>config</code> does not have a sub-type definition for it.
   */
  public static Schema jdbcToArrowSchema(ResultSetMetaData rsmd, JdbcToArrowConfig config) throws SQLException {
    Preconditions.checkNotNull(rsmd, "JDBC ResultSetMetaData object can't be null");
    Preconditions.checkNotNull(config, "The configuration object must not be null");

    List<Field> fields = new ArrayList<>();
    int columnCount = rsmd.getColumnCount();
    for (int i = 1; i <= columnCount; i++) {
      final String columnName = rsmd.getColumnName(i);

      final Map<String, String> metadata;
      if (config.shouldIncludeMetadata()) {
        metadata = new HashMap<>();
        metadata.put(Constants.SQL_CATALOG_NAME_KEY, rsmd.getCatalogName(i));
        metadata.put(Constants.SQL_TABLE_NAME_KEY, rsmd.getTableName(i));
        metadata.put(Constants.SQL_COLUMN_NAME_KEY, columnName);
        metadata.put(Constants.SQL_TYPE_KEY, rsmd.getColumnTypeName(i));

      } else {
        metadata = null;
      }

      final ArrowType arrowType = getArrowTypeForJdbcField(new JdbcFieldInfo(rsmd, i), config.getCalendar());
      if (arrowType != null) {
        final FieldType fieldType = new FieldType(true, arrowType, /* dictionary encoding */ null, metadata);

        List<Field> children = null;
        if (arrowType.getTypeID() == ArrowType.List.TYPE_TYPE) {
          final JdbcFieldInfo arrayFieldInfo = getJdbcFieldInfoForArraySubType(rsmd, i, config);
          if (arrayFieldInfo == null) {
            throw new IllegalArgumentException("Configuration does not provide a mapping for array column " + i);
          }
          children = new ArrayList<Field>();
          final ArrowType childType =
              getArrowTypeForJdbcField(arrayFieldInfo, config.getCalendar());
          children.add(new Field("child", FieldType.nullable(childType), null));
        }

        fields.add(new Field(columnName, fieldType, children));
      }
    }

    return new Schema(fields, null);
  }

  /**
   * Creates an {@link org.apache.arrow.vector.types.pojo.ArrowType}
   * from the {@link JdbcFieldInfo} and {@link java.util.Calendar}.
   *
   * <p>This method currently performs following type mapping for JDBC SQL data types to corresponding Arrow data types.
   *
   * <ul>
   *   <li>CHAR --> ArrowType.Utf8</li>
   *   <li>NCHAR --> ArrowType.Utf8</li>
   *   <li>VARCHAR --> ArrowType.Utf8</li>
   *   <li>NVARCHAR --> ArrowType.Utf8</li>
   *   <li>LONGVARCHAR --> ArrowType.Utf8</li>
   *   <li>LONGNVARCHAR --> ArrowType.Utf8</li>
   *   <li>NUMERIC --> ArrowType.Decimal(precision, scale)</li>
   *   <li>DECIMAL --> ArrowType.Decimal(precision, scale)</li>
   *   <li>BIT --> ArrowType.Bool</li>
   *   <li>TINYINT --> ArrowType.Int(8, signed)</li>
   *   <li>SMALLINT --> ArrowType.Int(16, signed)</li>
   *   <li>INTEGER --> ArrowType.Int(32, signed)</li>
   *   <li>BIGINT --> ArrowType.Int(64, signed)</li>
   *   <li>REAL --> ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)</li>
   *   <li>FLOAT --> ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)</li>
   *   <li>DOUBLE --> ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)</li>
   *   <li>BINARY --> ArrowType.Binary</li>
   *   <li>VARBINARY --> ArrowType.Binary</li>
   *   <li>LONGVARBINARY --> ArrowType.Binary</li>
   *   <li>DATE --> ArrowType.Date(DateUnit.MILLISECOND)</li>
   *   <li>TIME --> ArrowType.Time(TimeUnit.MILLISECOND, 32)</li>
   *   <li>TIMESTAMP --> ArrowType.Timestamp(TimeUnit.MILLISECOND, calendar timezone)</li>
   *   <li>CLOB --> ArrowType.Utf8</li>
   *   <li>BLOB --> ArrowType.Binary</li>
   * </ul>
   *
   * @param fieldInfo The field information to construct the <code>ArrowType</code> from.
   * @param calendar The calendar to use when constructing the <code>ArrowType.Timestamp</code>
   *                 for {@link java.sql.Types#TIMESTAMP} types.
   * @return The corresponding <code>ArrowType</code>.
   * @throws NullPointerException if either <code>fieldInfo</code> or <code>calendar</code> are <code>null</code>.
   */
  public static ArrowType getArrowTypeForJdbcField(JdbcFieldInfo fieldInfo, Calendar calendar) {
    Preconditions.checkNotNull(fieldInfo, "JdbcFieldInfo object cannot be null");

    final String timezone;
    if (calendar != null) {
      timezone = calendar.getTimeZone().getID();
    } else {
      timezone = null;
    }

    switch (fieldInfo.getJdbcType()) {
      case Types.BOOLEAN:
      case Types.BIT:
        return new ArrowType.Bool();
      case Types.TINYINT:
        return new ArrowType.Int(8, true);
      case Types.SMALLINT:
        return new ArrowType.Int(16, true);
      case Types.INTEGER:
        return new ArrowType.Int(32, true);
      case Types.BIGINT:
        return new ArrowType.Int(64, true);
      case Types.NUMERIC:
      case Types.DECIMAL:
        int precision = fieldInfo.getPrecision();
        int scale = fieldInfo.getScale();
        return new ArrowType.Decimal(precision, scale);
      case Types.REAL:
      case Types.FLOAT:
        return new ArrowType.FloatingPoint(SINGLE);
      case Types.DOUBLE:
        return new ArrowType.FloatingPoint(DOUBLE);
      case Types.CHAR:
      case Types.NCHAR:
      case Types.VARCHAR:
      case Types.NVARCHAR:
      case Types.LONGVARCHAR:
      case Types.LONGNVARCHAR:
      case Types.CLOB:
        return new ArrowType.Utf8();
      case Types.DATE:
        return new ArrowType.Date(DateUnit.MILLISECOND);
      case Types.TIME:
        return new ArrowType.Time(TimeUnit.MILLISECOND, 32);
      case Types.TIMESTAMP:
        return new ArrowType.Timestamp(TimeUnit.MILLISECOND, timezone);
      case Types.BINARY:
      case Types.VARBINARY:
      case Types.LONGVARBINARY:
      case Types.BLOB:
        return new ArrowType.Binary();
      case Types.ARRAY:
        return new ArrowType.List();
      default:
        // no-op, shouldn't get here
        return null;
    }
  }

  /* Uses the configuration to determine what the array sub-type JdbcFieldInfo is.
   * If no sub-type can be found, returns null.
   */
  private static JdbcFieldInfo getJdbcFieldInfoForArraySubType(
      ResultSetMetaData rsmd,
      int arrayColumn,
      JdbcToArrowConfig config)
          throws SQLException {

    Preconditions.checkNotNull(rsmd, "ResultSet MetaData object cannot be null");
    Preconditions.checkNotNull(config, "Configuration must not be null");
    Preconditions.checkArgument(
        arrayColumn > 0,
        "ResultSetMetaData columns start with 1; column cannot be less than 1");
    Preconditions.checkArgument(
        arrayColumn <= rsmd.getColumnCount(),
        "Column number cannot be more than the number of columns");

    JdbcFieldInfo fieldInfo = config.getArraySubTypeByColumnIndex(arrayColumn);
    if (fieldInfo == null) {
      fieldInfo = config.getArraySubTypeByColumnName(rsmd.getColumnName(arrayColumn));
    }
    return fieldInfo;
  }

  static void allocateVectors(VectorSchemaRoot root, int size) {
    List<FieldVector> vectors = root.getFieldVectors();
    for (FieldVector fieldVector : vectors) {
      if (fieldVector instanceof BaseFixedWidthVector) {
        ((BaseFixedWidthVector) fieldVector).allocateNew(size);
      } else {
        fieldVector.allocateNew();
      }
      fieldVector.setInitialCapacity(size);
    }
  }

  /**
   * Iterate the given JDBC {@link ResultSet} object to fetch the data and transpose it to populate
   * the given Arrow Vector objects.
   *
   * @param rs       ResultSet to use to fetch the data from underlying database
   * @param root     Arrow {@link VectorSchemaRoot} object to populate
   * @param calendar The calendar to use when reading {@link Date}, {@link Time}, or {@link Timestamp}
   *                 data types from the {@link ResultSet}, or <code>null</code> if not converting.
   * @throws SQLException on error
   */
  public static void jdbcToArrowVectors(ResultSet rs, VectorSchemaRoot root, Calendar calendar)
      throws SQLException, IOException {

    Preconditions.checkNotNull(calendar, "Calendar object can't be null");

    jdbcToArrowVectors(rs, root, new JdbcToArrowConfig(new RootAllocator(0), calendar));
  }

  /**
   * Iterate the given JDBC {@link ResultSet} object to fetch the data and transpose it to populate
   * the given Arrow Vector objects.
   *
   * @param rs     ResultSet to use to fetch the data from underlying database
   * @param root   Arrow {@link VectorSchemaRoot} object to populate
   * @param config The configuration to use when reading the data.
   * @throws SQLException on error
   */
  public static void jdbcToArrowVectors(ResultSet rs, VectorSchemaRoot root, JdbcToArrowConfig config)
      throws SQLException, IOException {

    ResultSetMetaData rsmd = rs.getMetaData();
    int columnCount = rsmd.getColumnCount();
    allocateVectors(root, DEFAULT_BUFFER_SIZE);

    JdbcConsumer[] consumers = new JdbcConsumer[columnCount];
    for (int i = 1; i <= columnCount; i++) {
      consumers[i - 1] = getConsumer(rs, i, rs.getMetaData().getColumnType(i),
          root.getVector(rsmd.getColumnName(i)), config);
    }

    CompositeJdbcConsumer compositeConsumer = null;
    // Only clean resources when occurs error,
    // vectors within consumers are useful and users are responsible for its close.
    try {
      compositeConsumer = new CompositeJdbcConsumer(consumers);
      int readRowCount = 0;
      while (rs.next()) {
        compositeConsumer.consume(rs);
        readRowCount++;
      }
      root.setRowCount(readRowCount);
    } catch (Exception e) {
      // error occurs and clean up resources.
      if (compositeConsumer != null) {
        compositeConsumer.close();
      }
    }
  }

  static JdbcConsumer getConsumer(ResultSet resultSet, int columnIndex, int jdbcColType,
      FieldVector vector, JdbcToArrowConfig config) throws SQLException {
    final Calendar calendar = config.getCalendar();
    switch (jdbcColType) {
      case Types.BOOLEAN:
      case Types.BIT:
        return new BitConsumer((BitVector) vector, columnIndex);
      case Types.TINYINT:
        return new TinyIntConsumer((TinyIntVector) vector, columnIndex);
      case Types.SMALLINT:
        return new SmallIntConsumer((SmallIntVector) vector, columnIndex);
      case Types.INTEGER:
        return new IntConsumer((IntVector) vector, columnIndex);
      case Types.BIGINT:
        return new BigIntConsumer((BigIntVector) vector, columnIndex);
      case Types.NUMERIC:
      case Types.DECIMAL:
        return new DecimalConsumer((DecimalVector) vector, columnIndex);
      case Types.REAL:
      case Types.FLOAT:
        return new FloatConsumer((Float4Vector) vector, columnIndex);
      case Types.DOUBLE:
        return new DoubleConsumer((Float8Vector) vector, columnIndex);
      case Types.CHAR:
      case Types.NCHAR:
      case Types.VARCHAR:
      case Types.NVARCHAR:
      case Types.LONGVARCHAR:
      case Types.LONGNVARCHAR:
        return new VarCharConsumer((VarCharVector) vector, columnIndex);
      case Types.DATE:
        return new DateConsumer((DateMilliVector) vector, columnIndex, calendar);
      case Types.TIME:
        return new TimeConsumer((TimeMilliVector) vector, columnIndex, calendar);
      case Types.TIMESTAMP:
        return new TimestampConsumer((TimeStampMilliTZVector) vector, columnIndex, calendar);
      case Types.BINARY:
      case Types.VARBINARY:
      case Types.LONGVARBINARY:
        return new BinaryConsumer((VarBinaryVector) vector, columnIndex);
      case Types.ARRAY:
        final JdbcFieldInfo fieldInfo = getJdbcFieldInfoForArraySubType(resultSet.getMetaData(), columnIndex, config);
        if (fieldInfo == null) {
          throw new IllegalArgumentException("Column " + columnIndex + " is an array of unknown type.");
        }
        JdbcConsumer delegate = getConsumer(resultSet, JDBC_ARRAY_VALUE_COLUMN,
            fieldInfo.getJdbcType(), ((ListVector)vector).getDataVector(), config);
        return new ArrayConsumer((ListVector) vector, delegate, columnIndex);
      case Types.CLOB:
        return new ClobConsumer((VarCharVector) vector, columnIndex);
      case Types.BLOB:
        BinaryConsumer blobDelegate = new BinaryConsumer((VarBinaryVector) vector, columnIndex);
        return new BlobConsumer(blobDelegate, columnIndex);
      default:
        // no-op, shouldn't get here
        throw new UnsupportedOperationException();
    }
  }
}
