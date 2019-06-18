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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
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

import org.apache.arrow.memory.RootAllocator;
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
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.holders.NullableBigIntHolder;
import org.apache.arrow.vector.holders.NullableBitHolder;
import org.apache.arrow.vector.holders.NullableDateMilliHolder;
import org.apache.arrow.vector.holders.NullableDecimalHolder;
import org.apache.arrow.vector.holders.NullableFloat4Holder;
import org.apache.arrow.vector.holders.NullableFloat8Holder;
import org.apache.arrow.vector.holders.NullableIntHolder;
import org.apache.arrow.vector.holders.NullableSmallIntHolder;
import org.apache.arrow.vector.holders.NullableTimeMilliHolder;
import org.apache.arrow.vector.holders.NullableTinyIntHolder;
import org.apache.arrow.vector.holders.NullableVarCharHolder;
import org.apache.arrow.vector.holders.VarBinaryHolder;
import org.apache.arrow.vector.holders.VarCharHolder;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.DecimalUtility;

import com.google.common.base.Preconditions;

import io.netty.buffer.ArrowBuf;

/**
 * Class that does most of the work to convert JDBC ResultSet data into Arrow columnar format Vector objects.
 *
 * @since 0.10.0
 */
public class JdbcToArrowUtils {

  private static final int DEFAULT_BUFFER_SIZE = 256;
  private static final int DEFAULT_STREAM_BUFFER_SIZE = 1024;
  private static final int DEFAULT_CLOB_SUBSTRING_READ_SIZE = 256;

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
    Preconditions.checkNotNull(rsmd, "JDBC ResultSetMetaData object can't be null");
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
   * If {@link JdbcToArrowConfig#getIncludeMetadata()} returns <code>true</code>, the following fields
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


    final ArrowType arrowType;

    switch (fieldInfo.getJdbcType()) {
      case Types.BOOLEAN:
      case Types.BIT:
        arrowType = new ArrowType.Bool();
        break;
      case Types.TINYINT:
        arrowType = new ArrowType.Int(8, true);
        break;
      case Types.SMALLINT:
        arrowType = new ArrowType.Int(16, true);
        break;
      case Types.INTEGER:
        arrowType = new ArrowType.Int(32, true);
        break;
      case Types.BIGINT:
        arrowType = new ArrowType.Int(64, true);
        break;
      case Types.NUMERIC:
      case Types.DECIMAL:
        int precision = fieldInfo.getPrecision();
        int scale = fieldInfo.getScale();
        arrowType = new ArrowType.Decimal(precision, scale);
        break;
      case Types.REAL:
      case Types.FLOAT:
        arrowType = new ArrowType.FloatingPoint(SINGLE);
        break;
      case Types.DOUBLE:
        arrowType = new ArrowType.FloatingPoint(DOUBLE);
        break;
      case Types.CHAR:
      case Types.NCHAR:
      case Types.VARCHAR:
      case Types.NVARCHAR:
      case Types.LONGVARCHAR:
      case Types.LONGNVARCHAR:
      case Types.CLOB:
        arrowType = new ArrowType.Utf8();
        break;
      case Types.DATE:
        arrowType = new ArrowType.Date(DateUnit.MILLISECOND);
        break;
      case Types.TIME:
        arrowType = new ArrowType.Time(TimeUnit.MILLISECOND, 32);
        break;
      case Types.TIMESTAMP:
        arrowType = new ArrowType.Timestamp(TimeUnit.MILLISECOND, timezone);
        break;
      case Types.BINARY:
      case Types.VARBINARY:
      case Types.LONGVARBINARY:
      case Types.BLOB:
        arrowType = new ArrowType.Binary();
        break;
      case Types.ARRAY:
        arrowType = new ArrowType.List();
        break;
      default:
        // no-op, shouldn't get here
        arrowType = null;
        break;
    }

    return arrowType;
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

  private static void allocateVectors(VectorSchemaRoot root, int size) {
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

    Preconditions.checkNotNull(rs, "JDBC ResultSet object can't be null");
    Preconditions.checkNotNull(root, "Vector Schema cannot be null");
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

    Preconditions.checkNotNull(rs, "JDBC ResultSet object can't be null");
    Preconditions.checkNotNull(root, "JDBC ResultSet object can't be null");
    Preconditions.checkNotNull(config, "JDBC-to-Arrow configuration cannot be null");

    ResultSetMetaData rsmd = rs.getMetaData();
    int columnCount = rsmd.getColumnCount();

    allocateVectors(root, DEFAULT_BUFFER_SIZE);

    int rowCount = 0;
    while (rs.next()) {
      for (int i = 1; i <= columnCount; i++) {
        jdbcToFieldVector(
            rs,
            i,
            rs.getMetaData().getColumnType(i),
            rowCount,
            root.getVector(rsmd.getColumnName(i)),
            config);
      }
      rowCount++;
    }
    root.setRowCount(rowCount);
  }

  private static void jdbcToFieldVector(
      ResultSet rs,
      int columnIndex,
      int jdbcColType,
      int rowCount,
      FieldVector vector,
      JdbcToArrowConfig config)
          throws SQLException, IOException {

    final Calendar calendar = config.getCalendar();

    switch (jdbcColType) {
      case Types.BOOLEAN:
      case Types.BIT:
        updateVector((BitVector) vector,
                rs.getBoolean(columnIndex), !rs.wasNull(), rowCount);
        break;
      case Types.TINYINT:
        updateVector((TinyIntVector) vector,
                rs.getInt(columnIndex), !rs.wasNull(), rowCount);
        break;
      case Types.SMALLINT:
        updateVector((SmallIntVector) vector,
                rs.getInt(columnIndex), !rs.wasNull(), rowCount);
        break;
      case Types.INTEGER:
        updateVector((IntVector) vector,
                rs.getInt(columnIndex), !rs.wasNull(), rowCount);
        break;
      case Types.BIGINT:
        updateVector((BigIntVector) vector,
                rs.getLong(columnIndex), !rs.wasNull(), rowCount);
        break;
      case Types.NUMERIC:
      case Types.DECIMAL:
        updateVector((DecimalVector) vector,
                rs.getBigDecimal(columnIndex), !rs.wasNull(), rowCount);
        break;
      case Types.REAL:
      case Types.FLOAT:
        updateVector((Float4Vector) vector,
                rs.getFloat(columnIndex), !rs.wasNull(), rowCount);
        break;
      case Types.DOUBLE:
        updateVector((Float8Vector) vector,
                rs.getDouble(columnIndex), !rs.wasNull(), rowCount);
        break;
      case Types.CHAR:
      case Types.NCHAR:
      case Types.VARCHAR:
      case Types.NVARCHAR:
      case Types.LONGVARCHAR:
      case Types.LONGNVARCHAR:
        updateVector((VarCharVector) vector,
                rs.getString(columnIndex), !rs.wasNull(), rowCount);
        break;
      case Types.DATE:
        final Date date;
        if (calendar != null) {
          date = rs.getDate(columnIndex, calendar);
        } else {
          date = rs.getDate(columnIndex);
        }

        updateVector((DateMilliVector) vector, date, !rs.wasNull(), rowCount);
        break;
      case Types.TIME:
        final Time time;
        if (calendar != null) {
          time = rs.getTime(columnIndex, calendar);
        } else {
          time = rs.getTime(columnIndex);
        }

        updateVector((TimeMilliVector) vector, time, !rs.wasNull(), rowCount);
        break;
      case Types.TIMESTAMP:
        final Timestamp ts;
        if (calendar != null) {
          ts = rs.getTimestamp(columnIndex, calendar);
        } else {
          ts = rs.getTimestamp(columnIndex);
        }

        // TODO: Need to handle precision such as milli, micro, nano
        updateVector((TimeStampVector) vector, ts, !rs.wasNull(), rowCount);
        break;
      case Types.BINARY:
      case Types.VARBINARY:
      case Types.LONGVARBINARY:
        updateVector((VarBinaryVector) vector,
                rs.getBinaryStream(columnIndex), !rs.wasNull(), rowCount);
        break;
      case Types.ARRAY:
        updateVector((ListVector) vector, rs, columnIndex, rowCount, config);
        break;
      case Types.CLOB:
        updateVector((VarCharVector) vector,
                rs.getClob(columnIndex), !rs.wasNull(), rowCount);
        break;
      case Types.BLOB:
        updateVector((VarBinaryVector) vector,
                rs.getBlob(columnIndex), !rs.wasNull(), rowCount);
        break;

      default:
        // no-op, shouldn't get here
        break;
    }
  }

  private static void updateVector(BitVector bitVector, boolean value, boolean isNonNull, int rowCount) {
    NullableBitHolder holder = new NullableBitHolder();
    holder.isSet = isNonNull ? 1 : 0;
    if (isNonNull) {
      holder.value = value ? 1 : 0;
    }
    bitVector.setSafe(rowCount, holder);
    bitVector.setValueCount(rowCount + 1);
  }

  private static void updateVector(TinyIntVector tinyIntVector, int value, boolean isNonNull, int rowCount) {
    NullableTinyIntHolder holder = new NullableTinyIntHolder();
    holder.isSet = isNonNull ? 1 : 0;
    if (isNonNull) {
      holder.value = (byte) value;
    }
    tinyIntVector.setSafe(rowCount, holder);
    tinyIntVector.setValueCount(rowCount + 1);
  }

  private static void updateVector(SmallIntVector smallIntVector, int value, boolean isNonNull, int rowCount) {
    NullableSmallIntHolder holder = new NullableSmallIntHolder();
    holder.isSet = isNonNull ? 1 : 0;
    if (isNonNull) {
      holder.value = (short) value;
    }
    smallIntVector.setSafe(rowCount, holder);
    smallIntVector.setValueCount(rowCount + 1);
  }

  private static void updateVector(IntVector intVector, int value, boolean isNonNull, int rowCount) {
    NullableIntHolder holder = new NullableIntHolder();
    holder.isSet = isNonNull ? 1 : 0;
    if (isNonNull) {
      holder.value = value;
    }
    intVector.setSafe(rowCount, holder);
    intVector.setValueCount(rowCount + 1);
  }

  private static void updateVector(BigIntVector bigIntVector, long value, boolean isNonNull, int rowCount) {
    NullableBigIntHolder holder = new NullableBigIntHolder();
    holder.isSet = isNonNull ? 1 : 0;
    if (isNonNull) {
      holder.value = value;
    }
    bigIntVector.setSafe(rowCount, holder);
    bigIntVector.setValueCount(rowCount + 1);
  }

  private static void updateVector(DecimalVector decimalVector, BigDecimal value, boolean isNonNull, int rowCount) {
    NullableDecimalHolder holder = new NullableDecimalHolder();
    holder.isSet = isNonNull ? 1 : 0;
    if (isNonNull) {
      holder.precision = value.precision();
      holder.scale = value.scale();
      holder.buffer = decimalVector.getAllocator().buffer(DEFAULT_BUFFER_SIZE);
      holder.start = 0;
      DecimalUtility.writeBigDecimalToArrowBuf(value, holder.buffer, holder.start);
    }
    decimalVector.setSafe(rowCount, holder);
    decimalVector.setValueCount(rowCount + 1);
  }

  private static void updateVector(Float4Vector float4Vector, float value, boolean isNonNull, int rowCount) {
    NullableFloat4Holder holder = new NullableFloat4Holder();
    holder.isSet = isNonNull ? 1 : 0;
    if (isNonNull) {
      holder.value = value;
    }
    float4Vector.setSafe(rowCount, holder);
    float4Vector.setValueCount(rowCount + 1);
  }

  private static void updateVector(Float8Vector float8Vector, double value, boolean isNonNull, int rowCount) {
    NullableFloat8Holder holder = new NullableFloat8Holder();
    holder.isSet = isNonNull ? 1 : 0;
    if (isNonNull) {
      holder.value = value;
    }
    float8Vector.setSafe(rowCount, holder);
    float8Vector.setValueCount(rowCount + 1);
  }

  private static void updateVector(VarCharVector varcharVector, String value, boolean isNonNull, int rowCount) {
    NullableVarCharHolder holder = new NullableVarCharHolder();
    holder.isSet = isNonNull ? 1 : 0;
    varcharVector.setIndexDefined(rowCount);
    if (isNonNull) {
      byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
      holder.buffer = varcharVector.getAllocator().buffer(bytes.length);
      holder.buffer.setBytes(0, bytes, 0, bytes.length);
      holder.start = 0;
      holder.end = bytes.length;
    } else {
      holder.buffer = varcharVector.getAllocator().buffer(0);
    }
    varcharVector.setSafe(rowCount, holder);
    varcharVector.setValueCount(rowCount + 1);
  }

  private static void updateVector(DateMilliVector dateMilliVector, Date date, boolean isNonNull, int rowCount) {
    NullableDateMilliHolder holder = new NullableDateMilliHolder();
    holder.isSet = isNonNull ? 1 : 0;
    if (isNonNull) {
      holder.value = date.getTime();
    }
    dateMilliVector.setSafe(rowCount, holder);
    dateMilliVector.setValueCount(rowCount + 1);
  }

  private static void updateVector(TimeMilliVector timeMilliVector, Time time, boolean isNonNull, int rowCount) {
    NullableTimeMilliHolder holder = new NullableTimeMilliHolder();
    holder.isSet = isNonNull ? 1 : 0;
    if (isNonNull && time != null) {
      holder.value = (int) time.getTime();
    }
    timeMilliVector.setSafe(rowCount, holder);
    timeMilliVector.setValueCount(rowCount + 1);
  }

  private static void updateVector(
      TimeStampVector timeStampVector,
      Timestamp timestamp,
      boolean isNonNull,
      int rowCount) {
    //TODO: Need to handle precision such as milli, micro, nano
    timeStampVector.setValueCount(rowCount + 1);
    if (timestamp != null) {
      timeStampVector.setSafe(rowCount, timestamp.getTime());
    } else {
      timeStampVector.setNull(rowCount);
    }
  }

  private static void updateVector(
      VarBinaryVector varBinaryVector,
      InputStream is,
      boolean isNonNull,
      int rowCount) throws IOException {
    varBinaryVector.setValueCount(rowCount + 1);
    if (isNonNull && is != null) {
      VarBinaryHolder holder = new VarBinaryHolder();
      ArrowBuf arrowBuf = varBinaryVector.getDataBuffer();
      holder.start = 0;
      byte[] bytes = new byte[DEFAULT_STREAM_BUFFER_SIZE];
      int total = 0;
      while (true) {
        int read = is.read(bytes, 0, DEFAULT_STREAM_BUFFER_SIZE);
        if (read == -1) {
          break;
        }
        arrowBuf.setBytes(total, bytes, total, read);
        total += read;
      }
      holder.end = total;
      holder.buffer = arrowBuf;
      varBinaryVector.set(rowCount, holder);
      varBinaryVector.setIndexDefined(rowCount);
    } else {
      varBinaryVector.setNull(rowCount);
    }
  }

  private static void updateVector(
      VarCharVector varcharVector,
      Clob clob,
      boolean isNonNull,
      int rowCount) throws SQLException, IOException {
    varcharVector.setValueCount(rowCount + 1);
    if (isNonNull && clob != null) {
      VarCharHolder holder = new VarCharHolder();
      ArrowBuf arrowBuf = varcharVector.getDataBuffer();
      holder.start = 0;
      long length = clob.length();
      int read = 1;
      int readSize = length < DEFAULT_CLOB_SUBSTRING_READ_SIZE ? (int) length : DEFAULT_CLOB_SUBSTRING_READ_SIZE;
      int totalBytes = 0;
      while (read <= length) {
        String str = clob.getSubString(read, readSize);
        byte[] bytes = str.getBytes(StandardCharsets.UTF_8);
        arrowBuf.setBytes(totalBytes, new ByteArrayInputStream(bytes, 0, bytes.length), bytes.length);
        totalBytes += bytes.length;
        read += readSize;
      }
      holder.end = totalBytes;
      holder.buffer = arrowBuf;
      varcharVector.set(rowCount, holder);
      varcharVector.setIndexDefined(rowCount);
    } else {
      varcharVector.setNull(rowCount);
    }
  }

  private static void updateVector(VarBinaryVector varBinaryVector, Blob blob, boolean isNonNull, int rowCount)
      throws SQLException, IOException {
    updateVector(varBinaryVector, blob != null ? blob.getBinaryStream() : null, isNonNull, rowCount);
  }

  private static void updateVector(
      ListVector listVector,
      ResultSet resultSet,
      int arrayIndex,
      int rowCount,
      JdbcToArrowConfig config)
      throws SQLException, IOException {

    final JdbcFieldInfo fieldInfo = getJdbcFieldInfoForArraySubType(resultSet.getMetaData(), arrayIndex, config);
    if (fieldInfo == null) {
      throw new IllegalArgumentException("Column " + arrayIndex + " is an array of unknown type.");
    }

    final int valueCount = listVector.getValueCount();
    final Array array = resultSet.getArray(arrayIndex);

    FieldVector fieldVector = listVector.getDataVector();
    int arrayRowCount = 0;

    if (!resultSet.wasNull()) {
      listVector.startNewValue(rowCount);

      try (ResultSet rs = array.getResultSet()) {

        while (rs.next()) {
          jdbcToFieldVector(
              rs,
              JDBC_ARRAY_VALUE_COLUMN,
              fieldInfo.getJdbcType(),
              valueCount + arrayRowCount,
              fieldVector,
              config);
          arrayRowCount++;
        }
      }

      listVector.endValue(rowCount, arrayRowCount);
    }

    listVector.setValueCount(valueCount + arrayRowCount);
  }
}
