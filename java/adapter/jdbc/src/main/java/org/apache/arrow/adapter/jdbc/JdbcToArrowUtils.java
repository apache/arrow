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
import java.util.List;

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

  /**
   * Create Arrow {@link Schema} object for the given JDBC {@link ResultSetMetaData}.
   *
   * <p>This method currently performs following type mapping for JDBC SQL data types to corresponding Arrow data types.
   *
   * <p>CHAR --> ArrowType.Utf8
   * NCHAR --> ArrowType.Utf8
   * VARCHAR --> ArrowType.Utf8
   * NVARCHAR --> ArrowType.Utf8
   * LONGVARCHAR --> ArrowType.Utf8
   * LONGNVARCHAR --> ArrowType.Utf8
   * NUMERIC --> ArrowType.Decimal(precision, scale)
   * DECIMAL --> ArrowType.Decimal(precision, scale)
   * BIT --> ArrowType.Bool
   * TINYINT --> ArrowType.Int(8, signed)
   * SMALLINT --> ArrowType.Int(16, signed)
   * INTEGER --> ArrowType.Int(32, signed)
   * BIGINT --> ArrowType.Int(64, signed)
   * REAL --> ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)
   * FLOAT --> ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)
   * DOUBLE --> ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)
   * BINARY --> ArrowType.Binary
   * VARBINARY --> ArrowType.Binary
   * LONGVARBINARY --> ArrowType.Binary
   * DATE --> ArrowType.Date(DateUnit.MILLISECOND)
   * TIME --> ArrowType.Time(TimeUnit.MILLISECOND, 32)
   * TIMESTAMP --> ArrowType.Timestamp(TimeUnit.MILLISECOND, timezone=null)
   * CLOB --> ArrowType.Utf8
   * BLOB --> ArrowType.Binary
   *
   * @param rsmd ResultSetMetaData
   * @return {@link Schema}
   * @throws SQLException on error
   */
  public static Schema jdbcToArrowSchema(ResultSetMetaData rsmd, Calendar calendar) throws SQLException {

    Preconditions.checkNotNull(rsmd, "JDBC ResultSetMetaData object can't be null");
    Preconditions.checkNotNull(calendar, "Calendar object can't be null");

    List<Field> fields = new ArrayList<>();
    int columnCount = rsmd.getColumnCount();
    for (int i = 1; i <= columnCount; i++) {
      String columnName = rsmd.getColumnName(i);
      switch (rsmd.getColumnType(i)) {
        case Types.BOOLEAN:
        case Types.BIT:
          fields.add(new Field(columnName, FieldType.nullable(new ArrowType.Bool()), null));
          break;
        case Types.TINYINT:
          fields.add(new Field(columnName, FieldType.nullable(new ArrowType.Int(8, true)), null));
          break;
        case Types.SMALLINT:
          fields.add(new Field(columnName, FieldType.nullable(new ArrowType.Int(16, true)), null));
          break;
        case Types.INTEGER:
          fields.add(new Field(columnName, FieldType.nullable(new ArrowType.Int(32, true)), null));
          break;
        case Types.BIGINT:
          fields.add(new Field(columnName, FieldType.nullable(new ArrowType.Int(64, true)), null));
          break;
        case Types.NUMERIC:
        case Types.DECIMAL:
          int precision = rsmd.getPrecision(i);
          int scale = rsmd.getScale(i);
          fields.add(new Field(columnName, FieldType.nullable(new ArrowType.Decimal(precision, scale)), null));
          break;
        case Types.REAL:
        case Types.FLOAT:
          fields.add(new Field(columnName, FieldType.nullable(new ArrowType.FloatingPoint(SINGLE)), null));
          break;
        case Types.DOUBLE:
          fields.add(new Field(columnName, FieldType.nullable(new ArrowType.FloatingPoint(DOUBLE)), null));
          break;
        case Types.CHAR:
        case Types.NCHAR:
        case Types.VARCHAR:
        case Types.NVARCHAR:
        case Types.LONGVARCHAR:
        case Types.LONGNVARCHAR:
          fields.add(new Field(columnName, FieldType.nullable(new ArrowType.Utf8()), null));
          break;
        case Types.DATE:
          fields.add(new Field(columnName, FieldType.nullable(new ArrowType.Date(DateUnit.MILLISECOND)), null));
          break;
        case Types.TIME:
          fields.add(new Field(columnName, FieldType.nullable(new ArrowType.Time(TimeUnit.MILLISECOND, 32)), null));
          break;
        case Types.TIMESTAMP:
          fields.add(new Field(columnName, FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MILLISECOND,
              calendar.getTimeZone().getID())), null));
          break;
        case Types.BINARY:
        case Types.VARBINARY:
        case Types.LONGVARBINARY:
          fields.add(new Field(columnName, FieldType.nullable(new ArrowType.Binary()), null));
          break;
        case Types.ARRAY:
          // TODO Need to handle this type
          // fields.add(new Field("list", FieldType.nullable(new ArrowType.List()), null));
          break;
        case Types.CLOB:
          fields.add(new Field(columnName, FieldType.nullable(new ArrowType.Utf8()), null));
          break;
        case Types.BLOB:
          fields.add(new Field(columnName, FieldType.nullable(new ArrowType.Binary()), null));
          break;

        default:
          // no-op, shouldn't get here
          break;
      }
    }

    return new Schema(fields, null);
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
   * @param rs   ResultSet to use to fetch the data from underlying database
   * @param root Arrow {@link VectorSchemaRoot} object to populate
   * @throws SQLException on error
   */
  public static void jdbcToArrowVectors(ResultSet rs, VectorSchemaRoot root, Calendar calendar)
      throws SQLException, IOException {

    Preconditions.checkNotNull(rs, "JDBC ResultSet object can't be null");
    Preconditions.checkNotNull(root, "JDBC ResultSet object can't be null");
    Preconditions.checkNotNull(calendar, "Calendar object can't be null");

    ResultSetMetaData rsmd = rs.getMetaData();
    int columnCount = rsmd.getColumnCount();

    allocateVectors(root, DEFAULT_BUFFER_SIZE);

    int rowCount = 0;
    while (rs.next()) {
      for (int i = 1; i <= columnCount; i++) {
        String columnName = rsmd.getColumnName(i);
        switch (rsmd.getColumnType(i)) {
          case Types.BOOLEAN:
          case Types.BIT:
            updateVector((BitVector) root.getVector(columnName),
                    rs.getBoolean(i), !rs.wasNull(), rowCount);
            break;
          case Types.TINYINT:
            updateVector((TinyIntVector) root.getVector(columnName),
                    rs.getInt(i), !rs.wasNull(), rowCount);
            break;
          case Types.SMALLINT:
            updateVector((SmallIntVector) root.getVector(columnName),
                    rs.getInt(i), !rs.wasNull(), rowCount);
            break;
          case Types.INTEGER:
            updateVector((IntVector) root.getVector(columnName),
                    rs.getInt(i), !rs.wasNull(), rowCount);
            break;
          case Types.BIGINT:
            updateVector((BigIntVector) root.getVector(columnName),
                    rs.getLong(i), !rs.wasNull(), rowCount);
            break;
          case Types.NUMERIC:
          case Types.DECIMAL:
            updateVector((DecimalVector) root.getVector(columnName),
                    rs.getBigDecimal(i), !rs.wasNull(), rowCount);
            break;
          case Types.REAL:
          case Types.FLOAT:
            updateVector((Float4Vector) root.getVector(columnName),
                    rs.getFloat(i), !rs.wasNull(), rowCount);
            break;
          case Types.DOUBLE:
            updateVector((Float8Vector) root.getVector(columnName),
                    rs.getDouble(i), !rs.wasNull(), rowCount);
            break;
          case Types.CHAR:
          case Types.NCHAR:
          case Types.VARCHAR:
          case Types.NVARCHAR:
          case Types.LONGVARCHAR:
          case Types.LONGNVARCHAR:
            updateVector((VarCharVector) root.getVector(columnName),
                    rs.getString(i), !rs.wasNull(), rowCount);
            break;
          case Types.DATE:
            updateVector((DateMilliVector) root.getVector(columnName),
                    rs.getDate(i, calendar), !rs.wasNull(), rowCount);
            break;
          case Types.TIME:
            updateVector((TimeMilliVector) root.getVector(columnName),
                    rs.getTime(i, calendar), !rs.wasNull(), rowCount);
            break;
          case Types.TIMESTAMP:
            // TODO: Need to handle precision such as milli, micro, nano
            updateVector((TimeStampVector) root.getVector(columnName),
                    rs.getTimestamp(i, calendar), !rs.wasNull(), rowCount);
            break;
          case Types.BINARY:
          case Types.VARBINARY:
          case Types.LONGVARBINARY:
            updateVector((VarBinaryVector) root.getVector(columnName),
                    rs.getBinaryStream(i), !rs.wasNull(), rowCount);
            break;
          case Types.ARRAY:
            // TODO Need to handle this type
            // fields.add(new Field("list", FieldType.nullable(new ArrowType.List()), null));
            break;
          case Types.CLOB:
            updateVector((VarCharVector) root.getVector(columnName),
                    rs.getClob(i), !rs.wasNull(), rowCount);
            break;
          case Types.BLOB:
            updateVector((VarBinaryVector) root.getVector(columnName),
                    rs.getBlob(i), !rs.wasNull(), rowCount);
            break;

          default:
            // no-op, shouldn't get here
            break;
        }
      }
      rowCount++;
    }
    root.setRowCount(rowCount);
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

}
