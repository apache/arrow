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
import java.math.RoundingMode;
import java.sql.Date;
import java.sql.ParameterMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
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
import org.apache.arrow.adapter.jdbc.consumer.CompositeJdbcConsumer;
import org.apache.arrow.adapter.jdbc.consumer.DateConsumer;
import org.apache.arrow.adapter.jdbc.consumer.Decimal256Consumer;
import org.apache.arrow.adapter.jdbc.consumer.DecimalConsumer;
import org.apache.arrow.adapter.jdbc.consumer.DoubleConsumer;
import org.apache.arrow.adapter.jdbc.consumer.FloatConsumer;
import org.apache.arrow.adapter.jdbc.consumer.IntConsumer;
import org.apache.arrow.adapter.jdbc.consumer.JdbcConsumer;
import org.apache.arrow.adapter.jdbc.consumer.MapConsumer;
import org.apache.arrow.adapter.jdbc.consumer.NullConsumer;
import org.apache.arrow.adapter.jdbc.consumer.SmallIntConsumer;
import org.apache.arrow.adapter.jdbc.consumer.TimeConsumer;
import org.apache.arrow.adapter.jdbc.consumer.TimestampConsumer;
import org.apache.arrow.adapter.jdbc.consumer.TimestampTZConsumer;
import org.apache.arrow.adapter.jdbc.consumer.TinyIntConsumer;
import org.apache.arrow.adapter.jdbc.consumer.VarCharConsumer;
import org.apache.arrow.adapter.jdbc.consumer.exceptions.JdbcConsumerException;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.Decimal256Vector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.NullVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeStampMilliTZVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.ValueVectorUtility;

/**
 * Class that does most of the work to convert JDBC ResultSet data into Arrow columnar format Vector
 * objects.
 *
 * @since 0.10.0
 */
public class JdbcToArrowUtils {

  private static final int JDBC_ARRAY_VALUE_COLUMN = 2;

  /** Returns the instance of a {java.util.Calendar} with the UTC time zone and root locale. */
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
  public static Schema jdbcToArrowSchema(ResultSetMetaData rsmd, Calendar calendar)
      throws SQLException {
    Preconditions.checkNotNull(calendar, "Calendar object can't be null");

    return jdbcToArrowSchema(rsmd, new JdbcToArrowConfig(new RootAllocator(0), calendar));
  }

  /**
   * Create Arrow {@link Schema} object for the given JDBC {@link ResultSetMetaData}.
   *
   * @param parameterMetaData The ResultSetMetaData containing the results, to read the JDBC
   *     metadata from.
   * @param calendar The calendar to use the time zone field of, to construct Timestamp fields from.
   * @return {@link Schema}
   * @throws SQLException on error
   */
  public static Schema jdbcToArrowSchema(
      final ParameterMetaData parameterMetaData, final Calendar calendar) throws SQLException {
    Preconditions.checkNotNull(calendar, "Calendar object can't be null");
    Preconditions.checkNotNull(parameterMetaData);
    final List<Field> parameterFields = new ArrayList<>(parameterMetaData.getParameterCount());
    for (int parameterCounter = 1;
        parameterCounter <= parameterMetaData.getParameterCount();
        parameterCounter++) {
      final int jdbcDataType = parameterMetaData.getParameterType(parameterCounter);
      final int jdbcIsNullable = parameterMetaData.isNullable(parameterCounter);
      final boolean arrowIsNullable = jdbcIsNullable != ParameterMetaData.parameterNoNulls;
      final int precision = parameterMetaData.getPrecision(parameterCounter);
      final int scale = parameterMetaData.getScale(parameterCounter);
      final ArrowType arrowType =
          getArrowTypeFromJdbcType(new JdbcFieldInfo(jdbcDataType, precision, scale), calendar);
      final FieldType fieldType = new FieldType(arrowIsNullable, arrowType, /*dictionary=*/ null);
      parameterFields.add(new Field(null, fieldType, null));
    }

    return new Schema(parameterFields);
  }

  /**
   * Converts the provided JDBC type to its respective {@link ArrowType} counterpart.
   *
   * @param fieldInfo the {@link JdbcFieldInfo} with information about the original JDBC type.
   * @param calendar the {@link Calendar} to use for datetime data types.
   * @return a new {@link ArrowType}.
   */
  public static ArrowType getArrowTypeFromJdbcType(
      final JdbcFieldInfo fieldInfo, final Calendar calendar) {
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
        if (precision > 38) {
          return new ArrowType.Decimal(precision, scale, 256);
        } else {
          return new ArrowType.Decimal(precision, scale, 128);
        }
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
        return new ArrowType.Date(DateUnit.DAY);
      case Types.TIME:
        return new ArrowType.Time(TimeUnit.MILLISECOND, 32);
      case Types.TIMESTAMP:
        final String timezone;
        if (calendar != null) {
          timezone = calendar.getTimeZone().getID();
        } else {
          timezone = null;
        }
        return new ArrowType.Timestamp(TimeUnit.MILLISECOND, timezone);
      case Types.BINARY:
      case Types.VARBINARY:
      case Types.LONGVARBINARY:
      case Types.BLOB:
        return new ArrowType.Binary();
      case Types.ARRAY:
        return new ArrowType.List();
      case Types.NULL:
        return new ArrowType.Null();
      case Types.STRUCT:
        return new ArrowType.Struct();
      default:
        // no-op, shouldn't get here
        throw new UnsupportedOperationException("Unmapped JDBC type: " + fieldInfo.getJdbcType());
    }
  }

  /**
   * Create Arrow {@link Schema} object for the given JDBC {@link java.sql.ResultSetMetaData}.
   *
   * <p>If {@link JdbcToArrowConfig#shouldIncludeMetadata()} returns <code>true</code>, the
   * following fields will be added to the {@link FieldType#getMetadata()}:
   *
   * <ul>
   *   <li>{@link Constants#SQL_CATALOG_NAME_KEY} representing {@link
   *       ResultSetMetaData#getCatalogName(int)}
   *   <li>{@link Constants#SQL_TABLE_NAME_KEY} representing {@link
   *       ResultSetMetaData#getTableName(int)}
   *   <li>{@link Constants#SQL_COLUMN_NAME_KEY} representing {@link
   *       ResultSetMetaData#getColumnLabel(int)}
   *   <li>{@link Constants#SQL_TYPE_KEY} representing {@link
   *       ResultSetMetaData#getColumnTypeName(int)}
   * </ul>
   *
   * <p>If any columns are of type {@link java.sql.Types#ARRAY}, the configuration object will be
   * used to look up the array sub-type field. The {@link
   * JdbcToArrowConfig#getArraySubTypeByColumnIndex(int)} method will be checked first, followed by
   * the {@link JdbcToArrowConfig#getArraySubTypeByColumnName(String)} method.
   *
   * @param rsmd The ResultSetMetaData containing the results, to read the JDBC metadata from.
   * @param config The configuration to use when constructing the schema.
   * @return {@link Schema}
   * @throws SQLException on error
   * @throws IllegalArgumentException if <code>rsmd</code> contains an {@link java.sql.Types#ARRAY}
   *     but the <code>config</code> does not have a sub-type definition for it.
   */
  public static Schema jdbcToArrowSchema(ResultSetMetaData rsmd, JdbcToArrowConfig config)
      throws SQLException {
    Preconditions.checkNotNull(rsmd, "JDBC ResultSetMetaData object can't be null");
    Preconditions.checkNotNull(config, "The configuration object must not be null");

    List<Field> fields = new ArrayList<>();
    int columnCount = rsmd.getColumnCount();
    for (int i = 1; i <= columnCount; i++) {
      final String columnName = rsmd.getColumnLabel(i);

      final Map<String, String> columnMetadata =
          config.getColumnMetadataByColumnIndex() != null
              ? config.getColumnMetadataByColumnIndex().get(i)
              : null;
      final Map<String, String> metadata;
      if (config.shouldIncludeMetadata()) {
        metadata = new HashMap<>();
        metadata.put(Constants.SQL_CATALOG_NAME_KEY, rsmd.getCatalogName(i));
        metadata.put(Constants.SQL_SCHEMA_NAME_KEY, rsmd.getSchemaName(i));
        metadata.put(Constants.SQL_TABLE_NAME_KEY, rsmd.getTableName(i));
        metadata.put(Constants.SQL_COLUMN_NAME_KEY, columnName);
        metadata.put(Constants.SQL_TYPE_KEY, rsmd.getColumnTypeName(i));
        if (columnMetadata != null && !columnMetadata.isEmpty()) {
          metadata.putAll(columnMetadata);
        }
      } else {
        if (columnMetadata != null && !columnMetadata.isEmpty()) {
          metadata = columnMetadata;
        } else {
          metadata = null;
        }
      }

      final JdbcFieldInfo columnFieldInfo = getJdbcFieldInfoForColumn(rsmd, i, config);
      final ArrowType arrowType = config.getJdbcToArrowTypeConverter().apply(columnFieldInfo);
      if (arrowType != null) {
        final FieldType fieldType =
            new FieldType(
                isColumnNullable(rsmd, i, columnFieldInfo),
                arrowType, /* dictionary encoding */
                null,
                metadata);

        List<Field> children = null;
        if (arrowType.getTypeID() == ArrowType.List.TYPE_TYPE) {
          final JdbcFieldInfo arrayFieldInfo = getJdbcFieldInfoForArraySubType(rsmd, i, config);
          if (arrayFieldInfo == null) {
            throw new IllegalArgumentException(
                "Configuration does not provide a mapping for array column " + i);
          }
          children = new ArrayList<Field>();
          final ArrowType childType = config.getJdbcToArrowTypeConverter().apply(arrayFieldInfo);
          children.add(new Field("child", FieldType.nullable(childType), null));
        } else if (arrowType.getTypeID() == ArrowType.ArrowTypeID.Map) {
          FieldType mapType = new FieldType(false, ArrowType.Struct.INSTANCE, null, null);
          FieldType keyType = new FieldType(false, new ArrowType.Utf8(), null, null);
          FieldType valueType = new FieldType(false, new ArrowType.Utf8(), null, null);
          children = new ArrayList<>();
          children.add(
              new Field(
                  "child",
                  mapType,
                  Arrays.asList(
                      new Field(MapVector.KEY_NAME, keyType, null),
                      new Field(MapVector.VALUE_NAME, valueType, null))));
        }

        fields.add(new Field(columnName, fieldType, children));
      }
    }
    return new Schema(fields, config.getSchemaMetadata());
  }

  static JdbcFieldInfo getJdbcFieldInfoForColumn(
      ResultSetMetaData rsmd, int arrayColumn, JdbcToArrowConfig config) throws SQLException {
    Preconditions.checkNotNull(rsmd, "ResultSet MetaData object cannot be null");
    Preconditions.checkNotNull(config, "Configuration must not be null");
    Preconditions.checkArgument(
        arrayColumn > 0, "ResultSetMetaData columns start with 1; column cannot be less than 1");
    Preconditions.checkArgument(
        arrayColumn <= rsmd.getColumnCount(),
        "Column number cannot be more than the number of columns");

    JdbcFieldInfo fieldInfo = config.getExplicitTypeByColumnIndex(arrayColumn);
    if (fieldInfo == null) {
      fieldInfo = config.getExplicitTypeByColumnName(rsmd.getColumnLabel(arrayColumn));
    }
    if (fieldInfo != null) {
      return fieldInfo;
    }
    return new JdbcFieldInfo(rsmd, arrayColumn);
  }

  /* Uses the configuration to determine what the array sub-type JdbcFieldInfo is.
   * If no sub-type can be found, returns null.
   */
  private static JdbcFieldInfo getJdbcFieldInfoForArraySubType(
      ResultSetMetaData rsmd, int arrayColumn, JdbcToArrowConfig config) throws SQLException {

    Preconditions.checkNotNull(rsmd, "ResultSet MetaData object cannot be null");
    Preconditions.checkNotNull(config, "Configuration must not be null");
    Preconditions.checkArgument(
        arrayColumn > 0, "ResultSetMetaData columns start with 1; column cannot be less than 1");
    Preconditions.checkArgument(
        arrayColumn <= rsmd.getColumnCount(),
        "Column number cannot be more than the number of columns");

    JdbcFieldInfo fieldInfo = config.getArraySubTypeByColumnIndex(arrayColumn);
    if (fieldInfo == null) {
      fieldInfo = config.getArraySubTypeByColumnName(rsmd.getColumnLabel(arrayColumn));
    }
    return fieldInfo;
  }

  /**
   * Iterate the given JDBC {@link ResultSet} object to fetch the data and transpose it to populate
   * the given Arrow Vector objects.
   *
   * @param rs ResultSet to use to fetch the data from underlying database
   * @param root Arrow {@link VectorSchemaRoot} object to populate
   * @param calendar The calendar to use when reading {@link Date}, {@link Time}, or {@link
   *     Timestamp} data types from the {@link ResultSet}, or <code>null</code> if not converting.
   * @throws SQLException on error
   */
  public static void jdbcToArrowVectors(ResultSet rs, VectorSchemaRoot root, Calendar calendar)
      throws SQLException, IOException {

    Preconditions.checkNotNull(calendar, "Calendar object can't be null");

    jdbcToArrowVectors(rs, root, new JdbcToArrowConfig(new RootAllocator(0), calendar));
  }

  static boolean isColumnNullable(
      ResultSetMetaData resultSetMetadata, int index, JdbcFieldInfo info) throws SQLException {
    int nullableValue;
    if (info != null && info.isNullable() != ResultSetMetaData.columnNullableUnknown) {
      nullableValue = info.isNullable();
    } else {
      nullableValue = resultSetMetadata.isNullable(index);
    }
    return nullableValue == ResultSetMetaData.columnNullable
        || nullableValue == ResultSetMetaData.columnNullableUnknown;
  }

  /**
   * Iterate the given JDBC {@link ResultSet} object to fetch the data and transpose it to populate
   * the given Arrow Vector objects.
   *
   * @param rs ResultSet to use to fetch the data from underlying database
   * @param root Arrow {@link VectorSchemaRoot} object to populate
   * @param config The configuration to use when reading the data.
   * @throws SQLException on error
   * @throws JdbcConsumerException on error from VectorConsumer
   */
  public static void jdbcToArrowVectors(
      ResultSet rs, VectorSchemaRoot root, JdbcToArrowConfig config)
      throws SQLException, IOException {

    ResultSetMetaData rsmd = rs.getMetaData();
    int columnCount = rsmd.getColumnCount();

    JdbcConsumer[] consumers = new JdbcConsumer[columnCount];
    for (int i = 1; i <= columnCount; i++) {
      FieldVector vector = root.getVector(rsmd.getColumnLabel(i));
      final JdbcFieldInfo columnFieldInfo = getJdbcFieldInfoForColumn(rsmd, i, config);
      consumers[i - 1] =
          getConsumer(
              vector.getField().getType(),
              i,
              isColumnNullable(rsmd, i, columnFieldInfo),
              vector,
              config);
    }

    CompositeJdbcConsumer compositeConsumer = null;
    // Only clean resources when occurs error,
    // vectors within consumers are useful and users are responsible for its close.
    try {
      compositeConsumer = new CompositeJdbcConsumer(consumers);
      int readRowCount = 0;
      if (config.getTargetBatchSize() == JdbcToArrowConfig.NO_LIMIT_BATCH_SIZE) {
        while (rs.next()) {
          ValueVectorUtility.ensureCapacity(root, readRowCount + 1);
          compositeConsumer.consume(rs);
          readRowCount++;
        }
      } else {
        while (readRowCount < config.getTargetBatchSize() && rs.next()) {
          compositeConsumer.consume(rs);
          readRowCount++;
        }
      }

      root.setRowCount(readRowCount);
    } catch (Exception e) {
      // error occurs and clean up resources.
      if (compositeConsumer != null) {
        compositeConsumer.close();
      }
      throw e;
    }
  }

  /**
   * Default function used for JdbcConsumerFactory. This function gets a JdbcConsumer for the given
   * column based on the Arrow type and provided vector.
   *
   * @param arrowType Arrow type for the column.
   * @param columnIndex Column index to fetch from the ResultSet
   * @param nullable Whether the value is nullable or not
   * @param vector Vector to store the consumed value
   * @param config Associated JdbcToArrowConfig, used mainly for the Calendar.
   * @return {@link JdbcConsumer}
   */
  public static JdbcConsumer getConsumer(
      ArrowType arrowType,
      int columnIndex,
      boolean nullable,
      FieldVector vector,
      JdbcToArrowConfig config) {
    final Calendar calendar = config.getCalendar();

    switch (arrowType.getTypeID()) {
      case Bool:
        return BitConsumer.createConsumer((BitVector) vector, columnIndex, nullable);
      case Int:
        switch (((ArrowType.Int) arrowType).getBitWidth()) {
          case 8:
            return TinyIntConsumer.createConsumer((TinyIntVector) vector, columnIndex, nullable);
          case 16:
            return SmallIntConsumer.createConsumer((SmallIntVector) vector, columnIndex, nullable);
          case 32:
            return IntConsumer.createConsumer((IntVector) vector, columnIndex, nullable);
          case 64:
            return BigIntConsumer.createConsumer((BigIntVector) vector, columnIndex, nullable);
          default:
            return null;
        }
      case Decimal:
        final RoundingMode bigDecimalRoundingMode = config.getBigDecimalRoundingMode();
        if (((ArrowType.Decimal) arrowType).getBitWidth() == 256) {
          return Decimal256Consumer.createConsumer(
              (Decimal256Vector) vector, columnIndex, nullable, bigDecimalRoundingMode);
        } else {
          return DecimalConsumer.createConsumer(
              (DecimalVector) vector, columnIndex, nullable, bigDecimalRoundingMode);
        }
      case FloatingPoint:
        switch (((ArrowType.FloatingPoint) arrowType).getPrecision()) {
          case SINGLE:
            return FloatConsumer.createConsumer((Float4Vector) vector, columnIndex, nullable);
          case DOUBLE:
            return DoubleConsumer.createConsumer((Float8Vector) vector, columnIndex, nullable);
          default:
            return null;
        }
      case Utf8:
      case LargeUtf8:
        return VarCharConsumer.createConsumer((VarCharVector) vector, columnIndex, nullable);
      case Binary:
      case LargeBinary:
        return BinaryConsumer.createConsumer((VarBinaryVector) vector, columnIndex, nullable);
      case Date:
        return DateConsumer.createConsumer((DateDayVector) vector, columnIndex, nullable, calendar);
      case Time:
        return TimeConsumer.createConsumer(
            (TimeMilliVector) vector, columnIndex, nullable, calendar);
      case Timestamp:
        if (config.getCalendar() == null) {
          return TimestampConsumer.createConsumer(
              (TimeStampMilliVector) vector, columnIndex, nullable);
        } else {
          return TimestampTZConsumer.createConsumer(
              (TimeStampMilliTZVector) vector, columnIndex, nullable, calendar);
        }
      case List:
        FieldVector childVector = ((ListVector) vector).getDataVector();
        JdbcConsumer delegate =
            getConsumer(
                childVector.getField().getType(),
                JDBC_ARRAY_VALUE_COLUMN,
                childVector.getField().isNullable(),
                childVector,
                config);
        return ArrayConsumer.createConsumer((ListVector) vector, delegate, columnIndex, nullable);
      case Map:
        return MapConsumer.createConsumer((MapVector) vector, columnIndex, nullable);
      case Null:
        return new NullConsumer((NullVector) vector);
      default:
        // no-op, shouldn't get here
        throw new UnsupportedOperationException("No consumer for Arrow type: " + arrowType);
    }
  }
}
