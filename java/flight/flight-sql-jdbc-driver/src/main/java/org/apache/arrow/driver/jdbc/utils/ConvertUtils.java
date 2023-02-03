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

package org.apache.arrow.driver.jdbc.utils;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.JDBCType;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.arrow.flight.sql.FlightSqlColumnMetadata;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.calcite.avatica.AvaticaParameter;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.proto.Common;
import org.apache.calcite.avatica.proto.Common.ColumnMetaData.Builder;

/**
 * Convert Fields To Column MetaData List functions.
 */
public final class ConvertUtils {

  private ConvertUtils() {
  }

  static boolean isSigned(ArrowType.ArrowTypeID type) {
    switch (type) {
      case Int:
      case FloatingPoint:
      case Decimal:
      case Interval:
      case Duration:
        return true;
    }
    return false;
  }

  static int toJdbcType(ArrowType type) {
    switch (type.getTypeID()) {
      case Null:
        return JDBCType.NULL.getVendorTypeNumber();
      case Struct:
        return JDBCType.STRUCT.getVendorTypeNumber();
      case List:
      case LargeList:
      case FixedSizeList:
        return JDBCType.ARRAY.getVendorTypeNumber();
      case Int:
        ArrowType.Int t = (ArrowType.Int)type;
        switch (t.getBitWidth()) {
          case 64:
            return JDBCType.BIGINT.getVendorTypeNumber();
          default:
            return JDBCType.INTEGER.getVendorTypeNumber();
        }
      case FloatingPoint:
        return JDBCType.FLOAT.getVendorTypeNumber();
      case Utf8:
      case LargeUtf8:
        return JDBCType.VARCHAR.getVendorTypeNumber();
      case Binary:
      case LargeBinary:
        return JDBCType.VARBINARY.getVendorTypeNumber();
      case FixedSizeBinary:
        return JDBCType.BINARY.getVendorTypeNumber();
      case Bool:
        return JDBCType.BOOLEAN.getVendorTypeNumber();
      case Decimal:
        return JDBCType.DECIMAL.getVendorTypeNumber();
      case Date:
        return JDBCType.DATE.getVendorTypeNumber();
      case Time:
        return JDBCType.TIME.getVendorTypeNumber();
      case Timestamp:
      case Interval:
      case Duration:
        return JDBCType.TIMESTAMP.getVendorTypeNumber();
    }
    return JDBCType.OTHER.getVendorTypeNumber();
  }

  static String getClassName(ArrowType type) {
    switch (type.getTypeID()) {
      case List:
      case LargeList:
      case FixedSizeList:
        return ArrayList.class.getCanonicalName();
      case Map:
        return HashMap.class.getCanonicalName();
      case Int:
        ArrowType.Int t = (ArrowType.Int)type;
        switch (t.getBitWidth()) {
          case 64:
            return long.class.getCanonicalName();
          default:
            return int.class.getCanonicalName();
        }
      case FloatingPoint:
        return float.class.getCanonicalName();
      case Utf8:
      case LargeUtf8:
        return String.class.getCanonicalName();
      case Binary:
      case LargeBinary:
      case FixedSizeBinary:
        return byte[].class.getCanonicalName();
      case Bool:
        return boolean.class.getCanonicalName();
      case Decimal:
        return BigDecimal.class.getCanonicalName();
      case Date:
        return Date.class.getCanonicalName();
      case Time:
        return Time.class.getCanonicalName();
      case Timestamp:
      case Interval:
      case Duration:
        return Timestamp.class.getCanonicalName();
    }
    return null;
  }

  /**
   * Convert Fields To Avatica Parameters.
   *
   * @param fields list of {@link Field}.
   * @return list of {@link AvaticaParameter}.
   */
  public static List<AvaticaParameter> convertArrowFieldsToAvaticaParameters(final List<Field> fields) {
    List<AvaticaParameter> list = new ArrayList<>();
    for(Field field : fields) {
      final boolean signed = isSigned(field.getType().getTypeID());
      final int precision = 0; // Would have to know about the actual number
      final int scale = 0; // According to https://www.postgresql.org/docs/current/datatype-numeric.html
      final int type = toJdbcType(field.getType());
      final String typeName = field.getType().toString();
      final String clazz = getClassName(field.getType());
      final String name = field.getName();
      final AvaticaParameter param = new AvaticaParameter(signed, precision, scale, type, typeName, clazz, name);
      list.add(param);
    }
    return list;
  }

  /**
   * Convert Fields To Column MetaData List functions.
   *
   * @param fields list of {@link Field}.
   * @return list of {@link ColumnMetaData}.
   */
  public static List<ColumnMetaData> convertArrowFieldsToColumnMetaDataList(final List<Field> fields) {
    return Stream.iterate(0, Math::incrementExact).limit(fields.size())
        .map(index -> {
          final Field field = fields.get(index);
          final ArrowType fieldType = field.getType();

          final Builder builder = Common.ColumnMetaData.newBuilder()
              .setOrdinal(index)
              .setColumnName(field.getName())
              .setLabel(field.getName());

          setOnColumnMetaDataBuilder(builder, field.getMetadata());

          builder.setType(Common.AvaticaType.newBuilder()
              .setId(SqlTypes.getSqlTypeIdFromArrowType(fieldType))
              .setName(SqlTypes.getSqlTypeNameFromArrowType(fieldType))
              .build());

          return ColumnMetaData.fromProto(builder.build());
        }).collect(Collectors.toList());
  }

  /**
   * Set on Column MetaData Builder.
   *
   * @param builder     {@link Builder}
   * @param metadataMap {@link Map}
   */
  public static void setOnColumnMetaDataBuilder(final Builder builder,
                                                final Map<String, String> metadataMap) {
    final FlightSqlColumnMetadata columnMetadata = new FlightSqlColumnMetadata(metadataMap);
    final String catalogName = columnMetadata.getCatalogName();
    if (catalogName != null) {
      builder.setCatalogName(catalogName);
    }
    final String schemaName = columnMetadata.getSchemaName();
    if (schemaName != null) {
      builder.setSchemaName(schemaName);
    }
    final String tableName = columnMetadata.getTableName();
    if (tableName != null) {
      builder.setTableName(tableName);
    }

    final Integer precision = columnMetadata.getPrecision();
    if (precision != null) {
      builder.setPrecision(precision);
    }
    final Integer scale = columnMetadata.getScale();
    if (scale != null) {
      builder.setScale(scale);
    }

    final Boolean isAutoIncrement = columnMetadata.isAutoIncrement();
    if (isAutoIncrement != null) {
      builder.setAutoIncrement(isAutoIncrement);
    }
    final Boolean caseSensitive = columnMetadata.isCaseSensitive();
    if (caseSensitive != null) {
      builder.setCaseSensitive(caseSensitive);
    }
    final Boolean readOnly = columnMetadata.isReadOnly();
    if (readOnly != null) {
      builder.setReadOnly(readOnly);
    }
    final Boolean searchable = columnMetadata.isSearchable();
    if (searchable != null) {
      builder.setSearchable(searchable);
    }
  }
}
