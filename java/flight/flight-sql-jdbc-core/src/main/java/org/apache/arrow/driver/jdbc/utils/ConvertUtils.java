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

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.arrow.driver.jdbc.converter.impl.BinaryAvaticaParameterConverter;
import org.apache.arrow.driver.jdbc.converter.impl.BoolAvaticaParameterConverter;
import org.apache.arrow.driver.jdbc.converter.impl.DateAvaticaParameterConverter;
import org.apache.arrow.driver.jdbc.converter.impl.DecimalAvaticaParameterConverter;
import org.apache.arrow.driver.jdbc.converter.impl.DurationAvaticaParameterConverter;
import org.apache.arrow.driver.jdbc.converter.impl.FixedSizeBinaryAvaticaParameterConverter;
import org.apache.arrow.driver.jdbc.converter.impl.FloatingPointAvaticaParameterConverter;
import org.apache.arrow.driver.jdbc.converter.impl.IntAvaticaParameterConverter;
import org.apache.arrow.driver.jdbc.converter.impl.IntervalAvaticaParameterConverter;
import org.apache.arrow.driver.jdbc.converter.impl.LargeBinaryAvaticaParameterConverter;
import org.apache.arrow.driver.jdbc.converter.impl.LargeUtf8AvaticaParameterConverter;
import org.apache.arrow.driver.jdbc.converter.impl.NullAvaticaParameterConverter;
import org.apache.arrow.driver.jdbc.converter.impl.TimeAvaticaParameterConverter;
import org.apache.arrow.driver.jdbc.converter.impl.TimestampAvaticaParameterConverter;
import org.apache.arrow.driver.jdbc.converter.impl.Utf8AvaticaParameterConverter;
import org.apache.arrow.flight.sql.FlightSqlColumnMetadata;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.calcite.avatica.AvaticaParameter;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.proto.Common;
import org.apache.calcite.avatica.proto.Common.ColumnMetaData.Builder;

/**
 * Convert objects between Arrow and Avatica.
 */
public final class ConvertUtils {

  private ConvertUtils() {
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

  /**
   * Convert Fields To Avatica Parameters.
   *
   * @param fields list of {@link Field}.
   * @return list of {@link AvaticaParameter}.
   */
  public static List<AvaticaParameter> convertArrowFieldsToAvaticaParameters(final List<Field> fields) {
    return fields.stream()
            .map(field -> field.getType().accept(new ConverterVisitor(field)))
            .collect(Collectors.toList());
  }

  private static class ConverterVisitor implements ArrowType.ArrowTypeVisitor<AvaticaParameter> {
    private final Field field;

    private ConverterVisitor(Field field) {
      this.field = field;
    }

    @Override
    public AvaticaParameter visit(ArrowType.Null type) {
      return new NullAvaticaParameterConverter(type).createParameter(field);
    }

    @Override
    public AvaticaParameter visit(ArrowType.Struct type) {
      throw new UnsupportedOperationException();
    }

    @Override
    public AvaticaParameter visit(ArrowType.List type) {
      throw new UnsupportedOperationException();
    }

    @Override
    public AvaticaParameter visit(ArrowType.LargeList type) {
      throw new UnsupportedOperationException();
    }

    @Override
    public AvaticaParameter visit(ArrowType.FixedSizeList type) {
      throw new UnsupportedOperationException();
    }

    @Override
    public AvaticaParameter visit(ArrowType.Union type) {
      throw new UnsupportedOperationException();
    }

    @Override
    public AvaticaParameter visit(ArrowType.Map type) {
      throw new UnsupportedOperationException();
    }

    @Override
    public AvaticaParameter visit(ArrowType.Int type) {
      return new IntAvaticaParameterConverter(type).createParameter(field);
    }

    @Override
    public AvaticaParameter visit(ArrowType.FloatingPoint type) {
      return new FloatingPointAvaticaParameterConverter(type).createParameter(field);
    }

    @Override
    public AvaticaParameter visit(ArrowType.Utf8 type) {
      return new Utf8AvaticaParameterConverter(type).createParameter(field);
    }

    @Override
    public AvaticaParameter visit(ArrowType.LargeUtf8 type) {
      return new LargeUtf8AvaticaParameterConverter(type).createParameter(field);
    }

    @Override
    public AvaticaParameter visit(ArrowType.Binary type) {
      return new BinaryAvaticaParameterConverter(type).createParameter(field);
    }

    @Override
    public AvaticaParameter visit(ArrowType.LargeBinary type) {
      return new LargeBinaryAvaticaParameterConverter(type).createParameter(field);
    }

    @Override
    public AvaticaParameter visit(ArrowType.FixedSizeBinary type) {
      return new FixedSizeBinaryAvaticaParameterConverter(type).createParameter(field);
    }

    @Override
    public AvaticaParameter visit(ArrowType.Bool type) {
      return new BoolAvaticaParameterConverter(type).createParameter(field);
    }

    @Override
    public AvaticaParameter visit(ArrowType.Decimal type) {
      return new DecimalAvaticaParameterConverter(type).createParameter(field);
    }

    @Override
    public AvaticaParameter visit(ArrowType.Date type) {
      return new DateAvaticaParameterConverter(type).createParameter(field);
    }

    @Override
    public AvaticaParameter visit(ArrowType.Time type) {
      return new TimeAvaticaParameterConverter(type).createParameter(field);
    }

    @Override
    public AvaticaParameter visit(ArrowType.Timestamp type) {
      return new TimestampAvaticaParameterConverter(type).createParameter(field);
    }

    @Override
    public AvaticaParameter visit(ArrowType.Interval type) {
      return new IntervalAvaticaParameterConverter(type).createParameter(field);
    }

    @Override
    public AvaticaParameter visit(ArrowType.Duration type) {
      return new DurationAvaticaParameterConverter(type).createParameter(field);
    }
  }

}
