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

package org.apache.arrow.driver.jdbc;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import java.util.TimeZone;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.calcite.avatica.AvaticaResultSet;
import org.apache.calcite.avatica.AvaticaResultSetMetaData;
import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.Meta.Frame;
import org.apache.calcite.avatica.Meta.Signature;
import org.apache.calcite.avatica.QueryState;
import org.apache.calcite.avatica.proto.Common;

/**
 * The {@link ResultSet} implementation for Arrow Flight.
 */
public class ArrowFlightResultSet extends AvaticaResultSet {

  VectorSchemaRoot vectorSchemaRoot;

  ArrowFlightResultSet(final AvaticaStatement statement, final QueryState state,
                       final Signature signature,
                       final ResultSetMetaData resultSetMetaData,
                       final TimeZone timeZone, final Frame firstFrame) throws SQLException {
    super(statement, state, signature, resultSetMetaData, timeZone, firstFrame);
  }

  /**
   * Instantiate a ResultSet backed up by given VectorSchemaRoot.
   *
   * @param vectorSchemaRoot root from which the ResultSet will access.
   * @return a ResultSet which accesses the given VectorSchemaRoot
   */
  public static ArrowFlightResultSet fromVectorSchemaRoot(VectorSchemaRoot vectorSchemaRoot) throws SQLException {
    // Similar to how org.apache.calcite.avatica.util.ArrayFactoryImpl does

    final String sql = "MOCKED";
    TimeZone timeZone = TimeZone.getDefault();
    QueryState state = new QueryState(sql);

    Meta.Signature signature = ArrowFlightMetaImpl.newSignature(sql);

    AvaticaResultSetMetaData resultSetMetaData = new AvaticaResultSetMetaData(null, sql, signature);
    ArrowFlightResultSet resultSet = new ArrowFlightResultSet(null, state, signature, resultSetMetaData,
        timeZone, null);

    resultSet.execute(vectorSchemaRoot);
    return resultSet;
  }

  @Override
  protected AvaticaResultSet execute() throws SQLException {
    try {
      VectorSchemaRoot vectorSchemaRoot = (((ArrowFlightConnection) statement
          .getConnection())
          .getClient()
          .runQuery(signature.sql));

      execute(vectorSchemaRoot);
    } catch (SQLException e) {
      throw e;
    } catch (Exception e) {
      throw new SQLException(e);
    }

    return this;
  }

  private void execute(VectorSchemaRoot vectorSchemaRoot) {
    final List<Field> fields = vectorSchemaRoot.getSchema().getFields();
    List<ColumnMetaData> columns = convertArrowFieldsToColumnMetaDataList(fields);
    signature.columns.addAll(columns);

    this.vectorSchemaRoot = vectorSchemaRoot;
    execute2(new ArrowFlightJdbcCursor(vectorSchemaRoot), this.signature.columns);
  }

  @Override
  public void close() {
    if (this.statement != null) {
      // An ArrowFlightResultSet will have a null statement when it is created by
      // ArrowFlightResultSet#fromVectorSchemaRoot. In this case it must skip calling AvaticaResultSet#close,
      // as it expects that statement is not null
      super.close();
    }

    this.vectorSchemaRoot.close();
  }

  private static List<ColumnMetaData> convertArrowFieldsToColumnMetaDataList(List<Field> fields) {
    return Stream.iterate(0, Math::incrementExact).limit(fields.size())
        .map(index -> {
          Field field = fields.get(index);
          ArrowType.ArrowTypeID fieldTypeId = field.getType().getTypeID();

          Common.ColumnMetaData.Builder builder = Common.ColumnMetaData.newBuilder();
          builder.setOrdinal(index);
          builder.setColumnName(field.getName());

          builder.setType(Common.AvaticaType.newBuilder()
              .setId(getSqlTypeIdFromArrowType(field.getType()))
              .setName(fieldTypeId.name())
              .build());

          return ColumnMetaData.fromProto(builder.build());
        }).collect(Collectors.toList());
  }

  /**
   * Convert given {@link ArrowType} to its corresponding SQL type.
   *
   * @param arrowType type to convert from
   * @return corresponding SQL type.
   * @see java.sql.Types
   */
  public static int getSqlTypeIdFromArrowType(ArrowType arrowType) {
    final ArrowType.ArrowTypeID typeID = arrowType.getTypeID();
    switch (typeID) {
      case Int:
        final int bitWidth = ((ArrowType.Int) arrowType).getBitWidth();
        switch (bitWidth) {
          case 8:
            return Types.TINYINT;
          case 16:
            return Types.SMALLINT;
          case 32:
            return Types.INTEGER;
          case 64:
            return Types.BIGINT;
          default:
            break;
        }
        break;
      case Binary:
        return Types.VARBINARY;
      case FixedSizeBinary:
        return Types.BINARY;
      case LargeBinary:
        return Types.LONGVARBINARY;
      case Utf8:
        return Types.VARCHAR;
      case LargeUtf8:
        return Types.LONGVARCHAR;
      case Date:
        return Types.DATE;
      case Time:
        return Types.TIME;
      case Timestamp:
        return Types.TIMESTAMP;
      case Bool:
        return Types.BOOLEAN;
      case Decimal:
        return Types.DECIMAL;
      case FloatingPoint:
        final FloatingPointPrecision floatingPointPrecision = ((ArrowType.FloatingPoint) arrowType).getPrecision();
        switch (floatingPointPrecision) {
          case DOUBLE:
            return Types.DOUBLE;
          case SINGLE:
            return Types.FLOAT;
          default:
            break;
        }
        break;
      case List:
      case FixedSizeList:
      case LargeList:
        return Types.ARRAY;
      case Struct:
        return Types.STRUCT;
      case Duration:
      case Interval:
      case Map:
      case Union:
        return Types.JAVA_OBJECT;
      case NONE:
      case Null:
        return Types.NULL;
      default:
        break;
    }

    throw new IllegalArgumentException("Unsupported ArrowType " + arrowType);
  }

}
