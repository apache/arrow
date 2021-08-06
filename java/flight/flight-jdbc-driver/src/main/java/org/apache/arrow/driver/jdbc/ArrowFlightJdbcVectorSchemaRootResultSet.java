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

import static java.util.Objects.isNull;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TimeZone;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.arrow.driver.jdbc.utils.SqlTypes;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.VectorSchemaRoot;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link ResultSet} implementation used to access a {@link VectorSchemaRoot}.
 */
public class ArrowFlightJdbcVectorSchemaRootResultSet extends AvaticaResultSet {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(ArrowFlightJdbcVectorSchemaRootResultSet.class);
  VectorSchemaRoot vectorSchemaRoot;

  ArrowFlightJdbcVectorSchemaRootResultSet(final AvaticaStatement statement, final QueryState state,
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
  public static ArrowFlightJdbcVectorSchemaRootResultSet fromVectorSchemaRoot(VectorSchemaRoot vectorSchemaRoot)
      throws SQLException {
    // Similar to how org.apache.calcite.avatica.util.ArrayFactoryImpl does

    final String sql = "MOCKED";
    TimeZone timeZone = TimeZone.getDefault();
    QueryState state = new QueryState(sql);

    Meta.Signature signature = ArrowFlightMetaImpl.newSignature(sql);

    AvaticaResultSetMetaData resultSetMetaData = new AvaticaResultSetMetaData(null, sql, signature);
    ArrowFlightJdbcVectorSchemaRootResultSet
        resultSet = new ArrowFlightJdbcVectorSchemaRootResultSet(null, state, signature, resultSetMetaData,
        timeZone, null);

    resultSet.execute(vectorSchemaRoot);
    return resultSet;
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
              .setId(SqlTypes.getSqlTypeIdFromArrowType(field.getType()))
              .setName(fieldTypeId.name())
              .build());

          return ColumnMetaData.fromProto(builder.build());
        }).collect(Collectors.toList());
  }

  @Override
  protected AvaticaResultSet execute() throws SQLException {
    throw new RuntimeException();
  }

  void execute(VectorSchemaRoot vectorSchemaRoot) {
    final List<Field> fields = vectorSchemaRoot.getSchema().getFields();
    List<ColumnMetaData> columns = convertArrowFieldsToColumnMetaDataList(fields);
    signature.columns.clear();
    signature.columns.addAll(columns);

    this.vectorSchemaRoot = vectorSchemaRoot;
    execute2(new ArrowFlightJdbcCursor(vectorSchemaRoot), this.signature.columns);
  }

  @Override
  protected final void cancel() {
    try {
      AutoCloseables.close(this);
    } catch (final Exception e) {
      LOGGER.error(e.getMessage(), e);
      throw new RuntimeException(e);
    } finally {
      super.cancel();
    }
  }

  @Override
  public void close() {
    final Set<Exception> exceptions = new HashSet<>();
    try {
      if (isClosed()) {
        return;
      }
    } catch (final SQLException e) {
      exceptions.add(e);
    }
    try {
      AutoCloseables.close(vectorSchemaRoot);
    } catch (final Exception e) {
      exceptions.add(e);
    }
    if (!isNull(statement)) {
      try {
        super.close();
      } catch (final Exception e) {
        exceptions.add(e);
      }
    }
    exceptions.parallelStream().forEach(e -> LOGGER.error(e.getMessage(), e));
    exceptions.stream().findAny().ifPresent(e -> {
      throw new RuntimeException(e);
    });
  }

}
