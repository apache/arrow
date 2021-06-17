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
import java.util.List;
import java.util.TimeZone;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.calcite.avatica.AvaticaResultSet;
import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.Meta.Frame;
import org.apache.calcite.avatica.Meta.Signature;
import org.apache.calcite.avatica.QueryState;

/**
 * The {@link ResultSet} implementation for Arrow Flight.
 */
public class ArrowFlightResultSet extends AvaticaResultSet {

  ArrowFlightResultSet(final AvaticaStatement statement, final QueryState state,
      final Signature signature,
      final ResultSetMetaData resultSetMetaData,
      final TimeZone timeZone, final Frame firstFrame) throws SQLException {
    super(statement, state, signature, resultSetMetaData, timeZone, firstFrame);
  }

  protected VectorSchemaRoot getRawData() {
    return rawData;
  }

  @Override
  protected AvaticaResultSet execute() throws SQLException {

    try {

      VectorSchemaRoot root = (((ArrowFlightConnection) statement
              .getConnection())
              .getClient()
              .runQuery(signature.sql));

      final List<Field> fields = root.getSchema().getFields();

      List<ColumnMetaData> metadata =
              Stream.iterate(0, Math::incrementExact).limit(fields.size())
                .map(index -> {
                  Field field = fields.get(index);
                  ArrowType.ArrowTypeID fieldTypeId = field.getType().getTypeID();

                  return new ColumnMetaData(
                          index,
                          false,
                          false,
                          false,
                          false,
                          0,
                          false,
                          1,
                          field.getName(),
                          field.getName(),
                          field.getName(),
                          0,
                          0,
                          "TABLE-HERE",
                          "CATALOG-HERE",
                          new ColumnMetaData.AvaticaType(
                                  1 /* String-only for now */,
                                  fieldTypeId.name(),
                                  ColumnMetaData.Rep.STRING),
                          false,
                          false,
                          false,
                          "teste"
                  );
                }).collect(Collectors.toList());

      signature.columns.addAll(metadata);

      execute2(new ArrowFlightJdbcCursor(root),
              this.signature.columns);
    } catch (Exception e) {
      throw new SQLException(e);
    }

    return this;
  }
}
