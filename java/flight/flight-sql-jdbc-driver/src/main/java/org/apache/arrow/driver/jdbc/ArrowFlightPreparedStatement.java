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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.arrow.driver.jdbc.client.ArrowFlightSqlClientHandler;
import org.apache.arrow.driver.jdbc.utils.ConvertUtils;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.avatica.AvaticaPreparedStatement;
import org.apache.calcite.avatica.Meta.Signature;
import org.apache.calcite.avatica.Meta.StatementHandle;


/**
 * Arrow Flight JBCS's implementation {@link PreparedStatement}.
 */
public class ArrowFlightPreparedStatement extends AvaticaPreparedStatement
    implements ArrowFlightInfoStatement {

  private final ArrowFlightSqlClientHandler.PreparedStatement preparedStatement;

  private ArrowFlightPreparedStatement(final ArrowFlightConnection connection,
                                       final ArrowFlightSqlClientHandler.PreparedStatement preparedStatement,
                                       final StatementHandle handle,
                                       final Signature signature, final int resultSetType,
                                       final int resultSetConcurrency,
                                       final int resultSetHoldability)
      throws SQLException {
    super(connection, handle, signature, resultSetType, resultSetConcurrency, resultSetHoldability);
    this.preparedStatement = Preconditions.checkNotNull(preparedStatement);
  }

  /**
   * Creates a new {@link ArrowFlightPreparedStatement} from the provided information.
   *
   * @param connection           the {@link Connection} to use.
   * @param statementHandle      the {@link StatementHandle} to use.
   * @param signature            the {@link Signature} to use.
   * @param resultSetType        the ResultSet type.
   * @param resultSetConcurrency the ResultSet concurrency.
   * @param resultSetHoldability the ResultSet holdability.
   * @return a new {@link PreparedStatement}.
   * @throws SQLException on error.
   */
  static ArrowFlightPreparedStatement createNewPreparedStatement(
      final ArrowFlightConnection connection,
      final StatementHandle statementHandle,
      final Signature signature,
      final int resultSetType,
      final int resultSetConcurrency,
      final int resultSetHoldability) throws SQLException {

    final ArrowFlightSqlClientHandler.PreparedStatement prepare = connection.getClientHandler().prepare(signature.sql);
    final Schema resultSetSchema = prepare.getDataSetSchema();

    signature.columns.addAll(ConvertUtils.convertArrowFieldsToColumnMetaDataList(resultSetSchema.getFields()));

    return new ArrowFlightPreparedStatement(
        connection, prepare, statementHandle,
        signature, resultSetType, resultSetConcurrency, resultSetHoldability);
  }

  static ArrowFlightPreparedStatement newPreparedStatement(final ArrowFlightConnection connection,
      final ArrowFlightSqlClientHandler.PreparedStatement preparedStmt,
      final StatementHandle statementHandle,
      final Signature signature,
      final int resultSetType,
      final int resultSetConcurrency,
      final int resultSetHoldability) throws SQLException {
    return new ArrowFlightPreparedStatement(
        connection, preparedStmt, statementHandle,
        signature, resultSetType, resultSetConcurrency, resultSetHoldability);
  }

  @Override
  public ArrowFlightConnection getConnection() throws SQLException {
    return (ArrowFlightConnection) super.getConnection();
  }

  @Override
  public synchronized void close() throws SQLException {
    this.preparedStatement.close();
    super.close();
  }

  @Override
  public FlightInfo executeFlightInfoQuery() throws SQLException {
    return preparedStatement.executeQuery();
  }
}
