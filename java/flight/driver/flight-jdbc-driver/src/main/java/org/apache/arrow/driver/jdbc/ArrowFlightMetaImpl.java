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
import java.util.List;

import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.MetaImpl;
import org.apache.calcite.avatica.MissingResultsException;
import org.apache.calcite.avatica.NoSuchStatementException;
import org.apache.calcite.avatica.QueryState;
import org.apache.calcite.avatica.remote.TypedValue;

/**
 * Metadata handler for Arrow Flight.
 */
public class ArrowFlightMetaImpl extends MetaImpl {

  private static final ArrowFlightJdbcDriver driver = new ArrowFlightJdbcDriver();

  public ArrowFlightMetaImpl(AvaticaConnection connection) {
    super(connection);
    setDefaultConnectionProperties();
  }

  @Override
  public void closeStatement(StatementHandle statementHandle) {
    // TODO Fill this stub.
  }

  @Override
  public void commit(ConnectionHandle connectionHandle) {
    // TODO Fill this stub.
  }

  @Override
  public ExecuteResult execute(StatementHandle statementHandle,
      List<TypedValue> typedValues, long maxRowCount)
      throws NoSuchStatementException {
    return null;
  }

  @Override
  public ExecuteResult execute(StatementHandle statementHandle,
      List<TypedValue> typedValues, int maxRowsInFirstFrame)
      throws NoSuchStatementException {
    return null;
  }

  @Override
  public ExecuteBatchResult executeBatch(StatementHandle statementHandle,
      List<List<TypedValue>> parameterValuesList)
      throws NoSuchStatementException {
    // TODO Fill this stub.
    return null;
  }

  @Override
  public Frame fetch(StatementHandle statementHandle, long offset,
      int fetchMaxRowCount)
      throws NoSuchStatementException, MissingResultsException {
    // TODO Fill this stub.
    return null;
  }

  @Override
  public StatementHandle prepare(ConnectionHandle connectionHandle,
      String query, long maxRowCount) {
    // TODO Fill this stub.
    return null;
  }

  @Override
  public ExecuteResult prepareAndExecute(StatementHandle statementHandle,
      String query, long maxRowCount, PrepareCallback prepareCallback)
      throws NoSuchStatementException {
    // TODO Fill this stub.
    return null;
  }

  @Override
  public ExecuteResult prepareAndExecute(StatementHandle statementHandle,
      String query, long maxRowCount, int maxRowsInFirstFrame,
      PrepareCallback prepareCallback) throws NoSuchStatementException {
    // TODO Fill this stub.
    return null;
  }

  @Override
  public ExecuteBatchResult prepareAndExecuteBatch(
      StatementHandle statementHandle, List<String> queries)
      throws NoSuchStatementException {
    // TODO Fill this stub.
    return null;
  }

  @Override
  public void rollback(ConnectionHandle connectionHandle) {
    // TODO Fill this stub.
  }

  @Override
  public boolean syncResults(StatementHandle statementHandle,
      QueryState queryState, long offset) throws NoSuchStatementException {
    // TODO Fill this stub.
    return false;
  }

  public ArrowFlightConnection getConnect() {
    return null;
  }

  private void setDefaultConnectionProperties() {
    connProps.setAutoCommit(false).setReadOnly(false)
        .setTransactionIsolation(Connection.TRANSACTION_NONE);
    connProps.setDirty(false);
  }
}
