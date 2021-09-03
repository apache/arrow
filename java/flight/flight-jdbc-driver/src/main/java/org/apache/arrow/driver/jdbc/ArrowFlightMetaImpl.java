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
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.AvaticaParameter;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.MetaImpl;
import org.apache.calcite.avatica.MissingResultsException;
import org.apache.calcite.avatica.NoSuchStatementException;
import org.apache.calcite.avatica.QueryState;
import org.apache.calcite.avatica.remote.TypedValue;

/**
 * Metadata handler for Arrow Flight.
 */
public class ArrowFlightMetaImpl extends MetaImpl {

  private final AtomicInteger statementHandleId = new AtomicInteger();

  public ArrowFlightMetaImpl(final AvaticaConnection connection) {
    super(connection);
    setDefaultConnectionProperties();
  }

  static Signature newSignature(final String sql) {
    return new Signature(
        new ArrayList<ColumnMetaData>(),
        sql,
        Collections.<AvaticaParameter>emptyList(),
        Collections.<String, Object>emptyMap(),
        null, // unnecessary, as SQL requests use ArrowFlightJdbcCursor
        StatementType.SELECT
    );
  }

  @Override
  public void closeStatement(final StatementHandle statementHandle) {
  }

  @Override
  public void commit(final ConnectionHandle connectionHandle) {
    // TODO Fill this stub.
  }

  @Override
  public ExecuteResult execute(final StatementHandle statementHandle,
      final List<TypedValue> typedValues, final long maxRowCount)
          throws NoSuchStatementException {
    return null;
  }

  @Override
  public ExecuteResult execute(final StatementHandle statementHandle,
      final List<TypedValue> typedValues, final int maxRowsInFirstFrame)
          throws NoSuchStatementException {
    // Avatica removes the signature in case of updates
    if (statementHandle.signature == null) {
      // TODO: Handle updates
      throw new IllegalStateException();
    } else {
      return new ExecuteResult(Collections.singletonList(MetaResultSet.create(
          statementHandle.connectionId, statementHandle.id, true, statementHandle.signature, null)));
    }
  }

  @Override
  public ExecuteBatchResult executeBatch(final StatementHandle statementHandle,
      final List<List<TypedValue>> parameterValuesList)
          throws NoSuchStatementException {
    throw new IllegalStateException();
  }

  @Override
  public Frame fetch(final StatementHandle statementHandle, final long offset,
      final int fetchMaxRowCount)
          throws NoSuchStatementException, MissingResultsException {
    throw new IllegalStateException();
  }

  @Override
  public StatementHandle prepare(final ConnectionHandle connectionHandle,
      final String query, final long maxRowCount) {

    return new StatementHandle(
        connectionHandle.id, statementHandleId.incrementAndGet(), newSignature(query));
  }

  @Override
  public ExecuteResult prepareAndExecute(final StatementHandle statementHandle,
      final String query, final long maxRowCount,
      final PrepareCallback prepareCallback)
          throws NoSuchStatementException {
    // TODO Fill this stub.
    return null;
  }

  @Override
  public ExecuteResult prepareAndExecute(final StatementHandle handle,
      final String query, final long maxRowCount, final int maxRowsInFirstFrame,
      final PrepareCallback callback) throws NoSuchStatementException {
    final Signature signature = newSignature(query);
    try {
      synchronized (callback.getMonitor()) {
        callback.clear();
        callback.assign(signature, null, -1);
      }
      callback.execute();
      final MetaResultSet metaResultSet = MetaResultSet.create(handle.connectionId, handle.id,
          false, signature, null);
      return new ExecuteResult(Collections.singletonList(metaResultSet));
    } catch (SQLTimeoutException e) {
      // So far AvaticaStatement(executeInternal) only handles NoSuchStatement and Runtime Exceptions.
      throw new RuntimeException(e);
    } catch (SQLException e) {
      throw new NoSuchStatementException(handle);
    }
  }

  @Override
  public ExecuteBatchResult prepareAndExecuteBatch(
      final StatementHandle statementHandle, final List<String> queries)
          throws NoSuchStatementException {
    // TODO Fill this stub.
    return null;
  }

  @Override
  public void rollback(final ConnectionHandle connectionHandle) {
    // TODO Fill this stub.
  }

  @Override
  public boolean syncResults(final StatementHandle statementHandle,
      final QueryState queryState, final long offset)
          throws NoSuchStatementException {
    // TODO Fill this stub.
    return false;
  }

  void setDefaultConnectionProperties() {
    // TODO Double-check this.
    connProps.setDirty(false)
        .setAutoCommit(true)
        .setReadOnly(true)
        .setCatalog(null)
        .setSchema(null)
        .setTransactionIsolation(Connection.TRANSACTION_NONE);
  }
}
