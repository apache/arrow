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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.arrow.driver.jdbc.client.ArrowFlightSqlClientHandler.PreparedStatement;
import org.apache.arrow.driver.jdbc.utils.AvaticaParameterBinder;
import org.apache.arrow.driver.jdbc.utils.ConvertUtils;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.AvaticaParameter;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.MetaImpl;
import org.apache.calcite.avatica.NoSuchStatementException;
import org.apache.calcite.avatica.QueryState;
import org.apache.calcite.avatica.remote.TypedValue;

/**
 * Metadata handler for Arrow Flight.
 */
public class ArrowFlightMetaImpl extends MetaImpl {
  private final Map<StatementHandleKey, PreparedStatement> statementHandlePreparedStatementMap;

  /**
   * Constructs a {@link MetaImpl} object specific for Arrow Flight.
   * @param connection A {@link AvaticaConnection}.
   */
  public ArrowFlightMetaImpl(final AvaticaConnection connection) {
    super(connection);
    this.statementHandlePreparedStatementMap = new ConcurrentHashMap<>();
    setDefaultConnectionProperties();
  }

  /**
   * Construct a signature.
   */
  static Signature newSignature(final String sql, Schema resultSetSchema, Schema parameterSchema) {
    List<ColumnMetaData> columnMetaData = resultSetSchema == null ?
            new ArrayList<>() : ConvertUtils.convertArrowFieldsToColumnMetaDataList(resultSetSchema.getFields());
    List<AvaticaParameter> parameters = parameterSchema == null ?
            new ArrayList<>() : ConvertUtils.convertArrowFieldsToAvaticaParameters(parameterSchema.getFields());

    return new Signature(
        columnMetaData,
        sql,
        parameters,
        Collections.emptyMap(),
        null, // unnecessary, as SQL requests use ArrowFlightJdbcCursor
        StatementType.SELECT
    );
  }

  @Override
  public void closeStatement(final StatementHandle statementHandle) {
    PreparedStatement preparedStatement =
        statementHandlePreparedStatementMap.remove(new StatementHandleKey(statementHandle));
    // Testing if the prepared statement was created because the statement can be not created until this moment
    if (preparedStatement != null) {
      preparedStatement.close();
    }
  }

  @Override
  public void commit(final ConnectionHandle connectionHandle) {
    // TODO Fill this stub.
  }

  @Override
  public ExecuteResult execute(final StatementHandle statementHandle,
                               final List<TypedValue> typedValues, final long maxRowCount) {
    Preconditions.checkArgument(connection.id.equals(statementHandle.connectionId),
            "Connection IDs are not consistent");
    PreparedStatement preparedStatement = getPreparedStatement(statementHandle);

    if (preparedStatement == null) {
      throw new IllegalStateException("Prepared statement not found: " + statementHandle);
    }


    new AvaticaParameterBinder(preparedStatement, ((ArrowFlightConnection) connection).getBufferAllocator())
            .bind(typedValues);

    if (statementHandle.signature == null) {
      // Update query
      long updatedCount = preparedStatement.executeUpdate();
      return new ExecuteResult(Collections.singletonList(MetaResultSet.count(statementHandle.connectionId,
              statementHandle.id, updatedCount)));
    } else {
      // TODO Why is maxRowCount ignored?
      return new ExecuteResult(
              Collections.singletonList(MetaResultSet.create(
                      statementHandle.connectionId, statementHandle.id,
                      true, statementHandle.signature, null)));
    }
  }

  @Override
  public ExecuteResult execute(final StatementHandle statementHandle,
                               final List<TypedValue> typedValues, final int maxRowsInFirstFrame) {
    return execute(statementHandle, typedValues, (long) maxRowsInFirstFrame);
  }

  @Override
  public ExecuteBatchResult executeBatch(final StatementHandle statementHandle,
                                         final List<List<TypedValue>> parameterValuesList)
      throws IllegalStateException {
    Preconditions.checkArgument(connection.id.equals(statementHandle.connectionId),
            "Connection IDs are not consistent");
    PreparedStatement preparedStatement = getPreparedStatement(statementHandle);

    if (preparedStatement == null) {
      throw new IllegalStateException("Prepared statement not found: " + statementHandle);
    }

    final AvaticaParameterBinder binder = new AvaticaParameterBinder(preparedStatement,
            ((ArrowFlightConnection) connection).getBufferAllocator());
    for (int i = 0; i < parameterValuesList.size(); i++) {
      binder.bind(parameterValuesList.get(i), i);
    }

    // Update query
    long[] updatedCounts = {preparedStatement.executeUpdate()};
    return new ExecuteBatchResult(updatedCounts);
  }

  @Override
  public Frame fetch(final StatementHandle statementHandle, final long offset,
                     final int fetchMaxRowCount) {
    /*
     * ArrowFlightMetaImpl does not use frames.
     * Instead, we have accessors that contain a VectorSchemaRoot with
     * the results.
     */
    throw AvaticaConnection.HELPER.wrap(
        String.format("%s does not use frames.", this),
        AvaticaConnection.HELPER.unsupported());
  }

  private PreparedStatement prepareForHandle(final String query, StatementHandle handle) {
    final PreparedStatement preparedStatement =
        ((ArrowFlightConnection) connection).getClientHandler().prepare(query);
    handle.signature = newSignature(query, preparedStatement.getDataSetSchema(),
            preparedStatement.getParameterSchema());
    statementHandlePreparedStatementMap.put(new StatementHandleKey(handle), preparedStatement);
    return preparedStatement;
  }

  @Override
  public StatementHandle prepare(final ConnectionHandle connectionHandle,
                                 final String query, final long maxRowCount) {
    final StatementHandle handle = super.createStatement(connectionHandle);
    prepareForHandle(query, handle);
    return handle;
  }

  @Override
  public ExecuteResult prepareAndExecute(final StatementHandle statementHandle,
                                         final String query, final long maxRowCount,
                                         final PrepareCallback prepareCallback)
      throws NoSuchStatementException {
    return prepareAndExecute(
        statementHandle, query, maxRowCount, -1 /* Not used */, prepareCallback);
  }

  @Override
  public ExecuteResult prepareAndExecute(final StatementHandle handle,
                                         final String query, final long maxRowCount,
                                         final int maxRowsInFirstFrame,
                                         final PrepareCallback callback)
      throws NoSuchStatementException {
    try {
      PreparedStatement preparedStatement = prepareForHandle(query, handle);
      final StatementType statementType = preparedStatement.getType();

      final long updateCount =
          statementType.equals(StatementType.UPDATE) ? preparedStatement.executeUpdate() : -1;
      synchronized (callback.getMonitor()) {
        callback.clear();
        callback.assign(handle.signature, null, updateCount);
      }
      callback.execute();
      final MetaResultSet metaResultSet = MetaResultSet.create(handle.connectionId, handle.id,
          false, handle.signature, null);
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

  PreparedStatement getPreparedStatement(StatementHandle statementHandle) {
    return statementHandlePreparedStatementMap.get(new StatementHandleKey(statementHandle));
  }

  // Helper used to look up prepared statement instances later. Avatica doesn't give us the signature in
  // an UPDATE code path so we can't directly use StatementHandle as a map key.
  private static final class StatementHandleKey {
    public final String connectionId;
    public final int id;

    StatementHandleKey(String connectionId, int id) {
      this.connectionId = connectionId;
      this.id = id;
    }

    StatementHandleKey(StatementHandle statementHandle) {
      this.connectionId = statementHandle.connectionId;
      this.id = statementHandle.id;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      StatementHandleKey that = (StatementHandleKey) o;

      if (id != that.id) {
        return false;
      }
      return connectionId.equals(that.connectionId);
    }

    @Override
    public int hashCode() {
      int result = connectionId.hashCode();
      result = 31 * result + id;
      return result;
    }
  }
}
