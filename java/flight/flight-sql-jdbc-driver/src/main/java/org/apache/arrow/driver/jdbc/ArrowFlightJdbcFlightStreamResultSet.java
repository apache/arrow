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

import static org.apache.arrow.driver.jdbc.utils.FlightStreamQueue.createNewQueue;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Optional;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import org.apache.arrow.driver.jdbc.utils.FlightStreamQueue;
import org.apache.arrow.driver.jdbc.utils.VectorSchemaRootTransformer;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.avatica.AvaticaResultSet;
import org.apache.calcite.avatica.AvaticaResultSetMetaData;
import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.QueryState;

/**
 * {@link ResultSet} implementation for Arrow Flight used to access the results of multiple {@link FlightStream}
 * objects.
 */
public final class ArrowFlightJdbcFlightStreamResultSet
    extends ArrowFlightJdbcVectorSchemaRootResultSet {

  private final ArrowFlightConnection connection;
  private FlightStream currentFlightStream;
  private FlightStreamQueue flightStreamQueue;

  private VectorSchemaRootTransformer transformer;
  private VectorSchemaRoot currentVectorSchemaRoot;

  private Schema schema;

  ArrowFlightJdbcFlightStreamResultSet(final AvaticaStatement statement,
                                       final QueryState state,
                                       final Meta.Signature signature,
                                       final ResultSetMetaData resultSetMetaData,
                                       final TimeZone timeZone,
                                       final Meta.Frame firstFrame) throws SQLException {
    super(statement, state, signature, resultSetMetaData, timeZone, firstFrame);
    this.connection = (ArrowFlightConnection) statement.connection;
  }

  ArrowFlightJdbcFlightStreamResultSet(final ArrowFlightConnection connection,
                                       final QueryState state,
                                       final Meta.Signature signature,
                                       final ResultSetMetaData resultSetMetaData,
                                       final TimeZone timeZone,
                                       final Meta.Frame firstFrame) throws SQLException {
    super(null, state, signature, resultSetMetaData, timeZone, firstFrame);
    this.connection = connection;
  }

  /**
   * Create a {@link ResultSet} which pulls data from given {@link FlightInfo}.
   *
   * @param connection  The connection linked to the returned ResultSet.
   * @param flightInfo  The FlightInfo from which data will be iterated by the returned ResultSet.
   * @param transformer Optional transformer for processing VectorSchemaRoot before access from ResultSet
   * @return A ResultSet which pulls data from given FlightInfo.
   */
  static ArrowFlightJdbcFlightStreamResultSet fromFlightInfo(
      final ArrowFlightConnection connection,
      final FlightInfo flightInfo,
      final VectorSchemaRootTransformer transformer) throws SQLException {
    // Similar to how org.apache.calcite.avatica.util.ArrayFactoryImpl does

    final TimeZone timeZone = TimeZone.getDefault();
    final QueryState state = new QueryState();

    final Meta.Signature signature = ArrowFlightMetaImpl.newSignature(null);

    final AvaticaResultSetMetaData resultSetMetaData =
        new AvaticaResultSetMetaData(null, null, signature);
    final ArrowFlightJdbcFlightStreamResultSet resultSet =
        new ArrowFlightJdbcFlightStreamResultSet(connection, state, signature, resultSetMetaData,
            timeZone, null);

    resultSet.transformer = transformer;

    resultSet.execute(flightInfo);
    return resultSet;
  }

  private void loadNewQueue() {
    Optional.ofNullable(flightStreamQueue).ifPresent(AutoCloseables::closeNoChecked);
    flightStreamQueue = createNewQueue(connection.getExecutorService());
  }

  private void loadNewFlightStream() throws SQLException {
    if (currentFlightStream != null) {
      AutoCloseables.closeNoChecked(currentFlightStream);
    }
    this.currentFlightStream = getNextFlightStream(true);
  }

  @Override
  protected AvaticaResultSet execute() throws SQLException {
    final FlightInfo flightInfo = ((ArrowFlightInfoStatement) statement).executeFlightInfoQuery();

    if (flightInfo != null) {
      schema = flightInfo.getSchema();
      execute(flightInfo);
    }
    return this;
  }

  private void execute(final FlightInfo flightInfo) throws SQLException {
    loadNewQueue();
    flightStreamQueue.enqueue(connection.getClientHandler().getStreams(flightInfo));
    loadNewFlightStream();

    // Ownership of the root will be passed onto the cursor.
    if (currentFlightStream != null) {
      executeForCurrentFlightStream();
    }
  }

  private void executeForCurrentFlightStream() throws SQLException {
    final VectorSchemaRoot originalRoot = currentFlightStream.getRoot();

    if (transformer != null) {
      try {
        currentVectorSchemaRoot = transformer.transform(originalRoot, currentVectorSchemaRoot);
      } catch (final Exception e) {
        throw new SQLException("Failed to transform VectorSchemaRoot.", e);
      }
    } else {
      currentVectorSchemaRoot = originalRoot;
    }

    if (schema != null) {
      execute(currentVectorSchemaRoot, schema);
    } else {
      execute(currentVectorSchemaRoot);
    }
  }

  @Override
  public boolean next() throws SQLException {
    if (currentVectorSchemaRoot == null) {
      return false;
    }
    while (true) {
      final boolean hasNext = super.next();
      final int maxRows = statement != null ? statement.getMaxRows() : 0;
      if (maxRows != 0 && this.getRow() > maxRows) {
        if (statement.isCloseOnCompletion()) {
          statement.close();
        }
        return false;
      }

      if (hasNext) {
        return true;
      }

      if (currentFlightStream != null) {
        currentFlightStream.getRoot().clear();
        if (currentFlightStream.next()) {
          executeForCurrentFlightStream();
          continue;
        }

        flightStreamQueue.enqueue(currentFlightStream);
      }

      currentFlightStream = getNextFlightStream(false);

      if (currentFlightStream != null) {
        executeForCurrentFlightStream();
        continue;
      }

      if (statement != null && statement.isCloseOnCompletion()) {
        statement.close();
      }

      return false;
    }
  }

  @Override
  protected void cancel() {
    super.cancel();
    final FlightStream currentFlightStream = this.currentFlightStream;
    if (currentFlightStream != null) {
      currentFlightStream.cancel("Cancel", null);
    }

    if (flightStreamQueue != null) {
      try {
        flightStreamQueue.close();
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public synchronized void close() {
    try {
      if (flightStreamQueue != null) {
        // flightStreamQueue should close currentFlightStream internally
        flightStreamQueue.close();
      } else if (currentFlightStream != null) {
        // close is only called for currentFlightStream if there's no queue
        currentFlightStream.close();
      }
    } catch (final Exception e) {
      throw new RuntimeException(e);
    } finally {
      super.close();
    }
  }

  private FlightStream getNextFlightStream(final boolean isExecution) throws SQLException {
    if (isExecution) {
      final int statementTimeout = statement != null ? statement.getQueryTimeout() : 0;
      return statementTimeout != 0 ?
          flightStreamQueue.next(statementTimeout, TimeUnit.SECONDS) : flightStreamQueue.next();
    } else {
      return flightStreamQueue.next();
    }
  }
}
