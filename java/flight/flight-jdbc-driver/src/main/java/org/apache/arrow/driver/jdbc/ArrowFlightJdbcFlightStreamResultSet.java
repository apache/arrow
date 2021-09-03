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
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.util.AutoCloseables;
import org.apache.calcite.avatica.AvaticaResultSet;
import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.QueryState;

/**
 * {@link ResultSet} implementation for Arrow Flight used to access the results of multiple {@link FlightStream}
 * objects.
 */
public class ArrowFlightJdbcFlightStreamResultSet extends ArrowFlightJdbcVectorSchemaRootResultSet {

  private final ArrowFlightConnection connection;
  private FlightStream currentFlightStream;
  private FlightStreamQueue flightStreamQueue;

  ArrowFlightJdbcFlightStreamResultSet(final AvaticaStatement statement,
                                       final QueryState state,
                                       final Meta.Signature signature,
                                       final ResultSetMetaData resultSetMetaData,
                                       final TimeZone timeZone,
                                       final Meta.Frame firstFrame,
                                       final ArrowFlightConnection connection) throws SQLException {
    super(statement, state, signature, resultSetMetaData, timeZone, firstFrame);
    this.connection = connection;
  }

  protected FlightStreamQueue getFlightStreamQueue() {
    return flightStreamQueue;
  }

  private void loadNewQueue() {
    Optional.ofNullable(getFlightStreamQueue()).ifPresent(AutoCloseables::closeNoChecked);
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
    if (statement instanceof ArrowFlightStatement) {
      return execute(((ArrowFlightStatement) statement).getFlightInfoToExecuteQuery());
    } else if (statement instanceof ArrowFlightPreparedStatement) {
      return execute(((ArrowFlightPreparedStatement) statement).getFlightInfoToExecuteQuery());
    }

    throw new IllegalStateException();
  }

  private AvaticaResultSet execute(final FlightInfo flightInfo) throws SQLException {
    loadNewQueue();
    getFlightStreamQueue().enqueue(connection.getClientHandler().getStreams(flightInfo));
    loadNewFlightStream();

    // Ownership of the root will be passed onto the cursor.
    if (currentFlightStream != null) {
      execute(currentFlightStream.getRoot());
    }
    return this;
  }

  @Override
  public boolean next() throws SQLException {
    while (true) {
      final boolean hasNext = super.next();
      final int maxRows = statement.getMaxRows();
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
          execute(currentFlightStream.getRoot());
          continue;
        }

        flightStreamQueue.enqueue(currentFlightStream);
      }

      currentFlightStream = getNextFlightStream(false);

      if (currentFlightStream != null) {
        execute(currentFlightStream.getRoot());
        continue;
      }

      if (statement.isCloseOnCompletion()) {
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

    final FlightStreamQueue flightStreamQueue = getFlightStreamQueue();
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
      AutoCloseables.close(currentFlightStream, getFlightStreamQueue());
    } catch (final Exception e) {
      throw new RuntimeException(e);
    } finally {
      super.close();
    }
  }

  private FlightStream getNextFlightStream(final boolean isExecution) throws SQLException {
    if (isExecution) {
      final int statementTimeout = statement.getQueryTimeout();
      return statementTimeout != 0 ?
          flightStreamQueue.next(statementTimeout, TimeUnit.SECONDS) : flightStreamQueue.next();
    } else {
      return flightStreamQueue.next();
    }
  }
}
