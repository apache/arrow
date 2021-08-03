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
import static org.apache.arrow.driver.jdbc.utils.FlightStreamQueue.createNewQueue;
import static org.apache.arrow.util.Preconditions.checkNotNull;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Optional;
import java.util.TimeZone;

import org.apache.arrow.driver.jdbc.utils.FlightStreamQueue;
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

  private FlightStream currentFlightStream;
  private FlightStreamQueue flightStreamQueue;

  ArrowFlightJdbcFlightStreamResultSet(AvaticaStatement statement,
                                       QueryState state,
                                       Meta.Signature signature,
                                       ResultSetMetaData resultSetMetaData,
                                       TimeZone timeZone,
                                       Meta.Frame firstFrame) throws SQLException {
    super(statement, state, signature, resultSetMetaData, timeZone, firstFrame);
  }

  protected FlightStreamQueue getFlightStreamQueue() {
    return flightStreamQueue;
  }

  private void loadNewQueue() {
    final Optional<FlightStreamQueue> oldQueue = Optional.ofNullable(getFlightStreamQueue());
    try {
      flightStreamQueue =
          checkNotNull(createNewQueue(((ArrowFlightConnection) getStatement().getConnection()).getExecutorService()));
    } catch (final SQLException e) {
      throw new RuntimeException(e);
    } finally {
      oldQueue.ifPresent(AutoCloseables::closeNoChecked);
    }
  }

  public FlightStream getCurrentFlightStream() {
    return currentFlightStream;
  }

  private void loadNewFlightStream() {
    final Optional<FlightStream> oldQueue = Optional.ofNullable(getCurrentFlightStream());
    try {
      this.currentFlightStream = checkNotNull(getFlightStreamQueue().next());
    } catch (final Exception e) {
      throw new RuntimeException(e);
    } finally {
      oldQueue.ifPresent(AutoCloseables::closeNoChecked);
    }
  }

  @Override
  protected AvaticaResultSet execute() throws SQLException {
    loadNewQueue();
    getFlightStreamQueue().enqueue(
        ((ArrowFlightConnection) getStatement().getConnection())
            .getClient().lazilyGetFlightStreams(signature.sql));
    loadNewFlightStream();
    // Ownership of the root will be passed onto the cursor.
    execute(currentFlightStream.getRoot());
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

      currentFlightStream.getRoot().clear();
      if (currentFlightStream.next()) {
        execute(currentFlightStream.getRoot());
        continue;
      }

      flightStreamQueue.enqueue(currentFlightStream);
      try {
        currentFlightStream = flightStreamQueue.next();
      } catch (final Exception e) {
        throw new SQLException(e);
      }

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
  public synchronized void close() {
    try {
      final FlightStream currentStream = getCurrentFlightStream();
      AutoCloseables.close(flightStreamQueue, isNull(currentStream) ? null : currentStream);
    } catch (final Exception e) {
      throw new RuntimeException(e);
    } finally {
      super.close();
    }
  }
}
