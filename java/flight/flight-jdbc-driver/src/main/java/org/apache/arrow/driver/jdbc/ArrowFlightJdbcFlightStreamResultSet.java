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
    Optional.ofNullable(getFlightStreamQueue()).ifPresent(AutoCloseables::closeNoChecked);
    try {
      ArrowFlightConnection connection = (ArrowFlightConnection) getStatement().getConnection();
      flightStreamQueue = createNewQueue(connection.getExecutorService());
    } catch (final SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public FlightStream getCurrentFlightStream() {
    return currentFlightStream;
  }

  private void loadNewFlightStream() throws SQLException {
    Optional.ofNullable(getCurrentFlightStream()).ifPresent(AutoCloseables::closeNoChecked);
    this.currentFlightStream = getFlightStreamQueue().next();
  }

  @Override
  protected AvaticaResultSet execute() throws SQLException {
    loadNewQueue();
    getFlightStreamQueue().enqueue(
        ((ArrowFlightConnection) getStatement().getConnection())
            .getClient().readilyGetFlightStreams(signature.sql));
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

      currentFlightStream = flightStreamQueue.next();

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
    FlightStream currentFlightStream = getCurrentFlightStream();
    if (currentFlightStream != null) {
      currentFlightStream.cancel("Cancel", null);
    }

    FlightStreamQueue flightStreamQueue = getFlightStreamQueue();
    if (flightStreamQueue != null) {
      try {
        flightStreamQueue.close();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public synchronized void close() {
    try {
      AutoCloseables.close(getFlightStreamQueue(), getCurrentFlightStream());
    } catch (final Exception e) {
      throw new RuntimeException(e);
    } finally {
      super.close();
    }
  }
}
