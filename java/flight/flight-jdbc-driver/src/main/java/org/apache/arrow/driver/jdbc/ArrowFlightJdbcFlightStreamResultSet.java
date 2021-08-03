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

import org.apache.arrow.driver.jdbc.utils.FlightStreamQueue;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.vector.VectorSchemaRoot;
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

  @Override
  protected AvaticaResultSet execute() throws SQLException {
    try {
      final ArrowFlightConnection connection = (ArrowFlightConnection) statement.getConnection();
      flightStreamQueue = new FlightStreamQueue(connection.getExecutorService());

      final List<FlightStream> flightStreams = connection
          .getClient()
          .getFlightStreams(signature.sql);

      flightStreams.forEach(flightStreamQueue::enqueue);

      currentFlightStream = flightStreamQueue.next();
      final VectorSchemaRoot root = currentFlightStream.getRoot();
      execute(root);
    } catch (SQLException e) {
      throw e;
    } catch (Exception e) {
      throw new SQLException(e);
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

      currentFlightStream.getRoot().clear();
      if (currentFlightStream.next()) {
        execute(currentFlightStream.getRoot());
        continue;
      }

      flightStreamQueue.enqueue(currentFlightStream);
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
  public void close() {
    super.close();

    try {
      if (this.currentFlightStream != null) {
        this.currentFlightStream.close();
      }

      flightStreamQueue.close();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
