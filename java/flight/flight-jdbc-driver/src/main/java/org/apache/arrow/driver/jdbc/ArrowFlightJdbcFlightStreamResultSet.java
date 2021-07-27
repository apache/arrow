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
import java.util.Iterator;
import java.util.TimeZone;

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

  private FlightStream flightStream;
  private Iterator<FlightStream> flightStreamIterator;

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
      flightStreamIterator = ((ArrowFlightConnection) statement
          .getConnection())
          .getClient()
          .getFlightStreams(signature.sql).iterator();

      flightStream = flightStreamIterator.next();
      final VectorSchemaRoot root = flightStream.getRoot();
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
    final boolean hasNext = super.next();
    if (hasNext) {
      return true;
    }

    flightStream.getRoot().clear();
    if (flightStream.next()) {
      execute(flightStream.getRoot());
      return next();
    }

    if (flightStreamIterator.hasNext()) {
      if (flightStream != null) {
        try {
          flightStream.close();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
      flightStream = flightStreamIterator.next();
      execute(flightStream.getRoot());
      return next();
    }
    return false;
  }

  @Override
  public void close() {
    super.close();

    try {
      this.flightStream.close();

      while (flightStreamIterator.hasNext()) {
        flightStreamIterator.next().close();
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
