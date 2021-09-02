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

import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.arrow.driver.jdbc.client.FlightClientHandler;
import org.apache.arrow.flight.FlightInfo;
import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.AvaticaPreparedStatement;
import org.apache.calcite.avatica.Meta;


/**
 * Arrow Flight JBCS's implementation {@link PreparedStatement}.
 *
 */
public class ArrowFlightPreparedStatement extends AvaticaPreparedStatement {

  private final FlightClientHandler.PreparedStatement preparedStatement;

  ArrowFlightPreparedStatement(final AvaticaConnection connection,
      final Meta.StatementHandle handle,
      final Meta.Signature signature, final int resultSetType,
      final int resultSetConcurrency, final int resultSetHoldability) throws SQLException {
    super(connection, handle, signature, resultSetType, resultSetConcurrency,
        resultSetHoldability);
    final FlightClientHandler clientHandler = ((ArrowFlightConnection) connection).getClientHandler();
    this.preparedStatement = clientHandler.prepare(signature.sql);
  }

  /**
   * Returns a FlightInfo for PreparedStatement query execution
   */
  public FlightInfo getFlightInfoToExecuteQuery() throws SQLException {
    return preparedStatement.executeQuery();
  }

  @Override
  public synchronized void close() throws SQLException {
    this.preparedStatement.close();
    super.close();
  }
}
