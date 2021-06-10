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
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Properties;
import java.util.TimeZone;

import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.AvaticaSpecificDatabaseMetaData;
import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.QueryState;

/**
 * Factory for the Arrow Flight JDBC Driver.
 */
public class ArrowFlightJdbcFactory extends AbstractFactory {
  // This need to be public so Avatica can call this constructor
  public ArrowFlightJdbcFactory() {
    this(4, 1);
  }

  protected ArrowFlightJdbcFactory(int major, int minor) {
    super(major, minor);
  }

  @Override
  ArrowFlightConnection newConnection(ArrowFlightJdbcDriver driver,
                                      AbstractFactory factory,
                                      String url,
                                      Properties info) throws SQLException {
    return new ArrowFlightConnection(driver, factory, url, info);
  }

  @Override
  public AvaticaStatement newStatement(AvaticaConnection avaticaConnection,
                                       Meta.StatementHandle statementHandle,
                                       int resultType,
                                       int resultSetConcurrency,
                                       int resultSetHoldability) throws SQLException {
    return null;
  }

  @Override
  public ArrowFlightJdbcPreparedStatement newPreparedStatement(AvaticaConnection connection,
                                                               Meta.StatementHandle statementHandle,
                                                               Meta.Signature signature,
                                                               int resultType,
                                                               int resultSetConcurrency,
                                                               int resultSetHoldability) throws SQLException {

    ArrowFlightConnection arrowFlightConnection = (ArrowFlightConnection) connection;

    return new ArrowFlightJdbcPreparedStatement(arrowFlightConnection, statementHandle,
            signature, resultType, resultSetConcurrency, resultSetHoldability, null);
  }

  @Override
  public ArrowFlightResultSet newResultSet(AvaticaStatement statement,
                                           QueryState state,
                                           Meta.Signature signature,
                                           TimeZone timeZone,
                                           Meta.Frame frame) throws SQLException {
    return null;
  }

  @Override
  public AvaticaSpecificDatabaseMetaData newDatabaseMetaData(AvaticaConnection connection) {
    return new ArrowDatabaseMetadata(connection);
  }

  @Override
  public ResultSetMetaData newResultSetMetaData(AvaticaStatement avaticaStatement,
                                                Meta.Signature signature) throws SQLException {
    return null;
  }

  @Override
  public int getJdbcMajorVersion() {
    return major;
  }

  @Override
  public int getJdbcMinorVersion() {
    return minor;
  }

  private static class ArrowFlightJdbcPreparedStatement extends ArrowFlightPreparedStatement {

    public ArrowFlightJdbcPreparedStatement(AvaticaConnection connection,
                                            Meta.StatementHandle h,
                                            Meta.Signature signature,
                                            int resultSetType,
                                            int resultSetConcurrency,
                                            int resultSetHoldability,
                                            PreparedStatement preparedStatement) throws SQLException {
      super(connection, h, signature, resultSetType,
              resultSetConcurrency, resultSetHoldability, preparedStatement);
    }
  }
}
