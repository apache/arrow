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

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Properties;
import java.util.TimeZone;

import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.AvaticaFactory;
import org.apache.calcite.avatica.AvaticaSpecificDatabaseMetaData;
import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.QueryState;
import org.apache.calcite.avatica.UnregisteredDriver;

/**
 * Factory for the Arrow Flight JDBC Driver.
 */
public class ArrowFlightJdbcFactory implements AvaticaFactory {
  private final int major;
  private final int minor;

  // This need to be public so Avatica can call this constructor
  public ArrowFlightJdbcFactory() {
    this(4, 1);
  }

  private ArrowFlightJdbcFactory(final int major, final int minor) {
    this.major = major;
    this.minor = minor;
  }

  @Override
  public AvaticaConnection newConnection(final UnregisteredDriver driver,
      final AvaticaFactory factory,
      final String url,
      final Properties info) throws SQLException {
    return new ArrowFlightConnection((ArrowFlightJdbcDriver) driver,
        factory, url, info);
  }

  @Override
  public AvaticaStatement newStatement(
      final AvaticaConnection avaticaConnection,
      final Meta.StatementHandle statementHandle,
      final int resultType,
      final int resultSetConcurrency,
      final int resultSetHoldability) throws SQLException {
    return null;
  }

  @Override
  public ArrowFlightPreparedStatement newPreparedStatement(
      final AvaticaConnection connection,
      final Meta.StatementHandle statementHandle,
      final Meta.Signature signature,
      final int resultType,
      final int resultSetConcurrency,
      final int resultSetHoldability) throws SQLException {

    final ArrowFlightConnection arrowFlightConnection =
        (ArrowFlightConnection) connection;

    return new ArrowFlightPreparedStatement(arrowFlightConnection, statementHandle,
        signature, resultType, resultSetConcurrency, resultSetHoldability, null);
  }

  @Override
  public ArrowFlightResultSet newResultSet(final AvaticaStatement statement,
      final QueryState state,
      final Meta.Signature signature,
      final TimeZone timeZone,
      final Meta.Frame frame) throws SQLException {
    final ResultSetMetaData metaData = newResultSetMetaData(statement, signature);

    return new ArrowFlightResultSet(statement, state, signature, metaData, timeZone, frame);
  }

  @Override
  public AvaticaSpecificDatabaseMetaData newDatabaseMetaData(
      final AvaticaConnection connection) {
    return new ArrowDatabaseMetadata(connection);
  }

  @Override
  public ResultSetMetaData newResultSetMetaData(
      final AvaticaStatement avaticaStatement,
      final Meta.Signature signature) throws SQLException {
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
}
