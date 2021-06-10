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
import org.apache.calcite.avatica.AvaticaPreparedStatement;
import org.apache.calcite.avatica.AvaticaResultSet;
import org.apache.calcite.avatica.AvaticaSpecificDatabaseMetaData;
import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.avatica.Meta.Frame;
import org.apache.calcite.avatica.Meta.Signature;
import org.apache.calcite.avatica.Meta.StatementHandle;
import org.apache.calcite.avatica.QueryState;
import org.apache.calcite.avatica.UnregisteredDriver;

/**
 * Factory for the Arrow Flight JDBC Driver.
 */
public class ArrowFlightFactory implements AvaticaFactory {

  @Override
  public int getJdbcMajorVersion() {
    return ArrowFlightJdbcDriver.Version.CURRENT
        .getDriverVersion().majorVersion;
  }

  @Override
  public int getJdbcMinorVersion() {
    return ArrowFlightJdbcDriver.Version.CURRENT
        .getDriverVersion().minorVersion;
  }

  @Override
  public AvaticaConnection newConnection(UnregisteredDriver driver,
      AvaticaFactory factory, String url, Properties info) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public AvaticaSpecificDatabaseMetaData newDatabaseMetaData(
      AvaticaConnection connection) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public AvaticaPreparedStatement newPreparedStatement(
      AvaticaConnection connection, StatementHandle handle, Signature signature,
      int resultSetType, int resultSetConcurrency, int resultSetHoldability)
      throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public AvaticaResultSet newResultSet(AvaticaStatement statement,
      QueryState state, Signature signature, TimeZone timeZone,
      Frame startingFrame) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ResultSetMetaData newResultSetMetaData(AvaticaStatement statement,
      Signature signature) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public AvaticaStatement newStatement(AvaticaConnection connection,
      StatementHandle handle, int resultSetType, int resultSetConcurrency,
      int resultSetHoldability) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

}
