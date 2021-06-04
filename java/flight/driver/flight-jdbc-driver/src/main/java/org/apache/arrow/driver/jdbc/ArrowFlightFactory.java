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

  public ArrowFlightFactory() {
    // TODO Auto-generated constructor stub
  }

  @Override
  public int getJdbcMajorVersion() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int getJdbcMinorVersion() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public AvaticaConnection newConnection(UnregisteredDriver arg0,
      AvaticaFactory arg1, String arg2, Properties arg3) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public AvaticaSpecificDatabaseMetaData newDatabaseMetaData(
      AvaticaConnection arg0) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public AvaticaPreparedStatement newPreparedStatement(AvaticaConnection arg0,
      StatementHandle arg1, Signature arg2, int arg3, int arg4, int arg5)
      throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public AvaticaResultSet newResultSet(AvaticaStatement arg0, QueryState arg1,
      Signature arg2, TimeZone arg3, Frame arg4) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ResultSetMetaData newResultSetMetaData(AvaticaStatement arg0,
      Signature arg1) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public AvaticaStatement newStatement(AvaticaConnection arg0,
      StatementHandle arg1, int arg2, int arg3, int arg4) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

}
