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
import java.sql.SQLException;
import java.util.TimeZone;

import org.apache.calcite.avatica.AvaticaResultSet;
import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.avatica.Meta.Frame;
import org.apache.calcite.avatica.Meta.Signature;
import org.apache.calcite.avatica.QueryState;

/**
 * The {@link ResultSet} implementation for Arrow Flight.
 */
public class ArrowFlightResultSet extends AvaticaResultSet {

  public ArrowFlightResultSet(final AvaticaStatement statement, final QueryState state,
      final Signature signature,
      final ArrowFlightResultSetMetadata resultSetMetaData,
      final TimeZone timeZone, final Frame firstFrame) throws SQLException {
    super(statement, state, signature, resultSetMetaData, timeZone, firstFrame);
    // TODO Auto-generated constructor stub
  }

  @Override
  protected AvaticaResultSet execute() throws SQLException {

    ArrowFlightJdbcCursor cursor = new ArrowFlightJdbcCursor();

    super.execute2(cursor, this.signature.columns);

    return this;
  }
}
