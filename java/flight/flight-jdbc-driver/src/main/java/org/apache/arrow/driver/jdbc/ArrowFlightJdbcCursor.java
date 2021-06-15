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


import java.sql.SQLException;
import java.util.Calendar;
import java.util.List;

import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.util.ArrayImpl;
import org.apache.calcite.avatica.util.Cursor;

/**
 * Arrow Flight Jdbc's Cursor class.
 */
public class ArrowFlightJdbcCursor implements Cursor {
  @Override
  public List<Accessor> createAccessors(List<ColumnMetaData> list, Calendar calendar, ArrayImpl.Factory factory) {
    // TODO Fill this stub.
    return null;
  }

  @Override
  public boolean next() throws SQLException {
    // TODO Fill this stub.
    return false;
  }

  @Override
  public void close() {

  }

  @Override
  public boolean wasNull() throws SQLException {
    // TODO Fill this stub.
    return false;
  }
}
