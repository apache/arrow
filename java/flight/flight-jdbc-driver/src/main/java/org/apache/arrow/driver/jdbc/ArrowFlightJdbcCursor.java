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
import java.util.Iterator;
import java.util.List;

import com.google.common.base.Optional;
import org.apache.arrow.vector.FieldVector;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.util.ArrayImpl;
import org.apache.calcite.avatica.util.Cursor;
import org.apache.calcite.avatica.util.IteratorCursor;
import org.apache.calcite.avatica.util.PositionedCursor;

/**
 * Arrow Flight Jdbc's Cursor class.
 */
public class ArrowFlightJdbcCursor extends IteratorCursor<FieldVector> {

  protected ArrowFlightJdbcCursor(Iterator<FieldVector> iterator) {
    super(iterator);
  }

  @Override
  protected Getter createGetter(int i) {
    return new Getter() {

      protected int index = 0;

      @Override
      public Object getObject() throws SQLException {

        Optional<Object> o = Optional.absent();

        try {
          o = Optional.fromNullable(((FieldVector) ArrowFlightJdbcCursor
                  .super
                  .current())
                  .getObject(index++));
        } catch (Exception e) {
          throw new SQLException(e);
        }

        ArrowFlightJdbcCursor.this.wasNull[0] = o.isPresent();
        return o.get();
      }

      @Override
      public boolean wasNull() throws SQLException {
        return false;
      }
    };
  }
}
