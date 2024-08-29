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

package org.apache.arrow.adapter.jdbc.binder;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Calendar;

import org.apache.arrow.vector.DateMilliVector;

/**
 * A column binder for 64-bit dates.
 */
public class DateMilliBinder extends BaseColumnBinder<DateMilliVector> {
  private final Calendar calendar;

  public DateMilliBinder(DateMilliVector vector) {
    this(vector, null, Types.DATE);
  }

  public DateMilliBinder(DateMilliVector vector, Calendar calendar) {
    this(vector, calendar, Types.DATE);
  }


  public DateMilliBinder(DateMilliVector vector, Calendar calendar, int jdbcType) {
    super(vector, jdbcType);
    this.calendar = calendar;
  }

  @Override
  public void bind(PreparedStatement statement, int parameterIndex, int rowIndex) throws SQLException {
    final long index = (long) rowIndex * DateMilliVector.TYPE_WIDTH;
    final Date value = new Date(vector.getDataBuffer().getLong(index));
    if (calendar == null) {
      statement.setDate(parameterIndex, value);
    } else {
      statement.setDate(parameterIndex, value, calendar);
    }
  }
}
