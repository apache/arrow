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

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Types;

import org.apache.arrow.vector.BaseFixedWidthVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeNanoVector;

/**
 * A binder for 64-bit time types.
 */
public class Time64Binder extends BaseColumnBinder<BaseFixedWidthVector> {
  private static final long TYPE_WIDTH = 8;

  private final long factor;

  public Time64Binder(TimeMicroVector vector) {
    this(vector, Types.TIME);
  }

  public Time64Binder(TimeNanoVector vector) {
    this(vector, Types.TIME);
  }

  public Time64Binder(TimeMicroVector vector, int jdbcType) {
    this(vector, /*factor*/1_000, jdbcType);
  }

  public Time64Binder(TimeNanoVector vector, int jdbcType) {
    this(vector, /*factor*/1_000_000, jdbcType);
  }

  Time64Binder(BaseFixedWidthVector vector, long factor, int jdbcType) {
    super(vector, jdbcType);
    this.factor = factor;
  }

  @Override
  public void bind(PreparedStatement statement, int parameterIndex, int rowIndex) throws SQLException {
    // TODO: option to throw on truncation (vendor Guava IntMath#multiply)
    final Time value = new Time(vector.getDataBuffer().getLong(rowIndex * TYPE_WIDTH) / factor);
    statement.setTime(parameterIndex, value);
  }
}
