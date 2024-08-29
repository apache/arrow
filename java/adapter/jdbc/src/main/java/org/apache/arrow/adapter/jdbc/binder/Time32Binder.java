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
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeSecVector;

/**
 * A binder for 32-bit time types.
 */
public class Time32Binder extends BaseColumnBinder<BaseFixedWidthVector> {
  private static final long TYPE_WIDTH = 4;

  private final long factor;

  public Time32Binder(TimeSecVector vector) {
    this(vector, Types.TIME);
  }

  public Time32Binder(TimeMilliVector vector) {
    this(vector, Types.TIME);
  }

  public Time32Binder(TimeSecVector vector, int jdbcType) {
    this(vector, /*factor*/1_000, jdbcType);
  }

  public Time32Binder(TimeMilliVector vector, int jdbcType) {
    this(vector, /*factor*/1, jdbcType);
  }

  Time32Binder(BaseFixedWidthVector vector, long factor, int jdbcType) {
    super(vector, jdbcType);
    this.factor = factor;
  }

  @Override
  public void bind(PreparedStatement statement, int parameterIndex, int rowIndex) throws SQLException {
    // TODO: multiply with overflow
    // TODO: take in a Calendar as well?
    final Time value = new Time(vector.getDataBuffer().getInt(rowIndex * TYPE_WIDTH) * factor);
    statement.setTime(parameterIndex, value);
  }
}
