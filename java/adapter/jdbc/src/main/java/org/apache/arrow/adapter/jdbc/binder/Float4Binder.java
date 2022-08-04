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
import java.sql.Types;

import org.apache.arrow.vector.Float4Vector;

/**
 * A binder for 32-bit floats.
 */
public class Float4Binder extends BaseColumnBinder<Float4Vector> {
  public Float4Binder(Float4Vector vector) {
    this(vector, Types.REAL);
  }

  public Float4Binder(Float4Vector vector, int jdbcType) {
    super(vector, jdbcType);
  }

  @Override
  public void bind(PreparedStatement statement, int parameterIndex, int rowIndex) throws SQLException {
    final float value = vector.getDataBuffer().getFloat((long) rowIndex * Float4Vector.TYPE_WIDTH);
    statement.setFloat(parameterIndex, value);
  }
}
