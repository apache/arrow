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

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;

import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.util.DecimalUtility;

/**
 * A binder for 128-bit decimals.
 */
public class Decimal128Binder extends BaseColumnBinder<DecimalVector> {
  public Decimal128Binder(DecimalVector vector) {
    this(vector, Types.DECIMAL);
  }

  public Decimal128Binder(DecimalVector vector, int jdbcType) {
    super(vector, jdbcType);
  }

  @Override
  public void bind(PreparedStatement statement, int parameterIndex, int rowIndex) throws SQLException {
    final BigDecimal value = DecimalUtility.getBigDecimalFromArrowBuf(
        vector.getDataBuffer(), rowIndex, vector.getScale(), DecimalVector.TYPE_WIDTH);
    statement.setBigDecimal(parameterIndex, value);
  }
}
