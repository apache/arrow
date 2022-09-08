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

import org.apache.arrow.vector.BitVector;

/** A column binder for booleans. */
public class BitBinder extends BaseColumnBinder<BitVector> {
  public BitBinder(BitVector vector) {
    this(vector, Types.BOOLEAN);
  }

  public BitBinder(BitVector vector, int jdbcType) {
    super(vector, jdbcType);
  }

  @Override
  public void bind(PreparedStatement statement, int parameterIndex, int rowIndex) throws SQLException {
    // See BitVector#getBit
    final int byteIndex = rowIndex >> 3;
    final byte b = vector.getDataBuffer().getByte(byteIndex);
    final int bitIndex = rowIndex & 7;
    final int value = (b >> bitIndex) & 0x01;
    statement.setBoolean(parameterIndex, value != 0);
  }
}
