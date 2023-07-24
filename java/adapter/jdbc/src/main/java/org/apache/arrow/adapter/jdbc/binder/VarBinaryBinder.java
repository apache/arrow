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

import org.apache.arrow.memory.util.ArrowBufPointer;
import org.apache.arrow.vector.ElementAddressableVector;
import org.apache.arrow.vector.FieldVector;

/**
 * A binder for variable-width binary types.
 *
 * @param <T> The binary vector.
 */
public class VarBinaryBinder<T extends FieldVector & ElementAddressableVector> extends BaseColumnBinder<T> {
  private final ArrowBufPointer element;

  /**
   * Create a binder for the given vector using the given JDBC type for null values.
   *
   * @param vector   The vector to draw values from.
   * @param jdbcType The JDBC type code.
   */
  public VarBinaryBinder(T vector, int jdbcType) {
    super(vector, jdbcType);
    this.element = new ArrowBufPointer();
  }

  @Override
  public void bind(PreparedStatement statement, int parameterIndex, int rowIndex) throws SQLException {
    vector.getDataPointer(rowIndex, element);
    if (element.getBuf() == null) {
      statement.setNull(parameterIndex, jdbcType);
      return;
    }
    if (element.getLength() > (long) Integer.MAX_VALUE) {
      final String message = String.format("Length of value at index %d (%d) exceeds Integer.MAX_VALUE",
          rowIndex, element.getLength());
      throw new RuntimeException(message);
    }
    byte[] binaryData = new byte[(int) element.getLength()];
    element.getBuf().getBytes(element.getOffset(), binaryData);
    statement.setBytes(parameterIndex, binaryData);
  }
}
