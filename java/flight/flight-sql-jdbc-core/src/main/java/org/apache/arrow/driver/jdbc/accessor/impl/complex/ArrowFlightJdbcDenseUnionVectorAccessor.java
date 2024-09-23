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
package org.apache.arrow.driver.jdbc.accessor.impl.complex;

import java.util.function.IntSupplier;
import org.apache.arrow.driver.jdbc.accessor.ArrowFlightJdbcAccessor;
import org.apache.arrow.driver.jdbc.accessor.ArrowFlightJdbcAccessorFactory;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.complex.DenseUnionVector;

/** Accessor for the Arrow type {@link DenseUnionVector}. */
public class ArrowFlightJdbcDenseUnionVectorAccessor
    extends AbstractArrowFlightJdbcUnionVectorAccessor {

  private final DenseUnionVector vector;

  /**
   * Instantiate an accessor for a {@link DenseUnionVector}.
   *
   * @param vector an instance of a DenseUnionVector.
   * @param currentRowSupplier the supplier to track the rows.
   * @param setCursorWasNull the consumer to set if value was null.
   */
  public ArrowFlightJdbcDenseUnionVectorAccessor(
      DenseUnionVector vector,
      IntSupplier currentRowSupplier,
      ArrowFlightJdbcAccessorFactory.WasNullConsumer setCursorWasNull) {
    super(currentRowSupplier, setCursorWasNull);
    this.vector = vector;
  }

  @Override
  protected ArrowFlightJdbcAccessor createAccessorForVector(ValueVector vector) {
    return ArrowFlightJdbcAccessorFactory.createAccessor(
        vector, () -> this.vector.getOffset(this.getCurrentRow()), (boolean wasNull) -> {});
  }

  @Override
  protected byte getCurrentTypeId() {
    int index = getCurrentRow();
    return this.vector.getTypeId(index);
  }

  @Override
  protected ValueVector getVectorByTypeId(byte typeId) {
    return this.vector.getVectorByType(typeId);
  }
}
