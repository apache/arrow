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
import org.apache.arrow.driver.jdbc.accessor.ArrowFlightJdbcAccessorWrapper;
import org.apache.arrow.driver.jdbc.accessor.impl.ArrowFlightJdbcNullVectorAccessor;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.complex.DenseUnionVector;

/**
 * Accessor for the Arrow type {@link DenseUnionVector}.
 */
public class ArrowFlightJdbcDenseUnionVectorAccessor extends ArrowFlightJdbcAccessorWrapper {

  private final DenseUnionVector vector;
  private final ArrowFlightJdbcAccessor[] accessors;
  private final ArrowFlightJdbcNullVectorAccessor nullAccessor = new ArrowFlightJdbcNullVectorAccessor();

  public ArrowFlightJdbcDenseUnionVectorAccessor(DenseUnionVector vector, IntSupplier currentRowSupplier) {
    super(currentRowSupplier);
    this.vector = vector;
    this.accessors = new ArrowFlightJdbcAccessor[128];
  }

  private ArrowFlightJdbcAccessor createAccessorForVector(ValueVector vector) {
    return ArrowFlightJdbcAccessorFactory.createAccessor(vector, () -> this.vector.getOffset(this.getCurrentRow()));
  }

  protected ArrowFlightJdbcAccessor getAccessor() {
    int index = getCurrentRow();
    byte typeId = this.vector.getTypeId(index);
    ValueVector vector = this.vector.getVectorByType(typeId);
    if (typeId < 0) {
      return this.nullAccessor;
    }
    if (this.accessors[typeId] == null) {
      this.accessors[typeId] = this.createAccessorForVector(vector);
    }

    return this.accessors[typeId];
  }
}
