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
import org.apache.arrow.vector.complex.UnionVector;
import org.apache.arrow.vector.types.Types;

/**
 * Accessor for the Arrow type {@link UnionVector}.
 */
public class ArrowFlightJdbcUnionVectorAccessor extends ArrowFlightJdbcAccessorWrapper {

  private final UnionVector vector;

  /**
   * Array of accessors for each type contained in UnionVector.
   * Index corresponds to UnionVector's typeIds, which are the ordinal values for {@link Types.MinorType}.
   */
  private final ArrowFlightJdbcAccessor[] accessors;
  private final ArrowFlightJdbcNullVectorAccessor nullAccessor = new ArrowFlightJdbcNullVectorAccessor();

  /**
   * Instantiate an accessor for a {@link UnionVector}.
   *
   * @param vector             an instance of a UnionVector.
   * @param currentRowSupplier the supplier to track the rows.
   */
  public ArrowFlightJdbcUnionVectorAccessor(UnionVector vector, IntSupplier currentRowSupplier) {
    super(currentRowSupplier);
    this.vector = vector;
    this.accessors = new ArrowFlightJdbcAccessor[128];
  }

  private ArrowFlightJdbcAccessor createAccessorForVector(ValueVector vector) {
    return ArrowFlightJdbcAccessorFactory.createAccessor(vector, this::getCurrentRow);
  }

  /**
   * Returns an accessor for UnionVector child vector on current row.
   *
   * @return ArrowFlightJdbcAccessor for child vector on current row.
   */
  protected ArrowFlightJdbcAccessor getAccessor() {
    int index = getCurrentRow();

    // Get the typeId and child vector for the current row being accessed.
    int typeId = this.vector.getTypeValue(index);
    ValueVector vector = this.vector.getVectorByType(typeId);

    if (typeId < 0) {
      // typeId may be negative if the current row has no type defined.
      return this.nullAccessor;
    }

    // Ensure there is an accessor for given typeId
    if (this.accessors[typeId] == null) {
      this.accessors[typeId] = this.createAccessorForVector(vector);
    }

    return this.accessors[typeId];
  }
}
