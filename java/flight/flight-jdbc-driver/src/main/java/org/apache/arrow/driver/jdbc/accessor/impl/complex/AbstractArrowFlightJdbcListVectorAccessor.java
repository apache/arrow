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

import java.sql.Array;
import java.util.List;
import java.util.function.IntSupplier;

import org.apache.arrow.driver.jdbc.accessor.ArrowFlightJdbcAccessor;
import org.apache.arrow.vector.FieldVector;

public abstract class AbstractArrowFlightJdbcListVectorAccessor extends ArrowFlightJdbcAccessor {

  protected AbstractArrowFlightJdbcListVectorAccessor(IntSupplier currentRowSupplier) {
    super(currentRowSupplier);
  }

  @Override
  public Class<?> getObjectClass() {
    return List.class;
  }

  protected abstract long getStart(int index);

  protected abstract long getEnd(int index);

  protected abstract FieldVector getDataVector();

  @Override
  public final Array getArray() {
    int index = getCurrentRow();
    FieldVector dataVector = getDataVector();
    if (this.wasNull = dataVector.isNull(index)) {
      return null;
    }

    long start = getStart(index);
    long end = getEnd(index);

    long count = end - start;
    return new ArrowFlightJdbcArray(dataVector, start, count);
  }
}

