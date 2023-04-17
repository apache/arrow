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

import org.apache.arrow.driver.jdbc.ArrowFlightJdbcArray;
import org.apache.arrow.driver.jdbc.accessor.ArrowFlightJdbcAccessor;
import org.apache.arrow.driver.jdbc.accessor.ArrowFlightJdbcAccessorFactory;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.LargeListVector;
import org.apache.arrow.vector.complex.ListVector;

/**
 * Base Accessor for the Arrow types {@link ListVector}, {@link LargeListVector} and {@link FixedSizeListVector}.
 */
public abstract class AbstractArrowFlightJdbcListVectorAccessor extends ArrowFlightJdbcAccessor {

  protected AbstractArrowFlightJdbcListVectorAccessor(IntSupplier currentRowSupplier,
                                                      ArrowFlightJdbcAccessorFactory.WasNullConsumer setCursorWasNull) {
    super(currentRowSupplier, setCursorWasNull);
  }

  @Override
  public Class<?> getObjectClass() {
    return List.class;
  }

  protected abstract long getStartOffset(int index);

  protected abstract long getEndOffset(int index);

  protected abstract FieldVector getDataVector();

  protected abstract boolean isNull(int index);

  @Override
  public final Array getArray() {
    int index = getCurrentRow();
    FieldVector dataVector = getDataVector();

    this.wasNull = isNull(index);
    this.wasNullConsumer.setWasNull(this.wasNull);
    if (this.wasNull) {
      return null;
    }

    long startOffset = getStartOffset(index);
    long endOffset = getEndOffset(index);

    long valuesCount = endOffset - startOffset;
    return new ArrowFlightJdbcArray(dataVector, startOffset, valuesCount);
  }
}

