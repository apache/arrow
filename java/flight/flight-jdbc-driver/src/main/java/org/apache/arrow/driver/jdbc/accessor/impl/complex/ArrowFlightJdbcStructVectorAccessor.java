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

import java.sql.Struct;
import java.util.List;
import java.util.Map;
import java.util.function.IntSupplier;
import java.util.stream.Collectors;

import org.apache.arrow.driver.jdbc.accessor.ArrowFlightJdbcAccessor;
import org.apache.arrow.driver.jdbc.accessor.ArrowFlightJdbcAccessorFactory;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.calcite.avatica.util.StructImpl;

/**
 * Accessor for the Arrow type {@link StructVector}.
 */
public class ArrowFlightJdbcStructVectorAccessor extends ArrowFlightJdbcAccessor {

  private final StructVector vector;

  public ArrowFlightJdbcStructVectorAccessor(StructVector vector, IntSupplier currentRowSupplier,
                                             ArrowFlightJdbcAccessorFactory.WasNullConsumer setCursorWasNull) {
    super(currentRowSupplier, setCursorWasNull);
    this.vector = vector;
  }

  @Override
  public Class<?> getObjectClass() {
    return Map.class;
  }

  @Override
  public Object getObject() {
    Map<String, ?> object = vector.getObject(getCurrentRow());
    this.wasNull = object == null;
    this.wasNullConsumer.setWasNull(this.wasNull);

    return object;
  }

  @Override
  public Struct getStruct() {
    int currentRow = getCurrentRow();

    this.wasNull = vector.isNull(currentRow);
    this.wasNullConsumer.setWasNull(this.wasNull);
    if (this.wasNull) {
      return null;
    }

    List<Object> attributes = vector.getChildrenFromFields()
        .stream()
        .map(vector -> vector.getObject(currentRow))
        .collect(Collectors.toList());

    return new StructImpl(attributes);
  }
}
