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
package org.apache.arrow.driver.jdbc.converter.impl;

import java.util.List;
import org.apache.arrow.driver.jdbc.utils.AvaticaParameterBinder;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.calcite.avatica.AvaticaParameter;
import org.apache.calcite.avatica.remote.TypedValue;

/** AvaticaParameterConverter for FixedSizeList Arrow types. */
public class FixedSizeListAvaticaParameterConverter extends BaseAvaticaParameterConverter {

  public FixedSizeListAvaticaParameterConverter(ArrowType.FixedSizeList type) {}

  @Override
  public boolean bindParameter(FieldVector vector, TypedValue typedValue, int index) {
    final List<?> values = (List<?>) typedValue.value;
    final int arraySize = values.size();

    if (vector instanceof FixedSizeListVector) {
      FixedSizeListVector listVector = ((FixedSizeListVector) vector);
      FieldVector childVector = listVector.getDataVector();
      int maxArraySize = listVector.getListSize();

      if (arraySize != maxArraySize) {
        if (!childVector.getField().isNullable()) {
          throw new UnsupportedOperationException(
              "Each array must contain " + maxArraySize + " elements");
        } else if (arraySize > maxArraySize) {
          throw new UnsupportedOperationException(
              "Each array must contain at most " + maxArraySize + " elements");
        }
      }

      int startPos = listVector.startNewValue(index);
      for (int i = 0; i < arraySize; i++) {
        Object val = values.get(i);
        int childIndex = startPos + i;
        if (val == null) {
          if (childVector.getField().isNullable()) {
            childVector.setNull(childIndex);
          } else {
            throw new UnsupportedOperationException("Can't set null on non-nullable child list");
          }
        } else {
          childVector
              .getField()
              .getType()
              .accept(
                  new AvaticaParameterBinder.BinderVisitor(
                      childVector, TypedValue.ofSerial(typedValue.componentType, val), childIndex));
        }
      }
      listVector.setValueCount(index + 1);
      return true;
    }
    return false;
  }

  @Override
  public AvaticaParameter createParameter(Field field) {
    return createParameter(field, false);
  }
}
