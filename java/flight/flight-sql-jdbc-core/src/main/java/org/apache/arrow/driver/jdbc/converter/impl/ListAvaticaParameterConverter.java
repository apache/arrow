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
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.calcite.avatica.AvaticaParameter;
import org.apache.calcite.avatica.remote.TypedValue;

/**
 * AvaticaParameterConverter for List Arrow types.
 */
public class ListAvaticaParameterConverter extends BaseAvaticaParameterConverter {

  public ListAvaticaParameterConverter(ArrowType.List type) {
  }

  @Override
  public boolean bindParameter(FieldVector vector, TypedValue typedValue, int index) {
    final List<?> values = (List<?>) typedValue.value;

    if (vector instanceof ListVector) {
      ListVector listVector = ((ListVector) vector);
      FieldVector childVector = listVector.getDataVector();

      int startPos = listVector.startNewValue(index);
      for (int i = 0; i < values.size(); i++) {
        Object val = values.get(i);
        int childIndex = startPos + i;
        if (val == null) {
          if (childVector.getField().isNullable()) {
            childVector.setNull(childIndex);
          } else {
            throw new UnsupportedOperationException("Can't set null on non-nullable child list");
          }
        } else {
          childVector.getField().getType().accept(
                  new AvaticaParameterBinder.BinderVisitor(
                          childVector, TypedValue.ofSerial(typedValue.componentType, val), childIndex));
        }
      }
      listVector.endValue(index, values.size());
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
