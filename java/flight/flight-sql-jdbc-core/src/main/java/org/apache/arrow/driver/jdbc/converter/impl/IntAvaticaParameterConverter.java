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

import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.UInt1Vector;
import org.apache.arrow.vector.UInt2Vector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.calcite.avatica.AvaticaParameter;
import org.apache.calcite.avatica.remote.TypedValue;

/**
 * AvaticaParameterConverter for Int Arrow types.
 */
public class IntAvaticaParameterConverter extends BaseAvaticaParameterConverter {
  private final ArrowType.Int type;

  public IntAvaticaParameterConverter(ArrowType.Int type) {
    this.type = type;
  }

  @Override
  public boolean bindParameter(FieldVector vector, TypedValue typedValue, int index) {
    Number value = (Number) typedValue.value;
    if (vector instanceof TinyIntVector) {
      ((TinyIntVector) vector).setSafe(index, value.intValue());
      return true;
    } else if (vector instanceof SmallIntVector) {
      ((SmallIntVector) vector).setSafe(index, value.intValue());
      return true;
    } else if (vector instanceof IntVector) {
      ((IntVector) vector).setSafe(index, value.intValue());
      return true;
    } else if (vector instanceof BigIntVector) {
      ((BigIntVector) vector).setSafe(index, value.longValue());
      return true;
    } else if (vector instanceof UInt1Vector) {
      ((UInt1Vector) vector).setSafe(index, value.intValue());
      return true;
    } else if (vector instanceof UInt2Vector) {
      ((UInt2Vector) vector).setSafe(index, value.intValue());
      return true;
    } else if (vector instanceof UInt4Vector) {
      ((UInt4Vector) vector).setSafe(index, value.intValue());
      return true;
    } else if (vector instanceof UInt8Vector) {
      ((UInt8Vector) vector).setSafe(index, value.longValue());
      return true;
    }
    return false;
  }

  @Override
  public AvaticaParameter createParameter(Field field) {
    return createParameter(field, type.getIsSigned());
  }
}
