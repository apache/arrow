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

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.LargeVarCharVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.Text;
import org.apache.calcite.avatica.AvaticaParameter;
import org.apache.calcite.avatica.remote.TypedValue;

/** AvaticaParameterConverter for LargeUtf8 Arrow types. */
public class LargeUtf8AvaticaParameterConverter extends BaseAvaticaParameterConverter {

  public LargeUtf8AvaticaParameterConverter(ArrowType.LargeUtf8 type) {}

  @Override
  public boolean bindParameter(FieldVector vector, TypedValue typedValue, int index) {
    String value = (String) typedValue.toLocal();
    if (vector instanceof LargeVarCharVector) {
      ((LargeVarCharVector) vector).setSafe(index, new Text(value));
      return true;
    }
    return false;
  }

  @Override
  public AvaticaParameter createParameter(Field field) {
    return createParameter(field, false);
  }
}
