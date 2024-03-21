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

package org.apache.arrow.driver.jdbc.converter;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.calcite.avatica.AvaticaParameter;
import org.apache.calcite.avatica.remote.TypedValue;

/**
 * Interface for a class in charge of converting between AvaticaParameters and TypedValues and
 * Arrow.
 */
public interface AvaticaParameterConverter {

  /**
   * Bind a TypedValue to a FieldVector at the given index.
   *
   * @param vector FieldVector that the parameter should be bound to.
   * @param typedValue TypedValue to bind as a parameter.
   * @param index Vector index (0-indexed) that the TypedValue should be bound to.
   * @return Whether the value was set successfully.
   */
  boolean bindParameter(FieldVector vector, TypedValue typedValue, int index);

  /**
   * Create an AvaticaParameter from the given Field.
   *
   * @param field Arrow Field to generate an AvaticaParameter from.
   */
  AvaticaParameter createParameter(Field field);
}
