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

import org.apache.arrow.driver.jdbc.converter.AvaticaParameterConverter;
import org.apache.arrow.driver.jdbc.utils.SqlTypes;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.calcite.avatica.AvaticaParameter;
import org.apache.calcite.avatica.SqlType;

/**
 * Base AvaticaParameterConverter with a generic createParameter method that can be used by most
 * Arrow types.
 */
abstract class BaseAvaticaParameterConverter implements AvaticaParameterConverter {
  protected AvaticaParameter createParameter(Field field, boolean signed) {
    final String name = field.getName();
    final ArrowType arrowType = field.getType();
    final String typeName = arrowType.toString();
    final int precision = 0; // Would have to know about the actual number
    final int scale = 0; // According to https://www.postgresql.org/docs/current/datatype-numeric.html
    final int jdbcType = SqlTypes.getSqlTypeIdFromArrowType(arrowType);
    final String className = SqlType.valueOf(jdbcType).clazz.getCanonicalName();
    return new AvaticaParameter(signed, precision, scale, jdbcType, typeName, className, name);
  }
}
