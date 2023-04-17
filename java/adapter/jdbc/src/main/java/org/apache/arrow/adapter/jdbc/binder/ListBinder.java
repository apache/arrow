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

package org.apache.arrow.adapter.jdbc.binder;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.impl.UnionListReader;
import org.apache.arrow.vector.util.Text;

/**
 * A column binder for list of primitive values.
 */
public class ListBinder extends BaseColumnBinder<ListVector> {

  private final UnionListReader listReader;
  private final Class<?> arrayElementClass;
  private final boolean isTextColumn;

  public ListBinder(ListVector vector) {
    this(vector, java.sql.Types.ARRAY);
  }

  /**
   * Init ListBinder and determine type of data vector.
   *
   * @param vector corresponding data vector from arrow buffer for binding
   * @param jdbcType parameter jdbc type
   */
  public ListBinder(ListVector vector, int jdbcType) {
    super(vector, jdbcType);
    listReader = vector.getReader();
    Class<? extends FieldVector> dataVectorClass = vector.getDataVector().getClass();
    try {
      arrayElementClass = dataVectorClass.getMethod("getObject", Integer.TYPE).getReturnType();
    } catch (NoSuchMethodException e) {
      final String message = String.format("Issue to determine type for getObject method of data vector class %s ",
              dataVectorClass.getName());
      throw new RuntimeException(message);
    }
    isTextColumn = arrayElementClass.isAssignableFrom(Text.class);
  }

  @Override
  public void bind(java.sql.PreparedStatement statement, int parameterIndex, int rowIndex)throws java.sql.SQLException {
    listReader.setPosition(rowIndex);
    ArrayList<?> sourceArray = (ArrayList<?>) listReader.readObject();
    Object array;
    if (!isTextColumn) {
      array = Array.newInstance(arrayElementClass, sourceArray.size());
      Arrays.setAll((Object[]) array, sourceArray::get);
    } else {
      array = new String[sourceArray.size()];
      Arrays.setAll((Object[]) array, idx -> sourceArray.get(idx) != null ? sourceArray.get(idx).toString() : null);
    }
    statement.setObject(parameterIndex, array);
  }
}
