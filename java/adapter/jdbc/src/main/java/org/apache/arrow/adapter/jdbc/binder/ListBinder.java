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

import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.impl.UnionListReader;

/**
 * A column binder for list of primitive values.
 */
public class ListBinder extends BaseColumnBinder<ListVector> {

  private UnionListReader listReader;

  public ListBinder(ListVector vector) {
    this(vector, java.sql.Types.ARRAY);
  }

  public ListBinder(ListVector vector, int jdbcType) {
    super(vector, jdbcType);
    listReader = vector.getReader();
  }

  @Override
  public void bind(java.sql.PreparedStatement statement, int parameterIndex, int rowIndex)throws java.sql.SQLException {
    listReader.setPosition(rowIndex);
    ArrayList<?> sourceArray = (ArrayList<?>) listReader.readObject();
    Class<?> arrayElementClass = sourceArray.get(0).getClass();
    Object array = Array.newInstance(arrayElementClass, sourceArray.size());
    Arrays.setAll((Object[]) array, sourceArray::get);
    statement.setObject(parameterIndex, array);
  }
}
