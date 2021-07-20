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

import java.util.Map;
import java.util.function.IntSupplier;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.impl.UnionMapReader;
import org.apache.arrow.vector.util.JsonStringHashMap;

public class ArrowFlightJdbcMapVectorAccessor extends AbstractArrowFlightJdbcListVectorAccessor {

  private final MapVector vector;

  public ArrowFlightJdbcMapVectorAccessor(MapVector vector, IntSupplier currentRowSupplier) {
    super(currentRowSupplier);
    this.vector = vector;
  }

  @Override
  public Class<?> getObjectClass() {
    return Map.class;
  }

  @Override
  public Object getObject() {
    int index = getCurrentRow();
    if (this.wasNull = vector.isNull(index)) {
      return null;
    }

    JsonStringHashMap<Object, Object> result = new JsonStringHashMap<>();
    UnionMapReader reader = vector.getReader();

    reader.setPosition(index);
    while (reader.next()) {
      Object key = reader.key().readObject();
      Object value = reader.value().readObject();

      result.put(key, value);
    }

    return result;
  }

  @Override
  protected long getStartOffset(int index) {
    return vector.getOffsetBuffer().getInt(index * 4L);
  }

  @Override
  protected long getEndOffset(int index) {
    return vector.getOffsetBuffer().getInt((index + 1) * 4L);
  }

  @Override
  protected FieldVector getDataVector() {
    return vector.getDataVector();
  }
}
