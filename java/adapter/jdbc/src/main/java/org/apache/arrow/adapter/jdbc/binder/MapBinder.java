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

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.LinkedHashMap;

import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.impl.UnionMapReader;
import org.apache.arrow.vector.util.JsonStringHashMap;

/**
 * A column binder for map of primitive values.
 */
public class MapBinder extends BaseColumnBinder<MapVector> {
  private final UnionMapReader reader;

  public MapBinder(MapVector vector) {
    this(vector, Types.VARCHAR);
  }

  public MapBinder(MapVector vector, int jdbcType) {
    super(vector, jdbcType);
    reader = vector.getReader();
  }

  @Override
  public void bind(PreparedStatement statement,
                   int parameterIndex, int rowIndex) throws SQLException {
    reader.setPosition(rowIndex);
    LinkedHashMap<Object, Object> tags = new JsonStringHashMap<>();
    while (reader.next()) {
      tags.put(reader.key().readObject(), reader.value().readObject());
    }
    switch (jdbcType) {
      case Types.VARCHAR:
        statement.setString(parameterIndex, tags.toString());
        break;
      case Types.OTHER:
      default:
        statement.setObject(parameterIndex, tags);
    }
  }
}
