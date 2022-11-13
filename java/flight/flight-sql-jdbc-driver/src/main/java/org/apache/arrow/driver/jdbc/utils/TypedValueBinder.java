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

package org.apache.arrow.driver.jdbc.utils;

import java.nio.charset.StandardCharsets;
import java.util.List;

import org.apache.arrow.driver.jdbc.client.ArrowFlightSqlClientHandler.PreparedStatement;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.calcite.avatica.remote.TypedValue;

/**
 * Bind {@link TypedValue}s to a {@link VectorSchemaRoot}.
 */
public class TypedValueBinder implements AutoCloseable {
  private final PreparedStatement preparedStatement;
  private final VectorSchemaRoot parameters;

  public TypedValueBinder(PreparedStatement preparedStatement, BufferAllocator bufferAllocator) {
    this.parameters = VectorSchemaRoot.create(preparedStatement.getParameterSchema(), bufferAllocator);
    this.preparedStatement = preparedStatement;
  }

  /**
   * Bind the given Avatica values to the prepared statement.
   * @param typedValues The parameter values.
   */
  public void bind(List<TypedValue> typedValues) {
    if (preparedStatement.getParameterSchema().getFields().size() != typedValues.size()) {
      throw new IllegalStateException(
          String.format("Prepared statement has %s parameters, but only received %s",
              preparedStatement.getParameterSchema().getFields().size(),
              typedValues.size()));
    }

    for (int i = 0; i < typedValues.size(); i++) {
      final TypedValue param = typedValues.get(i);
      final FieldVector vector = parameters.getVector(i);
      switch (param.type) {
        case STRING:
          bindValue((String) param.value, vector);
          break;
        default:
          throw new UnsupportedOperationException(
              String.format("Binding JDBC type %s to Arrow Flight SQL statement", param.type));
      }
    }

    if (!typedValues.isEmpty()) {
      parameters.setRowCount(1);
      preparedStatement.setParameters(parameters);
    }
  }

  private void bindValue(String value, FieldVector vector) {
    if (vector instanceof VarCharVector) {
      ((VarCharVector) vector).setSafe(0, value.getBytes(StandardCharsets.UTF_8));
    } else {
      throw new UnsupportedOperationException(
          String.format("Binding String to parameter of Arrow type %s", vector.getField().getType()));
    }
  }

  @Override
  public void close() {
    parameters.close();
  }
}
