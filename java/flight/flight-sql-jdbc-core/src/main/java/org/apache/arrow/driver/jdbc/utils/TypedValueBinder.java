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

import java.math.BigDecimal;
import java.util.List;

import org.apache.arrow.driver.jdbc.client.ArrowFlightSqlClientHandler.PreparedStatement;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.LargeVarBinaryVector;
import org.apache.arrow.vector.LargeVarCharVector;
import org.apache.arrow.vector.NullVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.util.Text;
import org.apache.calcite.avatica.remote.TypedValue;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;

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
      bindValue(param, vector);
    }

    if (!typedValues.isEmpty()) {
      parameters.setRowCount(1);
      preparedStatement.setParameters(parameters);
    }
  }

  private void bindValue(TypedValue param, FieldVector vector) {
    Object value = param.value;
    try {
      if (vector instanceof IntVector) {
        ((IntVector) vector).setSafe(0, (int) value);
      } else if (vector instanceof BigIntVector) {
        BigIntVector longVec = (BigIntVector) vector;
        if (value instanceof Long) {
          longVec.setSafe(0, (long) value);
        } else {
          longVec.setSafe(0, (int) value);
        }
      } else if (vector instanceof BitVector) {
        ((BitVector) vector).setSafe(0, (int) value);
      } else if (vector instanceof Float4Vector) {
        ((Float4Vector) vector).setSafe(0, (float) value);
      } else if (vector instanceof Float8Vector) {
        ((Float8Vector) vector).setSafe(0, (double) value);
      } else if (vector instanceof DecimalVector) {
        ((DecimalVector) vector).setSafe(0, (BigDecimal) value);
      } else if (vector instanceof VarCharVector) {
        ((VarCharVector) vector).setSafe(0, new Text((String) value));
      } else if (vector instanceof LargeVarCharVector) {
        ((LargeVarCharVector) vector).setSafe(0, new Text((String) value));
      } else if (vector instanceof DateMilliVector) {
        String pattern = "yyyy-MM-dd HH:mm:ss.SSS";
        DateTime dateTime = DateTime.parse((String) value, DateTimeFormat.forPattern(pattern));

        ((DateMilliVector) vector).setSafe(0, dateTime.getMillis());
      } else if (vector instanceof VarBinaryVector) {
        ((VarBinaryVector) vector).setSafe(0, (byte[]) value);
      } else if (vector instanceof LargeVarBinaryVector) {
        ((LargeVarBinaryVector) vector).setSafe(0, (byte[]) value);
      } else if (vector instanceof NullVector) {
        assert true;
      } else {
        throw new UnsupportedOperationException(
                String.format("Binding to parameter of Arrow type %s", vector.getField().getType()));
      }
    } catch (ClassCastException e) {
      throw new RuntimeException(String.format("Value of type %s is not compatible with Arrow type %s",
              param.type, vector.getField().getType()));
    }
  }

  @Override
  public void close() {
    parameters.close();
  }
}
