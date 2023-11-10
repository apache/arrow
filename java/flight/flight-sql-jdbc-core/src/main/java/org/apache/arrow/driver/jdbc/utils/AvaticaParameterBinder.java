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

import java.util.List;

import org.apache.arrow.driver.jdbc.client.ArrowFlightSqlClientHandler.PreparedStatement;
import org.apache.arrow.driver.jdbc.converter.impl.BinaryAvaticaParameterConverter;
import org.apache.arrow.driver.jdbc.converter.impl.BoolAvaticaParameterConverter;
import org.apache.arrow.driver.jdbc.converter.impl.DateAvaticaParameterConverter;
import org.apache.arrow.driver.jdbc.converter.impl.DecimalAvaticaParameterConverter;
import org.apache.arrow.driver.jdbc.converter.impl.DurationAvaticaParameterConverter;
import org.apache.arrow.driver.jdbc.converter.impl.FixedSizeBinaryAvaticaParameterConverter;
import org.apache.arrow.driver.jdbc.converter.impl.FixedSizeListAvaticaParameterConverter;
import org.apache.arrow.driver.jdbc.converter.impl.FloatingPointAvaticaParameterConverter;
import org.apache.arrow.driver.jdbc.converter.impl.IntAvaticaParameterConverter;
import org.apache.arrow.driver.jdbc.converter.impl.IntervalAvaticaParameterConverter;
import org.apache.arrow.driver.jdbc.converter.impl.LargeBinaryAvaticaParameterConverter;
import org.apache.arrow.driver.jdbc.converter.impl.LargeListAvaticaParameterConverter;
import org.apache.arrow.driver.jdbc.converter.impl.LargeUtf8AvaticaParameterConverter;
import org.apache.arrow.driver.jdbc.converter.impl.ListAvaticaParameterConverter;
import org.apache.arrow.driver.jdbc.converter.impl.MapAvaticaParameterConverter;
import org.apache.arrow.driver.jdbc.converter.impl.NullAvaticaParameterConverter;
import org.apache.arrow.driver.jdbc.converter.impl.StructAvaticaParameterConverter;
import org.apache.arrow.driver.jdbc.converter.impl.TimeAvaticaParameterConverter;
import org.apache.arrow.driver.jdbc.converter.impl.TimestampAvaticaParameterConverter;
import org.apache.arrow.driver.jdbc.converter.impl.UnionAvaticaParameterConverter;
import org.apache.arrow.driver.jdbc.converter.impl.Utf8AvaticaParameterConverter;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.calcite.avatica.remote.TypedValue;

/**
 * Convert Avatica PreparedStatement parameters from a list of TypedValue to Arrow and bind them to the
 * VectorSchemaRoot representing the PreparedStatement parameters.
 * <p>
 * NOTE: Make sure to close the parameters VectorSchemaRoot once we're done with them.
 */
public class AvaticaParameterBinder {
  private final PreparedStatement preparedStatement;
  private final VectorSchemaRoot parameters;

  public AvaticaParameterBinder(PreparedStatement preparedStatement, BufferAllocator bufferAllocator) {
    this.parameters = VectorSchemaRoot.create(preparedStatement.getParameterSchema(), bufferAllocator);
    this.preparedStatement = preparedStatement;
  }

  /**
   * Bind the given Avatica values to the prepared statement.
   * @param typedValues The parameter values.
   */
  public void bind(List<TypedValue> typedValues) {
    bind(typedValues, 0);
  }

  /**
   * Bind the given Avatica values to the prepared statement at the given index.
   * @param typedValues The parameter values.
   * @param index index for parameter.
   */
  public void bind(List<TypedValue> typedValues, int index) {
    if (preparedStatement.getParameterSchema().getFields().size() != typedValues.size()) {
      throw new IllegalStateException(
          String.format("Prepared statement has %s parameters, but only received %s",
              preparedStatement.getParameterSchema().getFields().size(),
              typedValues.size()));
    }

    for (int i = 0; i < typedValues.size(); i++) {
      bind(parameters.getVector(i), typedValues.get(i), index);
    }

    if (!typedValues.isEmpty()) {
      parameters.setRowCount(index + 1);
      preparedStatement.setParameters(parameters);
    }
  }

  /**
   * Bind a TypedValue to the given index on the FieldVctor.
   *
   * @param vector     FieldVector to bind to.
   * @param typedValue TypedValue to bind to the vector.
   * @param index      Vector index to bind the value at.
   */
  private void bind(FieldVector vector, TypedValue typedValue, int index) {
    try {
      if (typedValue.value == null) {
        if (vector.getField().isNullable()) {
          vector.setNull(index);
        } else {
          throw new UnsupportedOperationException("Can't set null on non-nullable parameter");
        }
      } else if (!vector.getField().getType().accept(new BinderVisitor(vector, typedValue, index))) {
        throw new UnsupportedOperationException(
                String.format("Binding to vector type %s is not yet supported", vector.getClass()));
      }
    } catch (ClassCastException e) {
      throw new UnsupportedOperationException(
              String.format("Binding value of type %s is not yet supported for expected Arrow type %s",
                      typedValue.type, vector.getField().getType()));
    }
  }

  private static class BinderVisitor implements ArrowType.ArrowTypeVisitor<Boolean> {
    private final FieldVector vector;
    private final TypedValue typedValue;
    private final int index;

    private BinderVisitor(FieldVector vector, TypedValue value, int index) {
      this.vector = vector;
      this.typedValue = value;
      this.index = index;
    }

    @Override
    public Boolean visit(ArrowType.Null type) {
      return new NullAvaticaParameterConverter(type).bindParameter(vector, typedValue, index);
    }

    @Override
    public Boolean visit(ArrowType.Struct type) {
      return new StructAvaticaParameterConverter(type).bindParameter(vector, typedValue, index);
    }

    @Override
    public Boolean visit(ArrowType.List type) {
      return new ListAvaticaParameterConverter(type).bindParameter(vector, typedValue, index);
    }

    @Override
    public Boolean visit(ArrowType.LargeList type) {
      return new LargeListAvaticaParameterConverter(type).bindParameter(vector, typedValue, index);
    }

    @Override
    public Boolean visit(ArrowType.FixedSizeList type) {
      return new FixedSizeListAvaticaParameterConverter(type).bindParameter(vector, typedValue, index);
    }

    @Override
    public Boolean visit(ArrowType.Union type) {
      return new UnionAvaticaParameterConverter(type).bindParameter(vector, typedValue, index);
    }

    @Override
    public Boolean visit(ArrowType.Map type) {
      return new MapAvaticaParameterConverter(type).bindParameter(vector, typedValue, index);
    }

    @Override
    public Boolean visit(ArrowType.Int type) {
      return new IntAvaticaParameterConverter(type).bindParameter(vector, typedValue, index);
    }

    @Override
    public Boolean visit(ArrowType.FloatingPoint type) {
      return new FloatingPointAvaticaParameterConverter(type).bindParameter(vector, typedValue, index);
    }

    @Override
    public Boolean visit(ArrowType.Utf8 type) {
      return new Utf8AvaticaParameterConverter(type).bindParameter(vector, typedValue, index);
    }

    @Override
    public Boolean visit(ArrowType.LargeUtf8 type) {
      return new LargeUtf8AvaticaParameterConverter(type).bindParameter(vector, typedValue, index);
    }

    @Override
    public Boolean visit(ArrowType.Binary type) {
      return new BinaryAvaticaParameterConverter(type).bindParameter(vector, typedValue, index);
    }

    @Override
    public Boolean visit(ArrowType.LargeBinary type) {
      return new LargeBinaryAvaticaParameterConverter(type).bindParameter(vector, typedValue, index);
    }

    @Override
    public Boolean visit(ArrowType.FixedSizeBinary type) {
      return new FixedSizeBinaryAvaticaParameterConverter(type).bindParameter(vector, typedValue, index);
    }

    @Override
    public Boolean visit(ArrowType.Bool type) {
      return new BoolAvaticaParameterConverter(type).bindParameter(vector, typedValue, index);
    }

    @Override
    public Boolean visit(ArrowType.Decimal type) {
      return new DecimalAvaticaParameterConverter(type).bindParameter(vector, typedValue, index);
    }

    @Override
    public Boolean visit(ArrowType.Date type) {
      return new DateAvaticaParameterConverter(type).bindParameter(vector, typedValue, index);
    }

    @Override
    public Boolean visit(ArrowType.Time type) {
      return new TimeAvaticaParameterConverter(type).bindParameter(vector, typedValue, index);
    }

    @Override
    public Boolean visit(ArrowType.Timestamp type) {
      return new TimestampAvaticaParameterConverter(type).bindParameter(vector, typedValue, index);
    }

    @Override
    public Boolean visit(ArrowType.Interval type) {
      return new IntervalAvaticaParameterConverter(type).bindParameter(vector, typedValue, index);
    }

    @Override
    public Boolean visit(ArrowType.Duration type) {
      return new DurationAvaticaParameterConverter(type).bindParameter(vector, typedValue, index);
    }
  }

}
