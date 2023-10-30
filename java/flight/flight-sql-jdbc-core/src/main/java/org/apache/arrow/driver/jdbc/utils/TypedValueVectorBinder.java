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

import org.apache.arrow.driver.jdbc.converter.impl.BinaryAvaticaParameterConverter;
import org.apache.arrow.driver.jdbc.converter.impl.BoolAvaticaParameterConverter;
import org.apache.arrow.driver.jdbc.converter.impl.DateAvaticaParameterConverter;
import org.apache.arrow.driver.jdbc.converter.impl.DecimalAvaticaParameterConverter;
import org.apache.arrow.driver.jdbc.converter.impl.DurationAvaticaParameterConverter;
import org.apache.arrow.driver.jdbc.converter.impl.FixedSizeBinaryAvaticaParameterConverter;
import org.apache.arrow.driver.jdbc.converter.impl.FloatingPointAvaticaParameterConverter;
import org.apache.arrow.driver.jdbc.converter.impl.IntAvaticaParameterConverter;
import org.apache.arrow.driver.jdbc.converter.impl.IntervalAvaticaParameterConverter;
import org.apache.arrow.driver.jdbc.converter.impl.LargeBinaryAvaticaParameterConverter;
import org.apache.arrow.driver.jdbc.converter.impl.LargeUtf8AvaticaParameterConverter;
import org.apache.arrow.driver.jdbc.converter.impl.NullAvaticaParameterConverter;
import org.apache.arrow.driver.jdbc.converter.impl.TimeAvaticaParameterConverter;
import org.apache.arrow.driver.jdbc.converter.impl.TimestampAvaticaParameterConverter;
import org.apache.arrow.driver.jdbc.converter.impl.Utf8AvaticaParameterConverter;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.calcite.avatica.remote.TypedValue;

/**
 * Bind an Avatica TypedValue to a FieldVector using an ArrowTypeVisitor.
 */
public class TypedValueVectorBinder {
  /**
   * Bind a TypedValue to the given index on the FieldVctor.
   *
   * @param vector     FieldVector to bind to.
   * @param typedValue TypedValue to bind to the vector.
   * @param index      Vector index to bind the value at.
   */
  static void bind(FieldVector vector, TypedValue typedValue, int index) {
    try {
      if (!vector.getField().getType().accept(new BinderVisitor(vector, typedValue, index))) {
        throw new UnsupportedOperationException(
                String.format("Binding to vector type %s is not yet supported", vector.getClass()));
      }
    } catch (ClassCastException e) {
      throw new RuntimeException(String.format("Value of type %s is not compatible with Arrow type %s",
              typedValue.type, vector.getField().getType()));
    }
  }

  private static class BinderVisitor implements ArrowType.ArrowTypeVisitor<Boolean> {
    private final FieldVector vector;
    private final TypedValue typedValue;
    private final Object value;
    private final int index;

    private BinderVisitor(FieldVector vector, TypedValue value, int index) {
      this.vector = vector;
      this.typedValue = value;
      this.value = value.value;
      this.index = index;
    }

    @Override
    public Boolean visit(ArrowType.Null type) {
      return new NullAvaticaParameterConverter(type).setParameter(vector, typedValue, index);
    }

    @Override
    public Boolean visit(ArrowType.Struct type) {
      return false;
    }

    @Override
    public Boolean visit(ArrowType.List type) {
      return false;
    }

    @Override
    public Boolean visit(ArrowType.LargeList type) {
      return false;
    }

    @Override
    public Boolean visit(ArrowType.FixedSizeList type) {
      return false;
    }

    @Override
    public Boolean visit(ArrowType.Union type) {
      return false;
    }

    @Override
    public Boolean visit(ArrowType.Map type) {
      return false;
    }

    @Override
    public Boolean visit(ArrowType.Int type) {
      return new IntAvaticaParameterConverter(type).setParameter(vector, typedValue, index);
    }

    @Override
    public Boolean visit(ArrowType.FloatingPoint type) {
      return new FloatingPointAvaticaParameterConverter(type).setParameter(vector, typedValue, index);
    }

    @Override
    public Boolean visit(ArrowType.Utf8 type) {
      return new Utf8AvaticaParameterConverter(type).setParameter(vector, typedValue, index);
    }

    @Override
    public Boolean visit(ArrowType.LargeUtf8 type) {
      return new LargeUtf8AvaticaParameterConverter(type).setParameter(vector, typedValue, index);
    }

    @Override
    public Boolean visit(ArrowType.Binary type) {
      return new BinaryAvaticaParameterConverter(type).setParameter(vector, typedValue, index);
    }

    @Override
    public Boolean visit(ArrowType.LargeBinary type) {
      return new LargeBinaryAvaticaParameterConverter(type).setParameter(vector, typedValue, index);
    }

    @Override
    public Boolean visit(ArrowType.FixedSizeBinary type) {
      return new FixedSizeBinaryAvaticaParameterConverter(type).setParameter(vector, typedValue, index);
    }

    @Override
    public Boolean visit(ArrowType.Bool type) {
      return new BoolAvaticaParameterConverter(type).setParameter(vector, typedValue, index);
    }

    @Override
    public Boolean visit(ArrowType.Decimal type) {
      return new DecimalAvaticaParameterConverter(type).setParameter(vector, typedValue, index);
    }

    @Override
    public Boolean visit(ArrowType.Date type) {
      return new DateAvaticaParameterConverter(type).setParameter(vector, typedValue, index);
    }

    @Override
    public Boolean visit(ArrowType.Time type) {
      return new TimeAvaticaParameterConverter(type).setParameter(vector, typedValue, index);
    }

    @Override
    public Boolean visit(ArrowType.Timestamp type) {
      return new TimestampAvaticaParameterConverter(type).setParameter(vector, typedValue, index);
    }

    @Override
    public Boolean visit(ArrowType.Interval type) {
      return new IntervalAvaticaParameterConverter(type).setParameter(vector, typedValue, index);
    }

    @Override
    public Boolean visit(ArrowType.Duration type) {
      return new DurationAvaticaParameterConverter(type).setParameter(vector, typedValue, index);
    }
  }
}
