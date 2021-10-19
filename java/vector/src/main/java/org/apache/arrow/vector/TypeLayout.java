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

package org.apache.arrow.vector;

import static java.util.Arrays.asList;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.BufferLayout.BufferType;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.ArrowType.ArrowTypeVisitor;
import org.apache.arrow.vector.types.pojo.ArrowType.Binary;
import org.apache.arrow.vector.types.pojo.ArrowType.Bool;
import org.apache.arrow.vector.types.pojo.ArrowType.Date;
import org.apache.arrow.vector.types.pojo.ArrowType.Decimal;
import org.apache.arrow.vector.types.pojo.ArrowType.Duration;
import org.apache.arrow.vector.types.pojo.ArrowType.FixedSizeBinary;
import org.apache.arrow.vector.types.pojo.ArrowType.FixedSizeList;
import org.apache.arrow.vector.types.pojo.ArrowType.FloatingPoint;
import org.apache.arrow.vector.types.pojo.ArrowType.Int;
import org.apache.arrow.vector.types.pojo.ArrowType.Interval;
import org.apache.arrow.vector.types.pojo.ArrowType.LargeBinary;
import org.apache.arrow.vector.types.pojo.ArrowType.LargeUtf8;
import org.apache.arrow.vector.types.pojo.ArrowType.Map;
import org.apache.arrow.vector.types.pojo.ArrowType.Null;
import org.apache.arrow.vector.types.pojo.ArrowType.Struct;
import org.apache.arrow.vector.types.pojo.ArrowType.Time;
import org.apache.arrow.vector.types.pojo.ArrowType.Timestamp;
import org.apache.arrow.vector.types.pojo.ArrowType.Union;
import org.apache.arrow.vector.types.pojo.ArrowType.Utf8;

/**
 * The buffer layout of vectors for a given type.
 * It defines its own buffers followed by the buffers for the children
 * if it is a nested type (Struct_, List, Union)
 */
public class TypeLayout {

  /**
   * Constructs a new {@TypeLayout} for the given <code>arrowType</code>.
   */
  public static TypeLayout getTypeLayout(final ArrowType arrowType) {
    TypeLayout layout = arrowType.accept(new ArrowTypeVisitor<TypeLayout>() {

      @Override
      public TypeLayout visit(Int type) {
        return newFixedWidthTypeLayout(BufferLayout.dataBuffer(type.getBitWidth()));
      }

      @Override
      public TypeLayout visit(Union type) {
        List<BufferLayout> vectors;
        switch (type.getMode()) {
          case Dense:
            vectors = asList(
                BufferLayout.typeBuffer(),
                BufferLayout.offsetBuffer() // offset to find the vector
            );
            break;
          case Sparse:
            vectors = asList(
                BufferLayout.typeBuffer() // type of the value at the index or 0 if null
            );
            break;
          default:
            throw new UnsupportedOperationException("Unsupported Union Mode: " + type.getMode());
        }
        return new TypeLayout(vectors);
      }

      @Override
      public TypeLayout visit(Struct type) {
        List<BufferLayout> vectors = asList(
            BufferLayout.validityVector()
        );
        return new TypeLayout(vectors);
      }

      @Override
      public TypeLayout visit(Timestamp type) {
        return newFixedWidthTypeLayout(BufferLayout.dataBuffer(64));
      }

      @Override
      public TypeLayout visit(org.apache.arrow.vector.types.pojo.ArrowType.List type) {
        List<BufferLayout> vectors = asList(
            BufferLayout.validityVector(),
            BufferLayout.offsetBuffer()
        );
        return new TypeLayout(vectors);
      }

      @Override
      public TypeLayout visit(ArrowType.LargeList type) {
        List<BufferLayout> vectors = asList(
            BufferLayout.validityVector(),
            BufferLayout.largeOffsetBuffer()
        );
        return new TypeLayout(vectors);
      }

      @Override
      public TypeLayout visit(FixedSizeList type) {
        List<BufferLayout> vectors = asList(
            BufferLayout.validityVector()
        );
        return new TypeLayout(vectors);
      }

      @Override
      public TypeLayout visit(Map type) {
        List<BufferLayout> vectors = asList(
            BufferLayout.validityVector(),
            BufferLayout.offsetBuffer()
        );
        return new TypeLayout(vectors);
      }

      @Override
      public TypeLayout visit(FloatingPoint type) {
        int bitWidth;
        switch (type.getPrecision()) {
          case HALF:
            bitWidth = 16;
            break;
          case SINGLE:
            bitWidth = 32;
            break;
          case DOUBLE:
            bitWidth = 64;
            break;
          default:
            throw new UnsupportedOperationException("Unsupported Precision: " + type.getPrecision());
        }
        return newFixedWidthTypeLayout(BufferLayout.dataBuffer(bitWidth));
      }

      @Override
      public TypeLayout visit(Decimal type) {
        return newFixedWidthTypeLayout(BufferLayout.dataBuffer(type.getBitWidth()));
      }

      @Override
      public TypeLayout visit(FixedSizeBinary type) {
        return newFixedWidthTypeLayout(new BufferLayout(BufferType.DATA, type.getByteWidth() * 8));
      }

      @Override
      public TypeLayout visit(Bool type) {
        return newFixedWidthTypeLayout(BufferLayout.booleanVector());
      }

      @Override
      public TypeLayout visit(Binary type) {
        return newVariableWidthTypeLayout();
      }

      @Override
      public TypeLayout visit(Utf8 type) {
        return newVariableWidthTypeLayout();
      }

      @Override
      public TypeLayout visit(LargeUtf8 type) {
        return newLargeVariableWidthTypeLayout();
      }

      @Override
      public TypeLayout visit(LargeBinary type) {
        return newLargeVariableWidthTypeLayout();
      }

      private TypeLayout newVariableWidthTypeLayout() {
        return newPrimitiveTypeLayout(BufferLayout.validityVector(), BufferLayout.offsetBuffer(),
          BufferLayout.byteVector());
      }

      private TypeLayout newLargeVariableWidthTypeLayout() {
        return newPrimitiveTypeLayout(BufferLayout.validityVector(), BufferLayout.largeOffsetBuffer(),
            BufferLayout.byteVector());
      }

      private TypeLayout newPrimitiveTypeLayout(BufferLayout... vectors) {
        return new TypeLayout(asList(vectors));
      }

      public TypeLayout newFixedWidthTypeLayout(BufferLayout dataVector) {
        return newPrimitiveTypeLayout(BufferLayout.validityVector(), dataVector);
      }

      @Override
      public TypeLayout visit(Null type) {
        return new TypeLayout(Collections.<BufferLayout>emptyList());
      }

      @Override
      public TypeLayout visit(Date type) {
        switch (type.getUnit()) {
          case DAY:
            return newFixedWidthTypeLayout(BufferLayout.dataBuffer(32));
          case MILLISECOND:
            return newFixedWidthTypeLayout(BufferLayout.dataBuffer(64));
          default:
            throw new UnsupportedOperationException("Unknown unit " + type.getUnit());
        }
      }

      @Override
      public TypeLayout visit(Time type) {
        return newFixedWidthTypeLayout(BufferLayout.dataBuffer(type.getBitWidth()));
      }

      @Override
      public TypeLayout visit(Interval type) {
        switch (type.getUnit()) {
          case DAY_TIME:
            return newFixedWidthTypeLayout(BufferLayout.dataBuffer(64));
          case YEAR_MONTH:
            return newFixedWidthTypeLayout(BufferLayout.dataBuffer(32));
          case MONTH_DAY_NANO:
            return newFixedWidthTypeLayout(BufferLayout.dataBuffer(128));
          default:
            throw new UnsupportedOperationException("Unknown unit " + type.getUnit());
        }
      }

      @Override
      public TypeLayout visit(Duration type) {
            return newFixedWidthTypeLayout(BufferLayout.dataBuffer(64));
      }

    });
    return layout;
  }

  /**
   * Gets the number of {@link BufferLayout}s for the given <code>arrowType</code>.
   */
  public static int getTypeBufferCount(final ArrowType arrowType) {
    return arrowType.accept(new ArrowTypeVisitor<Integer>() {

      /**
       * All fixed width vectors have a common number of buffers 2: one validity buffer, plus a data buffer.
       */
      static final int FIXED_WIDTH_BUFFER_COUNT = 2;

      /**
       * All variable width vectors have a common number of buffers 3: a validity buffer,
       * an offset buffer, and a data buffer.
       */
      static final int VARIABLE_WIDTH_BUFFER_COUNT = 3;

      @Override
      public Integer visit(Int type) {
        return FIXED_WIDTH_BUFFER_COUNT;
      }

      @Override
      public Integer visit(Union type) {
        switch (type.getMode()) {
          case Dense:
            // TODO: validate this
            return 2;
          case Sparse:
            // type buffer
            return 1;
          default:
            throw new UnsupportedOperationException("Unsupported Union Mode: " + type.getMode());
        }
      }

      @Override
      public Integer visit(Struct type) {
        // validity buffer
        return 1;
      }

      @Override
      public Integer visit(Timestamp type) {
        return FIXED_WIDTH_BUFFER_COUNT;
      }

      @Override
      public Integer visit(org.apache.arrow.vector.types.pojo.ArrowType.List type) {
        // validity buffer + offset buffer
        return 2;
      }

      @Override
      public Integer visit(ArrowType.LargeList type) {
        // validity buffer + offset buffer
        return 2;
      }

      @Override
      public Integer visit(FixedSizeList type) {
        // validity buffer
        return 1;
      }

      @Override
      public Integer visit(Map type) {
        // validity buffer + offset buffer
        return 2;
      }

      @Override
      public Integer visit(FloatingPoint type) {
        return FIXED_WIDTH_BUFFER_COUNT;
      }

      @Override
      public Integer visit(Decimal type) {
        return FIXED_WIDTH_BUFFER_COUNT;
      }

      @Override
      public Integer visit(FixedSizeBinary type) {
        return FIXED_WIDTH_BUFFER_COUNT;
      }

      @Override
      public Integer visit(Bool type) {
        return FIXED_WIDTH_BUFFER_COUNT;
      }

      @Override
      public Integer visit(Binary type) {
        return VARIABLE_WIDTH_BUFFER_COUNT;
      }

      @Override
      public Integer visit(Utf8 type) {
        return VARIABLE_WIDTH_BUFFER_COUNT;
      }

      @Override
      public Integer visit(LargeUtf8 type) {
        return VARIABLE_WIDTH_BUFFER_COUNT;
      }

      @Override
      public Integer visit(LargeBinary type) {
        return VARIABLE_WIDTH_BUFFER_COUNT;
      }

      @Override
      public Integer visit(Null type) {
        return 0;
      }

      @Override
      public Integer visit(Date type) {
        return FIXED_WIDTH_BUFFER_COUNT;
      }

      @Override
      public Integer visit(Time type) {
        return FIXED_WIDTH_BUFFER_COUNT;
      }

      @Override
      public Integer visit(Interval type) {
        return FIXED_WIDTH_BUFFER_COUNT;
      }

      @Override
      public Integer visit(Duration type) {
        return FIXED_WIDTH_BUFFER_COUNT;
      }

    });
  }

  private final List<BufferLayout> bufferLayouts;

  public TypeLayout(List<BufferLayout> bufferLayouts) {
    super();
    this.bufferLayouts = Preconditions.checkNotNull(bufferLayouts);
  }

  public TypeLayout(BufferLayout... bufferLayouts) {
    this(asList(bufferLayouts));
  }

  /**
   * Returns the individual {@linkplain BufferLayout}s for the given type.
   */
  public List<BufferLayout> getBufferLayouts() {
    return bufferLayouts;
  }

  /**
   * Returns the types of each buffer for this layout.  A layout can consist
   * of multiple buffers for example a validity bitmap buffer, a value buffer or
   * an offset buffer.
   */
  public List<BufferType> getBufferTypes() {
    List<BufferType> types = new ArrayList<>(bufferLayouts.size());
    for (BufferLayout vector : bufferLayouts) {
      types.add(vector.getType());
    }
    return types;
  }

  public String toString() {
    return bufferLayouts.toString();
  }

  @Override
  public int hashCode() {
    return bufferLayouts.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    TypeLayout other = (TypeLayout) obj;
    return bufferLayouts.equals(other.bufferLayouts);
  }

}
