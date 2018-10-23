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
import org.apache.arrow.vector.types.pojo.ArrowType.FixedSizeBinary;
import org.apache.arrow.vector.types.pojo.ArrowType.FixedSizeList;
import org.apache.arrow.vector.types.pojo.ArrowType.FloatingPoint;
import org.apache.arrow.vector.types.pojo.ArrowType.Int;
import org.apache.arrow.vector.types.pojo.ArrowType.Interval;
import org.apache.arrow.vector.types.pojo.ArrowType.Null;
import org.apache.arrow.vector.types.pojo.ArrowType.Struct;
import org.apache.arrow.vector.types.pojo.ArrowType.Time;
import org.apache.arrow.vector.types.pojo.ArrowType.Timestamp;
import org.apache.arrow.vector.types.pojo.ArrowType.Union;
import org.apache.arrow.vector.types.pojo.ArrowType.Utf8;

/**
 * The buffer layout of vectors for a given type
 * It defines its own buffers followed by the buffers for the children
 * if it is a nested type (Struct_, List, Union)
 */
public class TypeLayout {

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
                // TODO: validate this
                BufferLayout.validityVector(),
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
      public TypeLayout visit(FixedSizeList type) {
        List<BufferLayout> vectors = asList(
            BufferLayout.validityVector()
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
        return newFixedWidthTypeLayout(BufferLayout.dataBuffer(128));
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

      private TypeLayout newVariableWidthTypeLayout() {
        return newPrimitiveTypeLayout(BufferLayout.validityVector(), BufferLayout.offsetBuffer(),
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
          default:
            throw new UnsupportedOperationException("Unknown unit " + type.getUnit());
        }
      }

    });
    return layout;
  }

  private final List<BufferLayout> bufferLayouts;

  public TypeLayout(List<BufferLayout> bufferLayouts) {
    super();
    this.bufferLayouts = Preconditions.checkNotNull(bufferLayouts);
  }

  public TypeLayout(BufferLayout... bufferLayouts) {
    this(asList(bufferLayouts));
  }


  public List<BufferLayout> getBufferLayouts() {
    return bufferLayouts;
  }

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
