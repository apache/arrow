/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.arrow.vector;

import java.util.Iterator;
import java.util.List;

import org.apache.arrow.flatbuf.Precision;
import org.apache.arrow.schema.ArrowFieldNode;
import org.apache.arrow.vector.complex.ComplexVectorLoader;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.NestedVector;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.ArrowType.Binary;
import org.apache.arrow.vector.types.pojo.ArrowType.Bool;
import org.apache.arrow.vector.types.pojo.ArrowType.Date;
import org.apache.arrow.vector.types.pojo.ArrowType.Decimal;
import org.apache.arrow.vector.types.pojo.ArrowType.FloatingPoint;
import org.apache.arrow.vector.types.pojo.ArrowType.Int;
import org.apache.arrow.vector.types.pojo.ArrowType.IntervalDay;
import org.apache.arrow.vector.types.pojo.ArrowType.IntervalYear;
import org.apache.arrow.vector.types.pojo.ArrowType.Null;
import org.apache.arrow.vector.types.pojo.ArrowType.Time;
import org.apache.arrow.vector.types.pojo.ArrowType.Timestamp;
import org.apache.arrow.vector.types.pojo.ArrowType.Tuple;
import org.apache.arrow.vector.types.pojo.ArrowType.Union;
import org.apache.arrow.vector.types.pojo.ArrowType.Utf8;
import org.apache.arrow.vector.types.pojo.Field;

import io.netty.buffer.ArrowBuf;

public class VectorLoader {

  public static void addChild(final NestedVector container, final Field field, final Iterator<ArrowFieldNode> nodes, final Iterator<ArrowBuf> buffers) {
    MinorType minorType = Types.getMinorTypeForArrowType(field.getType());
    ValueVector vector = container.add(field.getName(), minorType);
    loadVector(vector, field, nodes, buffers);
    List<Field> children = field.getChildren();
    for (Field child : children) {
      addChild((NestedVector)vector, child, nodes, buffers);
    }
  }

  public static void loadVector(final ValueVector vector, Field field, Iterator<ArrowFieldNode> nodes, final Iterator<ArrowBuf> buffers) {
    final ArrowFieldNode node = nodes.next();
    field.getType().accept(new ArrowType.ArrowTypeVisitor<Void>() {
      @Override
      public Void visit(Null type) {
        return null;
      }

      @Override
      public Void visit(Tuple type) {
        MapVector mapVector = (MapVector)vector;
        ComplexVectorLoader.load(mapVector, node, buffers);
        return null;
      }

      @Override
      public Void visit(org.apache.arrow.vector.types.pojo.ArrowType.List type) {
        ListVector listVector = (ListVector)vector;
        ComplexVectorLoader.load(listVector, node, buffers);
        return null;
      }

      @Override
      public Void visit(Union type) {
        throw new UnsupportedOperationException("NYI");
      }

      @Override
      public Void visit(Int type) {
        switch (type.getBitWidth()) {
        case 8:
          if (type.getIsSigned()) {
            NullableTinyIntVector intVector = (NullableTinyIntVector)vector;
            intVector.bits.data = buffers.next();
            intVector.values.data = buffers.next();
          } else {
            NullableUInt1Vector intVector = (NullableUInt1Vector)vector;
            intVector.bits.data = buffers.next();
            intVector.values.data = buffers.next();
          }
          break;
        case 16:
          if (type.getIsSigned()) {
            NullableSmallIntVector intVector = (NullableSmallIntVector)vector;
            intVector.bits.data = buffers.next();
            intVector.values.data = buffers.next();
          } else {
            NullableUInt2Vector intVector = (NullableUInt2Vector)vector;
            intVector.bits.data = buffers.next();
            intVector.values.data = buffers.next();
          }
          break;
        case 32:
          if (type.getIsSigned()) {
            NullableIntVector intVector = (NullableIntVector)vector;
            intVector.bits.data = buffers.next();
            intVector.values.data = buffers.next();
          } else {
            NullableUInt4Vector intVector = (NullableUInt4Vector)vector;
            intVector.bits.data = buffers.next();
            intVector.values.data = buffers.next();
          }
          break;
        case 64:
          if (type.getIsSigned()) {
            NullableBigIntVector intVector = (NullableBigIntVector)vector;
            intVector.bits.data = buffers.next();
            intVector.values.data = buffers.next();
          } else {
            NullableUInt8Vector intVector = (NullableUInt8Vector)vector;
            intVector.bits.data = buffers.next();
            intVector.values.data = buffers.next();
          }
          break;
        default:
          throw new IllegalArgumentException("Illegal bit width: " + type.getBitWidth());
        }
        // TODO: the vector has an unused data field?
        return null;
      }

      @Override
      public Void visit(FloatingPoint type) {
        switch (type.getPrecision()) {
        case Precision.SINGLE:
          NullableFloat4Vector fVector = (NullableFloat4Vector)vector;
          fVector.bits.data = buffers.next();
          fVector.values.data = buffers.next();
          break;
        case Precision.DOUBLE:
          NullableFloat8Vector dVector = (NullableFloat8Vector)vector;
          dVector.bits.data = buffers.next();
          dVector.values.data = buffers.next();
          break;
        default:
          throw new IllegalArgumentException("unknown precision: " + type.getPrecision());
        }
        // TODO: the vector has an unused data field?
        return null;
      }

      @Override
      public Void visit(Utf8 type) {
        NullableVarCharVector stringVector = (NullableVarCharVector)vector;
        stringVector.bits.data = buffers.next();
        stringVector.values.offsetVector.data = buffers.next();
        stringVector.values.data = buffers.next();
        // TODO: the vector has an unused data field?
        return null;
      }

      @Override
      public Void visit(Binary type) {
        NullableVarBinaryVector bVector = (NullableVarBinaryVector)vector;
        bVector.bits.data = buffers.next();
        bVector.values.offsetVector.data = buffers.next();
        bVector.values.data = buffers.next();
        // TODO: the vector has an unused data field?
        return null;
      }

      @Override
      public Void visit(Bool type) {
        NullableBitVector bVector = (NullableBitVector)vector;
        bVector.bits.data = buffers.next();
        bVector.values.data = buffers.next();
        // TODO: the vector has an unused data field?
        return null;
      }

      @Override
      public Void visit(Decimal type) {
        throw new UnsupportedOperationException("NYI");
      }

      @Override
      public Void visit(Date type) {
        throw new UnsupportedOperationException("NYI");
      }

      @Override
      public Void visit(Time type) {
        throw new UnsupportedOperationException("NYI");
      }

      @Override
      public Void visit(Timestamp type) {
        throw new UnsupportedOperationException("NYI");
      }

      @Override
      public Void visit(IntervalDay type) {
        throw new UnsupportedOperationException("NYI");
      }

      @Override
      public Void visit(IntervalYear type) {
        throw new UnsupportedOperationException("NYI");
      }
    });

  }

  public static void load(BaseDataValueVector vector, ArrowBuf buffer) {
    vector.data = buffer;
  }

}
