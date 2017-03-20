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
package org.apache.arrow.vector.schema;

import static java.util.Arrays.asList;
import static org.apache.arrow.vector.schema.VectorLayout.booleanVector;
import static org.apache.arrow.vector.schema.VectorLayout.byteVector;
import static org.apache.arrow.vector.schema.VectorLayout.dataVector;
import static org.apache.arrow.vector.schema.VectorLayout.offsetVector;
import static org.apache.arrow.vector.schema.VectorLayout.typeVector;
import static org.apache.arrow.vector.schema.VectorLayout.validityVector;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.ArrowType.ArrowTypeVisitor;
import org.apache.arrow.vector.types.pojo.ArrowType.Binary;
import org.apache.arrow.vector.types.pojo.ArrowType.Bool;
import org.apache.arrow.vector.types.pojo.ArrowType.Date;
import org.apache.arrow.vector.types.pojo.ArrowType.Decimal;
import org.apache.arrow.vector.types.pojo.ArrowType.FloatingPoint;
import org.apache.arrow.vector.types.pojo.ArrowType.Int;
import org.apache.arrow.vector.types.pojo.ArrowType.Interval;
import org.apache.arrow.vector.types.pojo.ArrowType.Null;
import org.apache.arrow.vector.types.pojo.ArrowType.Struct;
import org.apache.arrow.vector.types.pojo.ArrowType.Time;
import org.apache.arrow.vector.types.pojo.ArrowType.Timestamp;
import org.apache.arrow.vector.types.pojo.ArrowType.Union;
import org.apache.arrow.vector.types.pojo.ArrowType.Utf8;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

/**
 * The layout of vectors for a given type
 * It defines its own vectors followed by the vectors for the children
 * if it is a nested type (Struct_, List, Union)
 */
public class TypeLayout {

  public static TypeLayout getTypeLayout(final ArrowType arrowType) {
    TypeLayout layout = arrowType.accept(new ArrowTypeVisitor<TypeLayout>() {

      @Override public TypeLayout visit(Int type) {
        return newFixedWidthTypeLayout(dataVector(type.getBitWidth()));
      }

      @Override public TypeLayout visit(Union type) {
        List<VectorLayout> vectors;
        switch (type.getMode()) {
          case Dense:
            vectors = asList(
                // TODO: validate this
                validityVector(),
                typeVector(),
                offsetVector() // offset to find the vector
                );
            break;
          case Sparse:
            vectors = asList(
                typeVector() // type of the value at the index or 0 if null
                );
            break;
          default:
            throw new UnsupportedOperationException("Unsupported Union Mode: " + type.getMode());
        }
        return new TypeLayout(vectors);
      }

      @Override public TypeLayout visit(Struct type) {
        List<VectorLayout> vectors = asList(
            validityVector()
            );
        return new TypeLayout(vectors);
      }

      @Override public TypeLayout visit(Timestamp type) {
        return newFixedWidthTypeLayout(dataVector(64));
      }

      @Override public TypeLayout visit(org.apache.arrow.vector.types.pojo.ArrowType.List type) {
        List<VectorLayout> vectors = asList(
            validityVector(),
            offsetVector()
            );
        return new TypeLayout(vectors);
      }

      @Override public TypeLayout visit(FloatingPoint type) {
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
        return newFixedWidthTypeLayout(dataVector(bitWidth));
      }

      @Override public TypeLayout visit(Decimal type) {
        // TODO: check size
        return newFixedWidthTypeLayout(dataVector(64)); // actually depends on the type fields
      }

      @Override public TypeLayout visit(Bool type) {
        return newFixedWidthTypeLayout(booleanVector());
      }

      @Override public TypeLayout visit(Binary type) {
        return newVariableWidthTypeLayout();
      }

      @Override public TypeLayout visit(Utf8 type) {
        return newVariableWidthTypeLayout();
      }

      private TypeLayout newVariableWidthTypeLayout() {
        return newPrimitiveTypeLayout(validityVector(), offsetVector(), byteVector());
      }

      private TypeLayout newPrimitiveTypeLayout(VectorLayout... vectors) {
        return new TypeLayout(asList(vectors));
      }

      public TypeLayout newFixedWidthTypeLayout(VectorLayout dataVector) {
        return newPrimitiveTypeLayout(validityVector(), dataVector);
      }

      @Override
      public TypeLayout visit(Null type) {
        return new TypeLayout(Collections.<VectorLayout>emptyList());
      }

      @Override
      public TypeLayout visit(Date type) {
        return newFixedWidthTypeLayout(dataVector(64));
      }

      @Override
      public TypeLayout visit(Time type) {
        return newFixedWidthTypeLayout(dataVector(type.getBitWidth()));
      }

      @Override
      public TypeLayout visit(Interval type) { // TODO: check size
        switch (type.getUnit()) {
        case DAY_TIME:
          return newFixedWidthTypeLayout(dataVector(64));
        case YEAR_MONTH:
          return newFixedWidthTypeLayout(dataVector(64));
        default:
          throw new UnsupportedOperationException("Unknown unit " + type.getUnit());
        }
      }

    });
    return layout;
  }

  private final List<VectorLayout> vectors;

  @JsonCreator
  public TypeLayout(@JsonProperty("vectors") List<VectorLayout> vectors) {
    super();
    this.vectors = Preconditions.checkNotNull(vectors);
  }

  public TypeLayout(VectorLayout... vectors) {
    this(asList(vectors));
  }


  public List<VectorLayout> getVectors() {
    return vectors;
  }

  @JsonIgnore
  public List<ArrowVectorType> getVectorTypes() {
    List<ArrowVectorType> types = new ArrayList<>(vectors.size());
    for (VectorLayout vector : vectors) {
      types.add(vector.getType());
    }
    return types;
  }

  public String toString() {
    return vectors.toString();
  }

  @Override
  public int hashCode() {
    return vectors.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    TypeLayout other = (TypeLayout) obj;
    return vectors.equals(other.vectors);
  }

}
