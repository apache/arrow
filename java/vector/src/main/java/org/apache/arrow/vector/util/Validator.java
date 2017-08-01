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

package org.apache.arrow.vector.util;

import java.util.Arrays;
import java.util.List;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import com.google.common.base.Objects;

/**
 * Utility class for validating arrow data structures
 */
public class Validator {

  /**
   * Validate two arrow schemas are equal.
   *
   * @param schema1 the 1st schema to compare
   * @param schema2 the 2nd schema to compare
   * @throws IllegalArgumentException if they are different.
   */
  public static void compareSchemas(Schema schema1, Schema schema2) {
    if (!schema2.equals(schema1)) {
      throw new IllegalArgumentException("Different schemas:\n" + schema2 + "\n" + schema1);
    }
  }

  /**
   * Validate two Dictionary encodings and dictionaries with id's from the encodings
   */
  public static void compareDictionaries(List<DictionaryEncoding> encodings1, List<DictionaryEncoding> encodings2, DictionaryProvider provider1, DictionaryProvider provider2) {

    if (encodings1.size() != encodings2.size()) {
      throw new IllegalArgumentException("Different dictionary encoding count:\n" + encodings1.size() + "\n" + encodings2.size());
    }

    for (int i = 0; i < encodings1.size(); i++) {
      if (!encodings1.get(i).equals(encodings2.get(i))) {
        throw new IllegalArgumentException("Different dictionary encodings:\n" + encodings1.get(i) + "\n" + encodings2.get(i));
      }

      long id = encodings1.get(i).getId();
      Dictionary dict1 = provider1.lookup(id);
      Dictionary dict2 = provider2.lookup(id);

      if (dict1 == null || dict2 == null) {
        throw new IllegalArgumentException("The DictionaryProvider did not contain the required dictionary with id: " + id + "\n" + dict1 + "\n" + dict2);
      }

      try {
        compareFieldVectors(dict1.getVector(), dict2.getVector());
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException("Different dictionaries:\n" + dict1 + "\n" + dict2, e);
      }
    }
  }

  /**
   * Validate two arrow vectorSchemaRoot are equal.
   *
   * @param root1 the 1st schema to compare
   * @param root2 the 2nd schema to compare
   * @throws IllegalArgumentException if they are different.
   */
  public static void compareVectorSchemaRoot(VectorSchemaRoot root1, VectorSchemaRoot root2) {
    compareSchemas(root2.getSchema(), root1.getSchema());
    if (root1.getRowCount() != root2.getRowCount()) {
      throw new IllegalArgumentException("Different row count:\n" + root1.getRowCount() + " != " + root2.getRowCount());
    }
    List<FieldVector> vectors1 = root1.getFieldVectors();
    List<FieldVector> vectors2 = root2.getFieldVectors();
    if (vectors1.size() != vectors2.size()) {
      throw new IllegalArgumentException("Different column count:\n" + vectors1.toString() + "\n!=\n" + vectors2.toString());
    }
    for (int i = 0; i < vectors1.size(); i++) {
      compareFieldVectors(vectors1.get(i), vectors2.get(i));
    }
  }

  /**
   * Validate two arrow FieldVectors are equal.
   *
   * @param vector1 the 1st VectorField to compare
   * @param vector2 the 2nd VectorField to compare
   * @throws IllegalArgumentException if they are different
   */
  public static void compareFieldVectors(FieldVector vector1, FieldVector vector2) {
    Field field1 = vector1.getField();
    if (!field1.equals(vector2.getField())) {
      throw new IllegalArgumentException("Different Fields:\n" + field1 + "\n!=\n" + vector2.getField());
    }
    int valueCount = vector1.getAccessor().getValueCount();
    if (valueCount != vector2.getAccessor().getValueCount()) {
      throw new IllegalArgumentException("Different value count for field " + field1 + " : " + valueCount + " != " + vector2.getAccessor().getValueCount());
    }
    for (int j = 0; j < valueCount; j++) {
      Object obj1 = vector1.getAccessor().getObject(j);
      Object obj2 = vector2.getAccessor().getObject(j);
      if (!equals(field1.getType(), obj1, obj2)) {
        throw new IllegalArgumentException(
            "Different values in column:\n" + field1 + " at index " + j + ": " + obj1 + " != " + obj2);
      }
    }
  }

  static boolean equals(ArrowType type, final Object o1, final Object o2) {
    if (type instanceof ArrowType.FloatingPoint) {
      ArrowType.FloatingPoint fpType = (ArrowType.FloatingPoint) type;
      switch (fpType.getPrecision()) {
        case DOUBLE:
          return equalEnough((Double) o1, (Double) o2);
        case SINGLE:
          return equalEnough((Float) o1, (Float) o2);
        case HALF:
        default:
          throw new UnsupportedOperationException("unsupported precision: " + fpType);
      }
    } else if (type instanceof ArrowType.Binary) {
      return Arrays.equals((byte[]) o1, (byte[]) o2);
    }

    return Objects.equal(o1, o2);
  }

  static boolean equalEnough(Float f1, Float f2) {
    if (f1 == null || f2 == null) {
      return f1 == null && f2 == null;
    }
    if (f1.isNaN()) {
      return f2.isNaN();
    }
    if (f1.isInfinite()) {
      return f2.isInfinite() && Math.signum(f1) == Math.signum(f2);
    }
    float average = Math.abs((f1 + f2) / 2);
    float differenceScaled = Math.abs(f1 - f2) / (average == 0.0f ? 1f : average);
    return differenceScaled < 1.0E-6f;
  }

  static boolean equalEnough(Double f1, Double f2) {
    if (f1 == null || f2 == null) {
      return f1 == null && f2 == null;
    }
    if (f1.isNaN()) {
      return f2.isNaN();
    }
    if (f1.isInfinite()) {
      return f2.isInfinite() && Math.signum(f1) == Math.signum(f2);
    }
    double average = Math.abs((f1 + f2) / 2);
    double differenceScaled = Math.abs(f1 - f2) / (average == 0.0d ? 1d : average);
    return differenceScaled < 1.0E-12d;
  }
}
