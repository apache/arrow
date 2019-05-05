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

package org.apache.arrow.performance.sql;

import static org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE;
import static org.apache.arrow.vector.types.FloatingPointPrecision.SINGLE;

import java.util.Random;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.FixedWidthVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VariableWidthVector;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.openjdk.jmh.annotations.State;

/**
 * The base class for all SQL performance evaluations.
 */
public class SqlPerformancefTestHelper {

  /**
   * The total amount of memory used in each evaluation.
   */
  public static final long ALLOCATION_CAPACITY = 5 * 1024 * 1024L;

  /**
   * The default capacity of each vector.
   */
  public static final int DEFAULT_CAPACITY = 1024;

  /**
   * The probability that a randomly generated value be null.
   */
  public static final double NULL_PROBABILITY = 0.1;

  /**
   * The minimum length of a random string (inclusive).
   */
  public static final int VAR_LENGTH_DATA_MIN_SIZE = 5;

  /**
   * The maximum length of a random string (exclusive).
   */
  public static final int VAR_LENGTH_DATA_MAX_SIZE = 20;

  /**
   * The initial size reserved for a random string.
   */
  public static final int VAR_LENGTH_DATA_INIT_SIZE = 16;

  // /UTILITIES FOR DATA TYPES FROM HERE

  public static final FieldType BOOLEAN_TYPE =
          new FieldType(true, new ArrowType.Bool(), null);

  public static final FieldType TINY_INT_TYPE =
          new FieldType(true, new ArrowType.Int(8, true), null);

  public static final FieldType SMALL_INT_TYPE =
          new FieldType(true, new ArrowType.Int(16, true), null);

  public static final FieldType INT_TYPE =
          new FieldType(true, new ArrowType.Int(32, true), null);

  public static final FieldType BIG_INT_TYPE =
          new FieldType(true, new ArrowType.Int(64, true), null);

  public static final FieldType FLOAT_TYPE =
          new FieldType(true, new ArrowType.FloatingPoint(SINGLE), null);

  public static final FieldType DOUBLE_TYPE =
          new FieldType(true, new ArrowType.FloatingPoint(DOUBLE), null);

  public static final FieldType STRING_TYPE =
          new FieldType(true, new ArrowType.Utf8(), null);

  public static final FieldType BYTE_ARRAY_TYPE =
          new FieldType(true, new ArrowType.Binary(), null);

  public static final FieldType DATE_DAY_TYPE =
          new FieldType(true, new ArrowType.Date(DateUnit.DAY), null);

  /**
   * The allocator for all vectors.
   */
  protected RootAllocator allocator = new RootAllocator(ALLOCATION_CAPACITY);

  /**
   * Random number generator.
   * A constant seed is applied so the evaluations can be reproduced with exactly the same data.
   */
  private Random random = new Random(0);

  // /UTILITY METHODS FROM HERE

  /**
   * Utility method to create vectors.
   *
   * @param fields   the schema of the vectors.
   * @param fillData if it is true, fill the vectors will random data.
   * @return the created vectors.
   */
  public ValueVector[] createVectors(Field[] fields, boolean fillData) {
    ValueVector[] ret = new ValueVector[fields.length];
    for (int i = 0; i < fields.length; i++) {
      // create an empty vector
      ret[i] = createVector(fields[i]);

      // reserve memory space for the vector
      if (ret[i] instanceof FixedWidthVector) {
        ((FixedWidthVector) ret[i]).allocateNew(DEFAULT_CAPACITY);
      } else if (ret[i] instanceof VariableWidthVector) {
        ((VariableWidthVector) ret[i]).allocateNew(VAR_LENGTH_DATA_INIT_SIZE * DEFAULT_CAPACITY, DEFAULT_CAPACITY);
      }

      // fill random data, if necessary
      if (fillData) {
        fillRandomData(ret[i]);
      }
    }
    return ret;
  }

  /**
   * Create an empty vector.
   *
   * @param field The name and data type of the vector to create.
   * @return the created vector.
   */
  protected ValueVector createVector(Field field) {
    ArrowType vectorType = field.getType();
    String name = field.getName();

    if (vectorType instanceof ArrowType.Bool) {
      return new BitVector(name, allocator);
    } else if (vectorType instanceof ArrowType.Int) {
      ArrowType.Int intType = (ArrowType.Int) vectorType;
      switch (intType.getBitWidth()) {
        case 8:
          return new TinyIntVector(name, allocator);
        case 16:
          return new SmallIntVector(name, allocator);
        case 32:
          return new IntVector(name, allocator);
        case 64:
          return new BigIntVector(name, allocator);
        default:
          throw new IllegalArgumentException("Unknown width for int type: " + intType.getBitWidth());
      }
    } else if (vectorType instanceof ArrowType.FloatingPoint) {
      ArrowType.FloatingPoint floatType = (ArrowType.FloatingPoint) vectorType;
      switch (floatType.getPrecision()) {
        case SINGLE:
          return new Float4Vector(name, allocator);
        case DOUBLE:
          return new Float8Vector(name, allocator);
        default:
          throw new IllegalArgumentException("Unknown precision for float type: " + floatType.getPrecision());
      }
    } else if (vectorType instanceof ArrowType.Binary) {
      return new VarBinaryVector(name, allocator);
    } else if (vectorType instanceof ArrowType.Utf8) {
      return new VarCharVector(name, allocator);
    } else if (vectorType instanceof ArrowType.Date && ((ArrowType.Date) vectorType).getUnit() == DateUnit.DAY) {
      return new DateDayVector(name, allocator);
    } else {
      throw new IllegalArgumentException("Unknown arrow type: " + vectorType);
    }
  }

  /**
   * Fill random data to a vector.
   *
   * @param vector the vector to fill.
   */
  private void fillRandomData(ValueVector vector) {
    if (vector instanceof BitVector) {
      BitVector vec = (BitVector) vector;
      for (int i = 0; i < vector.getValueCapacity(); i++) {
        boolean isNull = random.nextDouble() <= NULL_PROBABILITY;
        if (isNull) {
          vec.setNull(i);
        } else {
          vec.set(i, random.nextBoolean() ? 1 : 0);
        }
      }
    } else if (vector instanceof TinyIntVector) {
      TinyIntVector vec = (TinyIntVector) vector;
      for (int i = 0; i < vector.getValueCapacity(); i++) {
        boolean isNull = random.nextDouble() <= NULL_PROBABILITY;
        if (isNull) {
          vec.setNull(i);
        } else {
          vec.set(i, random.nextInt());
        }
      }
    } else if (vector instanceof SmallIntVector) {
      SmallIntVector vec = (SmallIntVector) vector;
      for (int i = 0; i < vector.getValueCapacity(); i++) {
        boolean isNull = random.nextDouble() <= NULL_PROBABILITY;
        if (isNull) {
          vec.setNull(i);
        } else {
          vec.set(i, random.nextInt());
        }
      }
    } else if (vector instanceof IntVector) {
      IntVector vec = (IntVector) vector;
      for (int i = 0; i < vector.getValueCapacity(); i++) {
        boolean isNull = random.nextDouble() <= NULL_PROBABILITY;
        if (isNull) {
          vec.setNull(i);
        } else {
          vec.set(i, random.nextInt());
        }
      }
    } else if (vector instanceof BigIntVector) {
      BigIntVector vec = (BigIntVector) vector;
      for (int i = 0; i < vector.getValueCapacity(); i++) {
        boolean isNull = random.nextDouble() <= NULL_PROBABILITY;
        if (isNull) {
          vec.setNull(i);
        } else {
          vec.set(i, random.nextLong());
        }
      }
    } else if (vector instanceof Float4Vector) {
      Float4Vector vec = (Float4Vector) vector;
      for (int i = 0; i < vector.getValueCapacity(); i++) {
        boolean isNull = random.nextDouble() <= NULL_PROBABILITY;
        if (isNull) {
          vec.setNull(i);
        } else {
          vec.set(i, random.nextFloat());
        }
      }
    } else if (vector instanceof Float8Vector) {
      Float8Vector vec = (Float8Vector) vector;
      for (int i = 0; i < vector.getValueCapacity(); i++) {
        boolean isNull = random.nextDouble() <= NULL_PROBABILITY;
        if (isNull) {
          vec.setNull(i);
        } else {
          vec.set(i, random.nextDouble());
        }
      }
    } else if (vector instanceof VarBinaryVector) {
      VarBinaryVector vec = (VarBinaryVector) vector;
      for (int i = 0; i < vector.getValueCapacity(); i++) {
        boolean isNull = random.nextDouble() <= NULL_PROBABILITY;
        if (isNull) {
          vec.setNull(i);
        } else {
          int length = getVariableDataLength();
          byte[] buffer = new byte[length];
          random.nextBytes(buffer);
          vec.setSafe(i, buffer);
        }
      }
    } else if (vector instanceof VarCharVector) {
      VarCharVector vec = (VarCharVector) vector;
      for (int i = 0; i < vector.getValueCapacity(); i++) {
        boolean isNull = random.nextDouble() <= NULL_PROBABILITY;
        if (isNull) {
          vec.setNull(i);
        } else {
          int length = getVariableDataLength();
          byte[] buffer = new byte[length];
          random.nextBytes(buffer);
          vec.setSafe(i, buffer);
        }
      }
    } else if (vector instanceof DateDayVector) {
      DateDayVector vec = (DateDayVector) vector;
      for (int i = 0; i < vector.getValueCapacity(); i++) {
        boolean isNull = random.nextDouble() <= NULL_PROBABILITY;
        if (isNull) {
          vec.setNull(i);
        } else {
          int day = random.nextInt() & Integer.MAX_VALUE % (2000 * 365);
          vec.set(i, day);
        }
      }
    } else {
      throw new IllegalArgumentException("Vector with type " + vector.getClass() + " is not supported.");
    }

  }

  /**
   * Generate the length of a random byte array.
   * @return the length for the byte array.
   */
  private int getVariableDataLength() {
    // first, generate a non-negative integer;
    int r = random.nextInt() & Integer.MAX_VALUE;
    return r % (VAR_LENGTH_DATA_MAX_SIZE - VAR_LENGTH_DATA_MIN_SIZE) + VAR_LENGTH_DATA_MIN_SIZE;
  }

  public void close() {
    this.allocator.close();
  }
}
