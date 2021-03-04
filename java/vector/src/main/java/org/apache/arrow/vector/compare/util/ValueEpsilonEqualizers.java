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

package org.apache.arrow.vector.compare.util;

import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.FloatingPointVector;
import org.apache.arrow.vector.compare.VectorValueEqualizer;

/**
 * Vector value equalizers that regard values as equal if their difference
 * is within a small threshold (epsilon).
 */
public class ValueEpsilonEqualizers {

  private ValueEpsilonEqualizers() {

  }

  /**
   * Difference function for floating point values.
   */
  public static class FloatingPointEpsilonEqualizer implements VectorValueEqualizer<FloatingPointVector> {
    private final double epsilon;

    public FloatingPointEpsilonEqualizer(double epsilon) {
      this.epsilon = epsilon;
    }

    @Override
    public final boolean valuesEqual(
            FloatingPointVector vector1, int index1, FloatingPointVector vector2, int index2) {
      boolean isNull1 = vector1.isNull(index1);
      boolean isNull2 = vector2.isNull(index2);

      if (isNull1 || isNull2) {
        return isNull1 == isNull2;
      }

      double d1 = vector1.getValueAsDouble(index1);
      double d2 = vector2.getValueAsDouble(index2);

      if (Double.isNaN(d1)) {
        return Double.isNaN(d2);
      }
      if (Double.isInfinite(d1)) {
        return Double.isInfinite(d2) && Math.signum(d1) == Math.signum(d2);
      }

      return Math.abs(d1 - d2) <= epsilon;
    }

    @Override
    public VectorValueEqualizer<FloatingPointVector> clone() {
      return new FloatingPointEpsilonEqualizer(epsilon);
    }
  }

  /**
   * Difference function for float values.
   */
  public static class Float4EpsilonEqualizer implements VectorValueEqualizer<Float4Vector> {
    private final float epsilon;

    public Float4EpsilonEqualizer(float epsilon) {
      this.epsilon = epsilon;
    }

    @Override
    public final boolean valuesEqual(Float4Vector vector1, int index1, Float4Vector vector2, int index2) {
      boolean isNull1 = vector1.isNull(index1);
      boolean isNull2 = vector2.isNull(index2);

      if (isNull1 || isNull2) {
        return isNull1 == isNull2;
      }

      float f1 = vector1.get(index1);
      float f2 = vector2.get(index2);

      if (Float.isNaN(f1)) {
        return Float.isNaN(f2);
      }
      if (Float.isInfinite(f1)) {
        return Float.isInfinite(f2) && Math.signum(f1) == Math.signum(f2);
      }

      return Math.abs(f1 - f2) <= epsilon;
    }

    @Override
    public VectorValueEqualizer<Float4Vector> clone() {
      return new Float4EpsilonEqualizer(epsilon);
    }
  }

  /**
   * Difference function for double values.
   */
  public static class Float8EpsilonEqualizer implements VectorValueEqualizer<Float8Vector> {
    private final double epsilon;

    public Float8EpsilonEqualizer(double epsilon) {
      this.epsilon = epsilon;
    }

    @Override
    public final boolean valuesEqual(Float8Vector vector1, int index1, Float8Vector vector2, int index2) {
      boolean isNull1 = vector1.isNull(index1);
      boolean isNull2 = vector2.isNull(index2);

      if (isNull1 || isNull2) {
        return isNull1 == isNull2;
      }

      double d1 = vector1.get(index1);
      double d2 = vector2.get(index2);

      if (Double.isNaN(d1)) {
        return Double.isNaN(d2);
      }
      if (Double.isInfinite(d1)) {
        return Double.isInfinite(d2) && Math.signum(d1) == Math.signum(d2);
      }

      return Math.abs(d1 - d2) <= epsilon;
    }

    @Override
    public VectorValueEqualizer<Float8Vector> clone() {
      return new Float8EpsilonEqualizer(epsilon);
    }
  }
}
