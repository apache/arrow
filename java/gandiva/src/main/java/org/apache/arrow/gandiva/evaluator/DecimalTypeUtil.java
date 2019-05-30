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

package org.apache.arrow.gandiva.evaluator;

import org.apache.arrow.vector.types.pojo.ArrowType.Decimal;

/**
 * Utility methods for working with {@link Decimal} values.
 */
public class DecimalTypeUtil {
  private DecimalTypeUtil() {}

  /**
   * Enum for supported mathematical operations.
   */
  public enum OperationType {
    ADD,
    SUBTRACT,
    MULTIPLY,
    DIVIDE,
    MOD
  }

  private static final int MIN_ADJUSTED_SCALE = 6;
  /// The maximum precision representable by a 16-byte decimal
  private static final int MAX_PRECISION = 38;

  /**
   * Determines the scale and precision of applying the given operation to the operands.
   */
  public static Decimal getResultTypeForOperation(OperationType operation, Decimal operand1, Decimal
          operand2) {
    int s1 = operand1.getScale();
    int s2 = operand2.getScale();
    int p1 = operand1.getPrecision();
    int p2 = operand2.getPrecision();
    int resultScale = 0;
    int resultPrecision = 0;
    switch (operation) {
      case ADD:
      case SUBTRACT:
        resultScale = Math.max(operand1.getScale(), operand2.getScale());
        resultPrecision = resultScale + Math.max(operand1.getPrecision() - operand1.getScale(),
                operand2.getPrecision() - operand2.getScale()) + 1;
        break;
      case MULTIPLY:
        resultScale = s1 + s2;
        resultPrecision = p1 + p2 + 1;
        break;
      case DIVIDE:
        resultScale =
                Math.max(MIN_ADJUSTED_SCALE, operand1.getScale() + operand2.getPrecision() + 1);
        resultPrecision =
                operand1.getPrecision() - operand1.getScale() + operand2.getScale() + resultScale;
        break;
      case MOD:
        resultScale = Math.max(operand1.getScale(), operand2.getScale());
        resultPrecision = Math.min(operand1.getPrecision() - operand1.getScale(),
                                    operand2.getPrecision() - operand2.getScale()) +
                           resultScale;
        break;
      default:
        throw new RuntimeException("Needs support");
    }
    return adjustScaleIfNeeded(resultPrecision, resultScale);
  }

  private static Decimal adjustScaleIfNeeded(int precision, int scale) {
    if (precision > MAX_PRECISION) {
      int minScale = Math.min(scale, MIN_ADJUSTED_SCALE);
      int delta = precision - MAX_PRECISION;
      precision = MAX_PRECISION;
      scale = Math.max(scale - delta, minScale);
    }
    return new Decimal(precision, scale);
  }

}

