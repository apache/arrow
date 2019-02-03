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

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.junit.Assert;
import org.junit.Test;

public class DecimalTypeUtilTest {

  @Test
  public void testOutputTypesForAdd() {
    ArrowType.Decimal operand1 = getDecimal(30, 10);
    ArrowType.Decimal operand2 = getDecimal(30, 10);
    ArrowType.Decimal resultType = DecimalTypeUtil.getResultTypeForOperation(DecimalTypeUtil
            .OperationType.ADD, operand1, operand2);
    Assert.assertTrue(getDecimal(31, 10).equals(resultType));

    operand1 = getDecimal(30, 6);
    operand2 = getDecimal(30, 5);
    resultType = DecimalTypeUtil.getResultTypeForOperation(DecimalTypeUtil
            .OperationType.ADD, operand1, operand2);
    Assert.assertTrue(getDecimal(32, 6).equals(resultType));

    operand1 = getDecimal(30, 10);
    operand2 = getDecimal(38, 10);
    resultType = DecimalTypeUtil.getResultTypeForOperation(DecimalTypeUtil
            .OperationType.ADD, operand1, operand2);
    Assert.assertTrue(getDecimal(38, 9).equals(resultType));

    operand1 = getDecimal(38, 10);
    operand2 = getDecimal(38, 38);
    resultType = DecimalTypeUtil.getResultTypeForOperation(DecimalTypeUtil
            .OperationType.ADD, operand1, operand2);
    Assert.assertTrue(getDecimal(38, 9).equals(resultType));

    operand1 = getDecimal(38, 10);
    operand2 = getDecimal(38, 2);
    resultType = DecimalTypeUtil.getResultTypeForOperation(DecimalTypeUtil
            .OperationType.ADD, operand1, operand2);
    Assert.assertTrue(getDecimal(38, 6).equals(resultType));

  }

  @Test
  public void testOutputTypesForMultiply() {
    ArrowType.Decimal operand1 = getDecimal(30, 10);
    ArrowType.Decimal operand2 = getDecimal(30, 10);
    ArrowType.Decimal resultType = DecimalTypeUtil.getResultTypeForOperation(DecimalTypeUtil
                    .OperationType.MULTIPLY, operand1, operand2);
    Assert.assertTrue(getDecimal(38, 6).equals(resultType));

    operand1 = getDecimal(38, 10);
    operand2 = getDecimal(9, 2);
    resultType = DecimalTypeUtil.getResultTypeForOperation(DecimalTypeUtil
            .OperationType.MULTIPLY, operand1, operand2);
    Assert.assertTrue(getDecimal(38, 6).equals(resultType));

  }

  @Test
  public void testOutputTypesForMod() {
    ArrowType.Decimal operand1 = getDecimal(30, 10);
    ArrowType.Decimal operand2 = getDecimal(28  , 7);
    ArrowType.Decimal resultType = DecimalTypeUtil.getResultTypeForOperation(DecimalTypeUtil
                    .OperationType.MOD, operand1, operand2);
    Assert.assertTrue(getDecimal(30, 10).equals(resultType));
  }

  private ArrowType.Decimal getDecimal(int precision, int scale) {
    return new ArrowType.Decimal(precision, scale);
  }

}
