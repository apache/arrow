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

package org.apache.arrow.vector.performance.sql;

import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.holders.NullableVarCharHolder;
import org.apache.arrow.vector.types.pojo.Field;
import org.junit.Test;

/**
 * Evaluate the benchmarks for TPC-H Q1.
 */
public class TpchQ1Test extends SqlPerormancefTestBase {

  /**
   * Evaluate the workload of an operator that produces new data by projecting and filtering the input data.
   *
   * @return the duration of the evaluation, in nano-second.
   */
  private long projectAndFilter() {
    // create input schema
    Field[] inputFields = new Field[]{
      new Field("inCol0", DOUBLE_TYPE, null),
      new Field("inCol1", DOUBLE_TYPE, null),
      new Field("inCol2", DOUBLE_TYPE, null),
      new Field("inCol3", DOUBLE_TYPE, null),
      new Field("inCol4", STRING_TYPE, null),
      new Field("inCol5", STRING_TYPE, null),
      new Field("inCol6", DATE_DAY_TYPE, null),
    };

    // create input data
    ValueVector[] inputVectors = createVectors(inputFields, true);

    // create output schema
    Field[] outputFields = new Field[]{
      new Field("outCol0", STRING_TYPE, null),
      new Field("outCol1", STRING_TYPE, null),
      new Field("outCol2", DOUBLE_TYPE, null),
      new Field("outCol3", DOUBLE_TYPE, null),
      new Field("outCol4", DOUBLE_TYPE, null),
      new Field("outCol5", DOUBLE_TYPE, null),
      new Field("outCol6", DOUBLE_TYPE, null),
    };

    // create output data
    ValueVector[] outputVectors = createVectors(outputFields, false);

    // vector references that will be used in the computations
    Float8Vector inputCol0 = (Float8Vector) inputVectors[0];
    Float8Vector inputCol1 = (Float8Vector) inputVectors[1];
    Float8Vector inputCol2 = (Float8Vector) inputVectors[2];
    Float8Vector inputCol3 = (Float8Vector) inputVectors[3];
    VarCharVector inputCol4 = (VarCharVector) inputVectors[4];
    VarCharVector inputCol5 = (VarCharVector) inputVectors[5];
    DateDayVector inputCol6 = (DateDayVector) inputVectors[6];

    VarCharVector outputCol0 = (VarCharVector) outputVectors[0];
    VarCharVector outputCol1 = (VarCharVector) outputVectors[1];
    Float8Vector outputCol2 = (Float8Vector) outputVectors[2];
    Float8Vector outputCol3 = (Float8Vector) outputVectors[3];
    Float8Vector outputCol4 = (Float8Vector) outputVectors[4];
    Float8Vector outputCol5 = (Float8Vector) outputVectors[5];
    Float8Vector outputCol6 = (Float8Vector) outputVectors[6];

    // do evaluation
    long start = System.nanoTime();
    for (int i = 0; i < DEFAULT_CAPACITY; i++) {
      boolean isInputCol6Null = inputCol6.isNull(i);
      int inputCol6Value = -1;
      if (!isInputCol6Null) {
        inputCol6Value = inputCol6.get(i);
      }

      boolean filterResult = false;
      if (!isInputCol6Null) {
        filterResult = inputCol6Value <= 10441;
      }

      if (filterResult) {
        boolean isInputCol5Null = inputCol5.isNull(i);
        NullableVarCharHolder inputCol5Value = new NullableVarCharHolder();
        if (!isInputCol5Null) {
          inputCol5.get(i, inputCol5Value);
        }
        boolean isInputCol0Null = inputCol0.isNull(i);
        double inputCol0Value = -1.0d;
        if (!isInputCol0Null) {
          inputCol0Value = inputCol0.get(i);
        }
        boolean isInputCol2Null = inputCol2.isNull(i);
        double inputCol2Value = -1.0d;
        if (!isInputCol2Null) {
          inputCol2Value = inputCol2.get(i);
        }
        boolean isInputCol4Null = inputCol4.isNull(i);
        NullableVarCharHolder inputCol4Value = new NullableVarCharHolder();
        if (!isInputCol4Null) {
          inputCol4.get(i, inputCol4Value);
        }
        boolean isInputCol1Null = inputCol1.isNull(i);
        double inputCol1Value = -1.0d;
        if (!isInputCol1Null) {
          inputCol1Value = inputCol1.get(i);
        }
        boolean isInputCol3Null = inputCol3.isNull(i);
        double inputCol3Value = -1.0d;
        if (!isInputCol3Null) {
          inputCol3Value = inputCol3.get(i);
        }

        if (isInputCol4Null) {
          outputCol0.setNull(i);
        } else {
          outputCol0.setSafe(i, inputCol4Value);
        }

        if (isInputCol5Null) {
          outputCol1.setNull(i);
        } else {
          outputCol1.setSafe(i, inputCol5Value);
        }

        if (isInputCol0Null) {
          outputCol2.setNull(i);
        } else {
          outputCol2.set(i, inputCol0Value);
        }

        if (isInputCol1Null) {
          outputCol3.setNull(i);
        } else {
          outputCol3.set(i, inputCol1Value);
        }

        double minusResult1 = -1.0d;
        if (!isInputCol2Null) {
          minusResult1 = 1.0d - inputCol2Value;
        }

        boolean isInputCol1OrCol2Null = isInputCol1Null || isInputCol2Null;
        double multResult1 = -1.0d;
        if (!isInputCol1OrCol2Null) {
          multResult1 = inputCol1Value * minusResult1;
        }

        if (isInputCol1OrCol2Null) {
          outputCol4.setNull(i);
        } else {
          outputCol4.set(i, multResult1);
        }

        double minusResult2 = -1.0d;
        if (!isInputCol2Null) {
          minusResult2 = 1.0d - inputCol2Value;
        }

        double multResult2 = -1.0d;
        if (!isInputCol1OrCol2Null) {
          multResult2 = inputCol1Value * minusResult2;
        }

        double addResult = -1.0d;
        if (!isInputCol3Null) {
          addResult = 1.0d + inputCol3Value;
        }

        boolean isInputCol1OrCol2OrCol3Null = isInputCol1OrCol2Null || isInputCol3Null;
        double multResult3 = -1.0d;
        if (!isInputCol1OrCol2OrCol3Null) {
          multResult3 = multResult2 * addResult;
        }

        if (isInputCol1OrCol2OrCol3Null) {
          outputCol5.setNull(i);
        } else {
          outputCol5.set(i, multResult3);
        }

        if (isInputCol2Null) {
          outputCol6.setNull(i);
        } else {
          outputCol6.set(i, inputCol2Value);
        }
      }
    }
    long end = System.nanoTime();

    // dispose input/output data
    for (int i = 0; i < inputVectors.length; i++) {
      inputVectors[i].clear();
    }

    for (int i = 0; i < outputVectors.length; i++) {
      outputVectors[i].clear();
    }

    return end - start;
  }

  @Test
  public void testProjectAndFilter() {
    runBenchmark("TPC-H Q1#Project & Filter", () -> projectAndFilter(), 1);
  }
}
