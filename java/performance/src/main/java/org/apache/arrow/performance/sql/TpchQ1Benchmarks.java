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

import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.holders.NullableVarCharHolder;
import org.apache.arrow.vector.types.pojo.Field;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

import static org.apache.arrow.performance.sql.SqlPerformancefTestHelper.*;

/**
 * Evaluate the benchmarks for TPC-H Q1.
 */
public class TpchQ1Benchmarks {

  /**
   * Evaluate the workload of an operator that produces new data by projecting and filtering the input data.
   *
   * @return the duration of the evaluation, in nano-second.
   */
  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void projectAndFilter1() {
    SqlPerformancefTestHelper helper = new SqlPerformancefTestHelper();

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
    ValueVector[] inputVectors = helper.createVectors(inputFields, true);

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
    ValueVector[] outputVectors = helper.createVectors(outputFields, false);

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
    for (int i = 0; i < helper.DEFAULT_CAPACITY; i++) {
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

    // dispose input/output data
    for (int i = 0; i < inputVectors.length; i++) {
      inputVectors[i].clear();
    }

    for (int i = 0; i < outputVectors.length; i++) {
      outputVectors[i].clear();
    }

    helper.close();
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void projectAndFilter2() {
    SqlPerformancefTestHelper helper = new SqlPerformancefTestHelper();

    // create input schema
    Field[] inputFields = new Field[]{
      new Field("inCol0", STRING_TYPE, null),
      new Field("inCol1", STRING_TYPE, null),
      new Field("inCol2", DOUBLE_TYPE, null),
      new Field("inCol3", DOUBLE_TYPE, null),
      new Field("inCol4", DOUBLE_TYPE, null),
      new Field("inCol5", DOUBLE_TYPE, null),
      new Field("inCol6", BIG_INT_TYPE, null),
      new Field("inCol7", DOUBLE_TYPE, null),
    };

    // create input data
    ValueVector[] inputVectors = helper.createVectors(inputFields, true);

    // create output schema
    Field[] outputFields = new Field[]{
      new Field("outCol0", STRING_TYPE, null),
      new Field("outCol1", STRING_TYPE, null),
      new Field("outCol2", DOUBLE_TYPE, null),
      new Field("outCol3", DOUBLE_TYPE, null),
      new Field("outCol4", DOUBLE_TYPE, null),
      new Field("outCol5", DOUBLE_TYPE, null),
      new Field("outCol6", DOUBLE_TYPE, null),
      new Field("outCol7", DOUBLE_TYPE, null),
      new Field("outCol8", DOUBLE_TYPE, null),
      new Field("outCol9", BIG_INT_TYPE, null),
    };

    // create output data
    ValueVector[] outputVectors = helper.createVectors(outputFields, false);

    // vector references that will be used in the computations
    VarCharVector inputCol0 = (VarCharVector) inputVectors[0];
    VarCharVector inputCol1 = (VarCharVector) inputVectors[1];
    Float8Vector inputCol2 = (Float8Vector) inputVectors[2];
    Float8Vector inputCol3 = (Float8Vector) inputVectors[3];
    Float8Vector inputCol4 = (Float8Vector) inputVectors[4];
    Float8Vector inputCol5 = (Float8Vector) inputVectors[5];
    BigIntVector inputCol6 = (BigIntVector) inputVectors[6];
    Float8Vector inputCol7 = (Float8Vector) inputVectors[7];

    VarCharVector outputCol0 = (VarCharVector) outputVectors[0];
    VarCharVector outputCol1 = (VarCharVector) outputVectors[1];
    Float8Vector outputCol2 = (Float8Vector) outputVectors[2];
    Float8Vector outputCol3 = (Float8Vector) outputVectors[3];
    Float8Vector outputCol4 = (Float8Vector) outputVectors[4];
    Float8Vector outputCol5 = (Float8Vector) outputVectors[5];
    Float8Vector outputCol6 = (Float8Vector) outputVectors[6];
    Float8Vector outputCol7 = (Float8Vector) outputVectors[7];
    Float8Vector outputCol8 = (Float8Vector) outputVectors[8];
    BigIntVector outputCol9 = (BigIntVector) outputVectors[9];

    // do evaluation
    for (int i = 0; i < DEFAULT_CAPACITY; i++) {
      boolean isInputCol6Null = inputCol6.isNull(i);
      long inputCol6Value = -1L;
      if (!isInputCol6Null) {
        inputCol6Value = inputCol6.get(i);
      }
      boolean isInputCol5Null = inputCol5.isNull(i);
      double inputCol5Value = -1.0d;
      if (!isInputCol5Null) {
        inputCol5Value = inputCol5.get(i);
      }
      boolean isInputCol2Null = inputCol2.isNull(i);
      double inputCol2Value = -1.0d;
      if (!isInputCol2Null) {
        inputCol2Value = inputCol2.get(i);
      }
      boolean isInputCol0Null = inputCol0.isNull(i);
      NullableVarCharHolder inputCol0Value = new NullableVarCharHolder();
      if (!isInputCol0Null) {
        inputCol0.get(i, inputCol0Value);
      }
      boolean isInputCol4Null = inputCol4.isNull(i);
      double inputCol4Value = -1.0d;
      if (!isInputCol4Null) {
        inputCol4Value = inputCol4.get(i);
      }
      boolean isInputCol1Null = inputCol1.isNull(i);
      NullableVarCharHolder inputCol1Value = new NullableVarCharHolder();
      if (!isInputCol1Null) {
        inputCol1.get(i, inputCol1Value);
      }
      boolean isInputCol7Null = inputCol7.isNull(i);
      double inputCol7Value = -1.0d;
      if (!isInputCol7Null) {
        inputCol7Value = inputCol7.get(i);
      }
      boolean isInputCol3Null = inputCol3.isNull(i);
      double inputCol3Value = -1.0d;
      if (!isInputCol3Null) {
        inputCol3Value = inputCol3.get(i);
      }

      if (isInputCol0Null) {
        outputCol0.setNull(i);
      } else {
        outputCol0.setSafe(i, inputCol0Value);
      }

      if (isInputCol1Null) {
        outputCol1.setNull(i);
      } else {
        outputCol1.setSafe(i, inputCol1Value);
      }

      if (isInputCol2Null) {
        outputCol2.setNull(i);
      } else {
        outputCol2.set(i, inputCol2Value);
      }

      if (isInputCol3Null) {
        outputCol3.setNull(i);
      } else {
        outputCol3.set(i, inputCol3Value);
      }

      if (isInputCol4Null) {
        outputCol4.setNull(i);
      } else {
        outputCol4.set(i, inputCol4Value);
      }

      if (isInputCol5Null) {
        outputCol5.setNull(i);
      } else {
        outputCol5.set(i, inputCol5Value);
      }

      boolean isInputCol2OrCol6Null = isInputCol2Null || isInputCol6Null;
      double divResul1 = -1.0d;
      if (!isInputCol2OrCol6Null) {
        divResul1 = inputCol2Value / Long.valueOf(inputCol6Value).doubleValue();
      }

      if (isInputCol2OrCol6Null) {
        outputCol6.setNull(i);
      } else {
        outputCol6.set(i, divResul1);
      }

      boolean isInputCol3OrCol6Null = isInputCol3Null || isInputCol6Null;
      double divResult2 = -1.0d;
      if (!isInputCol3OrCol6Null) {
        divResult2 = inputCol3Value / Long.valueOf(inputCol6Value).doubleValue();
      }

      if (isInputCol3OrCol6Null) {
        outputCol7.setNull(i);
      } else {
        outputCol7.set(i, divResult2);
      }

      boolean isInputCol7OrCol6Null = isInputCol7Null || isInputCol6Null;
      double divResult3 = -1.0d;
      if (!isInputCol7OrCol6Null) {
        divResult3 = inputCol7Value / Long.valueOf(inputCol6Value).doubleValue();
      }

      if (isInputCol7OrCol6Null) {
        outputCol8.setNull(i);
      } else {
        outputCol8.set(i, divResult3);
      }

      if (isInputCol6Null) {
        outputCol9.setNull(i);
      } else {
        outputCol9.set(i, inputCol6Value);
      }
    }

    // dispose input/output data
    for (int i = 0; i < inputVectors.length; i++) {
      inputVectors[i].clear();
    }

    for (int i = 0; i < outputVectors.length; i++) {
      outputVectors[i].clear();
    }
    helper.close();
  }

  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder()
            .include(TpchQ1Benchmarks.class.getSimpleName())
            .forks(1)
            .build();

    new Runner(opt).run();
  }
}
