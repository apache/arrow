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


import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.arrow.gandiva.exceptions.GandivaException;
import org.apache.arrow.gandiva.expression.ExpressionTree;
import org.apache.arrow.gandiva.expression.TreeBuilder;
import org.apache.arrow.gandiva.expression.TreeNode;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.ArrowType.Decimal;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Test;

import com.google.common.collect.Lists;

public class ProjectorDecimalTest extends org.apache.arrow.gandiva.evaluator.BaseEvaluatorTest {

  @Test
  public void test_add() throws GandivaException {
    int precision = 38;
    int scale = 8;
    ArrowType.Decimal decimal = new ArrowType.Decimal(precision, scale);
    Field a = Field.nullable("a", decimal);
    Field b = Field.nullable("b", decimal);
    List<Field> args = Lists.newArrayList(a, b);

    ArrowType.Decimal outputType = DecimalTypeUtil.getResultTypeForOperation(DecimalTypeUtil
            .OperationType.ADD, decimal, decimal);
    Field retType = Field.nullable("c", outputType);
    ExpressionTree root = TreeBuilder.makeExpression("add", args, retType);

    List<ExpressionTree> exprs = Lists.newArrayList(root);

    Schema schema = new Schema(args);
    Projector eval = Projector.make(schema, exprs);

    int numRows = 4;
    byte[] validity = new byte[]{(byte) 255};
    String[] aValues = new String[]{"1.12345678","2.12345678","3.12345678","4.12345678"};
    String[] bValues = new String[]{"2.12345678","3.12345678","4.12345678","5.12345678"};

    DecimalVector valuesa = decimalVector(aValues, precision, scale);
    DecimalVector valuesb = decimalVector(bValues, precision, scale);
    ArrowRecordBatch batch =
            new ArrowRecordBatch(
                    numRows,
                    Lists.newArrayList(new ArrowFieldNode(numRows, 0), new ArrowFieldNode(numRows, 0)),
                    Lists.newArrayList(valuesa.getValidityBuffer(), valuesa.getDataBuffer(),
                            valuesb.getValidityBuffer(), valuesb.getDataBuffer()));

    DecimalVector outVector = new DecimalVector("decimal_output", allocator, outputType.getPrecision(),
            outputType.getScale());
    outVector.allocateNew(numRows);

    List<ValueVector> output = new ArrayList<ValueVector>();
    output.add(outVector);
    eval.evaluate(batch, output);

    // should have scaled down.
    BigDecimal[] expOutput = new BigDecimal[]{BigDecimal.valueOf(3.2469136),
                                              BigDecimal.valueOf(5.2469136),
                                              BigDecimal.valueOf(7.2469136),
                                              BigDecimal.valueOf(9.2469136)};

    for (int i = 0; i < 4; i++) {
      assertFalse(outVector.isNull(i));
      assertTrue("index : " + i + " failed compare", expOutput[i].compareTo(outVector.getObject(i)
      ) == 0);
    }

    // free buffers
    releaseRecordBatch(batch);
    releaseValueVectors(output);
    eval.close();
  }

  @Test
  public void test_add_literal() throws GandivaException {
    int precision = 2;
    int scale = 0;
    ArrowType.Decimal decimal = new ArrowType.Decimal(precision, scale);
    ArrowType.Decimal literalType = new ArrowType.Decimal(2, 1);
    Field a = Field.nullable("a", decimal);

    ArrowType.Decimal outputType = DecimalTypeUtil.getResultTypeForOperation(DecimalTypeUtil
            .OperationType.ADD, decimal, literalType);
    Field retType = Field.nullable("c", outputType);
    TreeNode field = TreeBuilder.makeField(a);
    TreeNode literal = TreeBuilder.makeDecimalLiteral("6", 2, 1);
    List<TreeNode> args = Lists.newArrayList(field, literal);
    TreeNode root = TreeBuilder.makeFunction("add", args, outputType);
    ExpressionTree tree = TreeBuilder.makeExpression(root, retType);

    List<ExpressionTree> exprs = Lists.newArrayList(tree);

    Schema schema = new Schema(Lists.newArrayList(a));
    Projector eval = Projector.make(schema, exprs);

    int numRows = 4;
    String[] aValues = new String[]{"1", "2", "3", "4"};

    DecimalVector valuesa = decimalVector(aValues, precision, scale);
    ArrowRecordBatch batch =
            new ArrowRecordBatch(
                    numRows,
                    Lists.newArrayList(new ArrowFieldNode(numRows, 0)),
                    Lists.newArrayList(valuesa.getValidityBuffer(), valuesa.getDataBuffer()));

    DecimalVector outVector = new DecimalVector("decimal_output", allocator, outputType.getPrecision(),
            outputType.getScale());
    outVector.allocateNew(numRows);

    List<ValueVector> output = new ArrayList<ValueVector>();
    output.add(outVector);
    eval.evaluate(batch, output);

    BigDecimal[] expOutput = new BigDecimal[]{BigDecimal.valueOf(1.6), BigDecimal.valueOf(2.6),
            BigDecimal.valueOf(3.6), BigDecimal.valueOf(4.6)};

    for (int i = 0; i < 4; i++) {
      assertFalse(outVector.isNull(i));
      assertTrue(expOutput[i].compareTo(outVector.getObject(i)) == 0);
    }

    // free buffers
    releaseRecordBatch(batch);
    releaseValueVectors(output);
    eval.close();
  }

  @Test
  public void test_multiply() throws GandivaException {
    int precision = 38;
    int scale = 8;
    ArrowType.Decimal decimal = new ArrowType.Decimal(precision, scale);
    Field a = Field.nullable("a", decimal);
    Field b = Field.nullable("b", decimal);
    List<Field> args = Lists.newArrayList(a, b);

    ArrowType.Decimal outputType = DecimalTypeUtil.getResultTypeForOperation(DecimalTypeUtil
        .OperationType.MULTIPLY, decimal, decimal);
    Field retType = Field.nullable("c", outputType);
    ExpressionTree root = TreeBuilder.makeExpression("multiply", args, retType);

    List<ExpressionTree> exprs = Lists.newArrayList(root);

    Schema schema = new Schema(args);
    Projector eval = Projector.make(schema, exprs);

    int numRows = 4;
    byte[] validity = new byte[]{(byte) 255};
    String[] aValues = new String[]{"1.12345678","2.12345678","3.12345678", "999999999999.99999999"};
    String[] bValues = new String[]{"2.12345678","3.12345678","4.12345678", "999999999999.99999999"};

    DecimalVector valuesa = decimalVector(aValues, precision, scale);
    DecimalVector valuesb = decimalVector(bValues, precision, scale);
    ArrowRecordBatch batch =
        new ArrowRecordBatch(
            numRows,
            Lists.newArrayList(new ArrowFieldNode(numRows, 0), new ArrowFieldNode(numRows, 0)),
            Lists.newArrayList(valuesa.getValidityBuffer(), valuesa.getDataBuffer(),
                valuesb.getValidityBuffer(), valuesb.getDataBuffer()));

    DecimalVector outVector = new DecimalVector("decimal_output", allocator, outputType.getPrecision(),
        outputType.getScale());
    outVector.allocateNew(numRows);

    List<ValueVector> output = new ArrayList<ValueVector>();
    output.add(outVector);
    eval.evaluate(batch, output);

    // should have scaled down.
    BigDecimal[] expOutput = new BigDecimal[]{BigDecimal.valueOf(2.385612),
        BigDecimal.valueOf(6.632525),
        BigDecimal.valueOf(12.879439),
        new BigDecimal("999999999999999999980000.000000")};

    for (int i = 0; i < 4; i++) {
      assertFalse(outVector.isNull(i));
      assertTrue("index : " + i + " failed compare", expOutput[i].compareTo(outVector.getObject(i)
      ) == 0);
    }

    // free buffers
    releaseRecordBatch(batch);
    releaseValueVectors(output);
    eval.close();
  }

  @Test
  public void testCompare() throws GandivaException {
    Decimal aType = new Decimal(38, 3);
    Decimal bType = new Decimal(38, 2);
    Field a = Field.nullable("a", aType);
    Field b = Field.nullable("b", bType);
    List<Field> args = Lists.newArrayList(a, b);

    List<ExpressionTree> exprs = new ArrayList<>(
        Arrays.asList(
            TreeBuilder.makeExpression("equal", args, Field.nullable("eq", boolType)),
            TreeBuilder.makeExpression("not_equal", args, Field.nullable("ne", boolType)),
            TreeBuilder.makeExpression("less_than", args, Field.nullable("lt", boolType)),
            TreeBuilder.makeExpression("less_than_or_equal_to", args, Field.nullable("le", boolType)),
            TreeBuilder.makeExpression("greater_than", args, Field.nullable("gt", boolType)),
            TreeBuilder.makeExpression("greater_than_or_equal_to", args, Field.nullable("ge", boolType))
        )
    );

    Schema schema = new Schema(args);
    Projector eval = Projector.make(schema, exprs);

    List<ValueVector> output = null;
    ArrowRecordBatch batch = null;
    try {
      int numRows = 4;
      String[] aValues = new String[]{"7.620", "2.380", "3.860", "-18.160"};
      String[] bValues = new String[]{"7.62", "3.50", "1.90", "-1.45"};

      DecimalVector valuesa = decimalVector(aValues, aType.getPrecision(), aType.getScale());
      DecimalVector valuesb = decimalVector(bValues, bType.getPrecision(), bType.getScale());
      batch =
          new ArrowRecordBatch(
              numRows,
              Lists.newArrayList(new ArrowFieldNode(numRows, 0), new ArrowFieldNode(numRows, 0)),
              Lists.newArrayList(valuesa.getValidityBuffer(), valuesa.getDataBuffer(),
                  valuesb.getValidityBuffer(), valuesb.getDataBuffer()));

      // expected results.
      boolean[][] expected = {
          {true, false, false, false}, // eq
          {false, true, true, true}, // ne
          {false, true, false, true}, // lt
          {true, true, false, true}, // le
          {false, false, true, false}, // gt
          {true, false, true, false}, // ge
      };

      // Allocate output vectors.
      output = new ArrayList<>(
          Arrays.asList(
              new BitVector("eq", allocator),
              new BitVector("ne", allocator),
              new BitVector("lt", allocator),
              new BitVector("le", allocator),
              new BitVector("gt", allocator),
              new BitVector("ge", allocator)
          )
      );
      for (ValueVector v : output) {
        v.allocateNew();
      }

      // evaluate expressions.
      eval.evaluate(batch, output);

      // compare the outputs.
      for (int idx = 0; idx < output.size(); ++idx) {
        boolean[] expectedArray = expected[idx];
        BitVector resultVector = (BitVector) output.get(idx);

        for (int i = 0; i < numRows; i++) {
          assertFalse(resultVector.isNull(i));
          assertEquals("mismatch in result for expr at idx " + idx + " for row " + i,
              expectedArray[i], resultVector.getObject(i).booleanValue());
        }
      }
    } finally {
      // free buffers
      if (batch != null) {
        releaseRecordBatch(batch);
      }
      if (output != null) {
        releaseValueVectors(output);
      }
      eval.close();
    }
  }

  @Test
  public void testRound() throws GandivaException {
    Decimal aType = new Decimal(38, 2);
    Decimal aWithScaleZero = new Decimal(38, 0);
    Decimal aWithScaleOne = new Decimal(38, 1);
    Field a = Field.nullable("a", aType);
    List<Field> args = Lists.newArrayList(a);

    List<ExpressionTree> exprs = new ArrayList<>(
        Arrays.asList(
            TreeBuilder.makeExpression("abs", args, Field.nullable("abs", aType)),
            TreeBuilder.makeExpression("ceil", args, Field.nullable("ceil", aWithScaleZero)),
            TreeBuilder.makeExpression("floor", args, Field.nullable("floor", aWithScaleZero)),
            TreeBuilder.makeExpression("round", args, Field.nullable("round", aWithScaleZero)),
            TreeBuilder.makeExpression("truncate", args, Field.nullable("truncate", aWithScaleZero)),
            TreeBuilder.makeExpression(
                TreeBuilder.makeFunction("round",
                    Lists.newArrayList(TreeBuilder.makeField(a), TreeBuilder.makeLiteral(1)),
                    aWithScaleOne),
                Field.nullable("round_scale_1", aWithScaleOne)),
            TreeBuilder.makeExpression(
                TreeBuilder.makeFunction("truncate",
                    Lists.newArrayList(TreeBuilder.makeField(a), TreeBuilder.makeLiteral(1)),
                    aWithScaleOne),
                Field.nullable("truncate_scale_1", aWithScaleOne))
        )
    );

    Schema schema = new Schema(args);
    Projector eval = Projector.make(schema, exprs);

    List<ValueVector> output = null;
    ArrowRecordBatch batch = null;
    try {
      int numRows = 4;
      String[] aValues = new String[]{"1.23", "1.58", "-1.23", "-1.58"};

      DecimalVector valuesa = decimalVector(aValues, aType.getPrecision(), aType.getScale());
      batch =
          new ArrowRecordBatch(
              numRows,
              Lists.newArrayList(new ArrowFieldNode(numRows, 0)),
              Lists.newArrayList(valuesa.getValidityBuffer(), valuesa.getDataBuffer()));

      // expected results.
      BigDecimal[][] expected = {
          {BigDecimal.valueOf(1.23), BigDecimal.valueOf(1.58),
              BigDecimal.valueOf(1.23), BigDecimal.valueOf(1.58)}, // abs
          {BigDecimal.valueOf(2), BigDecimal.valueOf(2), BigDecimal.valueOf(-1), BigDecimal.valueOf(-1)}, // ceil
          {BigDecimal.valueOf(1), BigDecimal.valueOf(1), BigDecimal.valueOf(-2), BigDecimal.valueOf(-2)}, // floor
          {BigDecimal.valueOf(1), BigDecimal.valueOf(2), BigDecimal.valueOf(-1), BigDecimal.valueOf(-2)}, // round
          {BigDecimal.valueOf(1), BigDecimal.valueOf(1), BigDecimal.valueOf(-1), BigDecimal.valueOf(-1)}, // truncate
          {BigDecimal.valueOf(1.2), BigDecimal.valueOf(1.6),
              BigDecimal.valueOf(-1.2), BigDecimal.valueOf(-1.6)}, // round-to-scale-1
          {BigDecimal.valueOf(1.2), BigDecimal.valueOf(1.5),
              BigDecimal.valueOf(-1.2), BigDecimal.valueOf(-1.5)}, // truncate-to-scale-1
      };

      // Allocate output vectors.
      output = new ArrayList<>(
          Arrays.asList(
              new DecimalVector("abs", allocator, aType.getPrecision(), aType.getScale()),
              new DecimalVector("ceil", allocator, aType.getPrecision(), 0),
              new DecimalVector("floor", allocator, aType.getPrecision(), 0),
              new DecimalVector("round", allocator, aType.getPrecision(), 0),
              new DecimalVector("truncate", allocator, aType.getPrecision(), 0),
              new DecimalVector("round_to_scale_1", allocator, aType.getPrecision(), 1),
              new DecimalVector("truncate_to_scale_1", allocator, aType.getPrecision(), 1)
          )
      );
      for (ValueVector v : output) {
        v.allocateNew();
      }

      // evaluate expressions.
      eval.evaluate(batch, output);

      // compare the outputs.
      for (int idx = 0; idx < output.size(); ++idx) {
        BigDecimal[] expectedArray = expected[idx];
        DecimalVector resultVector = (DecimalVector) output.get(idx);

        for (int i = 0; i < numRows; i++) {
          assertFalse(resultVector.isNull(i));
          assertTrue("mismatch in result for " +
                  "field " + resultVector.getField().getName() +
                  " for row " + i +
                  " expected " + expectedArray[i] +
                  ", got " + resultVector.getObject(i),
              expectedArray[i].compareTo(resultVector.getObject(i)) == 0);
        }
      }
    } finally {
      // free buffers
      if (batch != null) {
        releaseRecordBatch(batch);
      }
      if (output != null) {
        releaseValueVectors(output);
      }
      eval.close();
    }
  }

  @Test
  public void testCastToDecimal() throws GandivaException {
    Decimal decimalType = new Decimal(38, 2);
    Decimal decimalWithScaleOne = new Decimal(38, 1);
    Field dec = Field.nullable("dec", decimalType);
    Field int64f = Field.nullable("int64", int64);
    Field doublef = Field.nullable("float64", float64);

    List<ExpressionTree> exprs = new ArrayList<>(
        Arrays.asList(
            TreeBuilder.makeExpression("castDECIMAL",
                Lists.newArrayList(int64f),
                Field.nullable("int64_to_dec", decimalType)),

            TreeBuilder.makeExpression("castDECIMAL",
                Lists.newArrayList(doublef),
                Field.nullable("float64_to_dec", decimalType)),

            TreeBuilder.makeExpression("castDECIMAL",
                Lists.newArrayList(dec),
                Field.nullable("dec_to_dec", decimalWithScaleOne))
        )
    );

    Schema schema = new Schema(Lists.newArrayList(int64f, doublef, dec));
    Projector eval = Projector.make(schema, exprs);

    List<ValueVector> output = null;
    ArrowRecordBatch batch = null;
    try {
      int numRows = 4;
      String[] aValues = new String[]{"1.23", "1.58", "-1.23", "-1.58"};
      DecimalVector valuesa = decimalVector(aValues, decimalType.getPrecision(), decimalType.getScale());
      batch = new ArrowRecordBatch(
          numRows,
          Lists.newArrayList(
              new ArrowFieldNode(numRows, 0),
              new ArrowFieldNode(numRows, 0),
              new ArrowFieldNode(numRows, 0)),
          Lists.newArrayList(
              arrowBufWithAllValid(4),
              longBuf(new long[]{123, 158, -123, -158}),
              arrowBufWithAllValid(4),
              doubleBuf(new double[]{1.23, 1.58, -1.23, -1.58}),
              valuesa.getValidityBuffer(),
              valuesa.getDataBuffer())
          );

      // Allocate output vectors.
      output = new ArrayList<>(
          Arrays.asList(
              new DecimalVector("int64_to_dec", allocator, decimalType.getPrecision(), decimalType.getScale()),
              new DecimalVector("float64_to_dec", allocator, decimalType.getPrecision(), decimalType.getScale()),
              new DecimalVector("dec_to_dec", allocator,
                  decimalWithScaleOne.getPrecision(), decimalWithScaleOne.getScale())
          )
      );
      for (ValueVector v : output) {
        v.allocateNew();
      }

      // evaluate expressions.
      eval.evaluate(batch, output);

      // compare the outputs.
      BigDecimal[][] expected = {
          { BigDecimal.valueOf(123), BigDecimal.valueOf(158),
              BigDecimal.valueOf(-123), BigDecimal.valueOf(-158)},
          { BigDecimal.valueOf(1.23), BigDecimal.valueOf(1.58),
              BigDecimal.valueOf(-1.23), BigDecimal.valueOf(-1.58)},
          { BigDecimal.valueOf(1.2), BigDecimal.valueOf(1.6),
              BigDecimal.valueOf(-1.2), BigDecimal.valueOf(-1.6)}
      };
      for (int idx = 0; idx < output.size(); ++idx) {
        BigDecimal[] expectedArray = expected[idx];
        DecimalVector resultVector = (DecimalVector) output.get(idx);
        for (int i = 0; i < numRows; i++) {
          assertFalse(resultVector.isNull(i));
          assertTrue("mismatch in result for " +
                  "field " + resultVector.getField().getName() +
                  " for row " + i +
                  " expected " + expectedArray[i] +
                  ", got " + resultVector.getObject(i),
                expectedArray[i].compareTo(resultVector.getObject(i)) == 0);
        }
      }
    } finally {
      // free buffers
      if (batch != null) {
        releaseRecordBatch(batch);
      }
      if (output != null) {
        releaseValueVectors(output);
      }
      eval.close();
    }
  }

  @Test
  public void testCastToLong() throws GandivaException {
    Decimal decimalType = new Decimal(38, 2);
    Field dec = Field.nullable("dec", decimalType);

    Schema schema = new Schema(Lists.newArrayList(dec));
    Projector eval = Projector.make(schema,
        Lists.newArrayList(
            TreeBuilder.makeExpression("castBIGINT",
                Lists.newArrayList(dec),
                Field.nullable("dec_to_int64", int64)
            )
        )
    );

    List<ValueVector> output = null;
    ArrowRecordBatch batch = null;
    try {
      int numRows = 4;
      String[] aValues = new String[]{"1.23", "1.58", "-1.23", "-1.58"};
      DecimalVector valuesa = decimalVector(aValues, decimalType.getPrecision(), decimalType.getScale());
      batch = new ArrowRecordBatch(
          numRows,
          Lists.newArrayList(
              new ArrowFieldNode(numRows, 0)
          ),
          Lists.newArrayList(
              valuesa.getValidityBuffer(),
              valuesa.getDataBuffer()
          )
      );

      // Allocate output vectors.
      BigIntVector resultVector = new BigIntVector("dec_to_int64", allocator);
      resultVector.allocateNew();
      output = new ArrayList<>(Arrays.asList(resultVector));

      // evaluate expressions.
      eval.evaluate(batch, output);

      // compare the outputs.
      long[] expected = {1, 1, -1, -1};
      for (int i = 0; i < numRows; i++) {
        assertFalse(resultVector.isNull(i));
        assertEquals(expected[i], resultVector.get(i));
      }
    } finally {
      // free buffers
      if (batch != null) {
        releaseRecordBatch(batch);
      }
      if (output != null) {
        releaseValueVectors(output);
      }
      eval.close();
    }
  }

  @Test
  public void testCastToDouble() throws GandivaException {
    Decimal decimalType = new Decimal(38, 2);
    Field dec = Field.nullable("dec", decimalType);

    Schema schema = new Schema(Lists.newArrayList(dec));
    Projector eval = Projector.make(schema,
        Lists.newArrayList(
            TreeBuilder.makeExpression("castFLOAT8",
                Lists.newArrayList(dec),
                Field.nullable("dec_to_float64", float64)
            )
        )
    );

    List<ValueVector> output = null;
    ArrowRecordBatch batch = null;
    try {
      int numRows = 4;
      String[] aValues = new String[]{"1.23", "1.58", "-1.23", "-1.58"};
      DecimalVector valuesa = decimalVector(aValues, decimalType.getPrecision(), decimalType.getScale());
      batch = new ArrowRecordBatch(
          numRows,
          Lists.newArrayList(
              new ArrowFieldNode(numRows, 0)
          ),
          Lists.newArrayList(
              valuesa.getValidityBuffer(),
              valuesa.getDataBuffer()
          )
      );

      // Allocate output vectors.
      Float8Vector resultVector = new Float8Vector("dec_to_float64", allocator);
      resultVector.allocateNew();
      output = new ArrayList<>(Arrays.asList(resultVector));

      // evaluate expressions.
      eval.evaluate(batch, output);

      // compare the outputs.
      double[] expected = {1.23, 1.58, -1.23, -1.58};
      for (int i = 0; i < numRows; i++) {
        assertFalse(resultVector.isNull(i));
        assertEquals(expected[i], resultVector.get(i), 0);
      }
    } finally {
      // free buffers
      if (batch != null) {
        releaseRecordBatch(batch);
      }
      if (output != null) {
        releaseValueVectors(output);
      }
      eval.close();
    }
  }
}
