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


import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import org.apache.arrow.gandiva.exceptions.GandivaException;
import org.apache.arrow.gandiva.expression.ExpressionTree;
import org.apache.arrow.gandiva.expression.TreeBuilder;
import org.apache.arrow.gandiva.expression.TreeNode;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.ArrowType;
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
}
