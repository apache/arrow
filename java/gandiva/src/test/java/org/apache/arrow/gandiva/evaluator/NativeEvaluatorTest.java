/*
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

package org.apache.arrow.gandiva.evaluator;

import com.google.common.collect.Lists;
import io.netty.buffer.ArrowBuf;
import org.apache.arrow.gandiva.exceptions.GandivaException;
import org.apache.arrow.gandiva.expression.ExpressionTree;
import org.apache.arrow.gandiva.expression.TreeBuilder;
import org.apache.arrow.gandiva.expression.TreeNode;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class NativeEvaluatorTest {

  private final static String EMPTY_SCHEMA_PATH = "";

  private BufferAllocator allocator;
  private ArrowType boolType;

  @Before
  public void init() {
    allocator = new RootAllocator(Long.MAX_VALUE);
    boolType = new ArrowType.Bool();
  }

  ArrowBuf buf(byte[] bytes) {
    ArrowBuf buffer = allocator.buffer(bytes.length);
    buffer.writeBytes(bytes);
    return buffer;
  }

  ArrowBuf intBuf(int[] ints) {
    ArrowBuf buffer = allocator.buffer(ints.length * 4);
    for (int i = 0; i < ints.length; i++) {
      buffer.writeInt(ints[i]);
    }
    return buffer;
  }

  ArrowBuf longBuf(long[] longs) {
    ArrowBuf buffer = allocator.buffer(longs.length * 8);
    for (int i = 0; i < longs.length; i++) {
      buffer.writeLong(longs[i]);
    }
    return buffer;
  }

  ArrowBuf doubleBuf(double[] data) {
    ArrowBuf buffer = allocator.buffer(data.length * 8);
    for (int i = 0; i < data.length; i++) {
      buffer.writeDouble(data[i]);
    }

    return buffer;
  }

  @Test
  public void testMakeProjector() throws GandivaException {
    Field a = Field.nullable("a", new ArrowType.Int(64, true));
    Field b = Field.nullable("b", new ArrowType.Int(64, true));
    TreeNode aNode = TreeBuilder.makeField(a);
    TreeNode bNode = TreeBuilder.makeField(b);
    List<TreeNode> args = Lists.newArrayList(aNode, bNode);

    List<Field> cols = Lists.newArrayList(a, b);
    Schema schema = new Schema(cols);

    ArrowType retType = new ArrowType.Int(64, true);
    TreeNode cond = TreeBuilder.makeFunction("greater_than", args, boolType);
    TreeNode ifNode = TreeBuilder.makeIf(cond, aNode, bNode, retType);

    ExpressionTree expr = TreeBuilder.makeExpression(ifNode, Field.nullable("c", retType));
    List<ExpressionTree> exprs = Lists.newArrayList(expr);

    NativeEvaluator evaluator1 = NativeEvaluator.makeProjector(schema, exprs);
    NativeEvaluator evaluator2 = NativeEvaluator.makeProjector(schema, exprs);
    NativeEvaluator evaluator3 = NativeEvaluator.makeProjector(schema, exprs);

    evaluator1.close();
    evaluator2.close();
    evaluator3.close();
  }

  @Test
  public void testEvaluate() throws GandivaException, Exception {
    Field a = Field.nullable("a", new ArrowType.Int(32, true));
    Field b = Field.nullable("b", new ArrowType.Int(32, true));
    List<Field> args = Lists.newArrayList(a, b);

    Field retType = Field.nullable("c", new ArrowType.Int(32, true));
    ExpressionTree root = TreeBuilder.makeExpression("add", args, retType);

    List<ExpressionTree> exprs = Lists.newArrayList(root);

    Schema schema = new Schema(args);
    NativeEvaluator eval = NativeEvaluator.makeProjector(schema, exprs);

    int numRows = 16;
    byte[] validity = new byte[]{(byte) 255, 0};
    // second half is "undefined"
    int[] values_a = new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};
    int[] values_b = new int[]{16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1};

    ArrowBuf validitya = buf(validity);
    ArrowBuf valuesa = intBuf(values_a);
    ArrowBuf validityb = buf(validity);
    ArrowBuf valuesb = intBuf(values_b);
    ArrowRecordBatch batch = new ArrowRecordBatch(
            numRows,
            Lists.newArrayList(new ArrowFieldNode(numRows, 8), new ArrowFieldNode(numRows, 8)),
            Lists.newArrayList(validitya, valuesa, validityb, valuesb));

    IntVector intVector = new IntVector(EMPTY_SCHEMA_PATH, allocator);
    intVector.allocateNew(numRows);

    List<ValueVector> output = new ArrayList<ValueVector>();
    output.add(intVector);
    eval.evaluate(batch, output);

    for (int i = 0; i < 8; i++) {
      assertFalse(intVector.isNull(i));
      assertEquals(17, intVector.get(i));
    }
    for (int i = 8; i < 16; i++) {
      assertTrue(intVector.isNull(i));
    }
    eval.close();
  }

  @Test
  public void testAdd3() throws GandivaException, Exception {
    ArrowType int32 = new ArrowType.Int(32, true);

    Field x = Field.nullable("x", int32);
    Field N2x = Field.nullable("N2x", int32);
    Field N3x = Field.nullable("N3x", int32);

    List<TreeNode> args = new ArrayList<TreeNode>();

    // x + N2x + N3x
    TreeNode add1 = TreeBuilder.makeFunction("add", Lists.newArrayList(TreeBuilder.makeField(x), TreeBuilder.makeField(N2x)), int32);
    TreeNode add = TreeBuilder.makeFunction("add", Lists.newArrayList(add1, TreeBuilder.makeField(N3x)), int32);
    ExpressionTree expr = TreeBuilder.makeExpression(add, x);

    List<Field> cols = Lists.newArrayList(x, N2x, N3x);
    Schema schema = new Schema(cols);

    NativeEvaluator eval = NativeEvaluator.makeProjector(schema, Lists.newArrayList(expr));

    int numRows = 16;
    byte[] validity = new byte[]{(byte) 255, 0};
    // second half is "undefined"
    int[] values_x = new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};
    int[] values_N2x = new int[]{16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1};
    int[] values_N3x = new int[]{1, 2, 3, 4, 4, 3, 2, 1, 5, 6, 7, 8, 8, 7, 6, 5};

    int[] expected = new int[]{18, 19, 20, 21, 21, 20, 19, 18, 18, 19, 20, 21, 21, 20, 19, 18};

    ArrowBuf validity_buf = buf(validity);
    ArrowBuf data_x = intBuf(values_x);
    ArrowBuf data_N2x = intBuf(values_N2x);
    ArrowBuf data_N3x = intBuf(values_N3x);

    ArrowFieldNode fieldNode = new ArrowFieldNode(numRows, 8);
    ArrowRecordBatch batch = new ArrowRecordBatch(
            numRows,
            Lists.newArrayList(fieldNode, fieldNode, fieldNode),
            Lists.newArrayList(validity_buf, data_x, validity_buf, data_N2x, validity_buf, data_N3x));

    IntVector intVector = new IntVector(EMPTY_SCHEMA_PATH, allocator);
    intVector.allocateNew(numRows);

    List<ValueVector> output = new ArrayList<ValueVector>();
    output.add(intVector);
    eval.evaluate(batch, output);

    for (int i = 0; i < 8; i++) {
      assertFalse(intVector.isNull(i));
      assertEquals(expected[i], intVector.get(i));
    }
    for (int i = 8; i < 16; i++) {
      assertTrue(intVector.isNull(i));
    }
    eval.close();
  }

  private TreeNode makeLongLessThanCond(TreeNode arg, long value, ArrowType type) {
    return TreeBuilder.makeFunction("less_than",
            Lists.newArrayList(arg, TreeBuilder.makeLiteral(value)),
            type);
  }

  private TreeNode ifLongLessThanElse(TreeNode arg, long value, long then_value, TreeNode elseNode, ArrowType type) {
    return TreeBuilder.makeIf(
            makeLongLessThanCond(arg, value, boolType),
            TreeBuilder.makeLiteral(then_value),
            elseNode,
            type);
  }

  @Test
  public void testIf() throws GandivaException, Exception {
    /*
     * when x < 10 then 0
     * when x < 20 then 1
     * when x < 30 then 2
     * when x < 40 then 3
     * when x < 50 then 4
     * when x < 60 then 5
     * when x < 70 then 6
     * when x < 80 then 7
     * when x < 90 then 8
     * when x < 100 then 9
     * else 10
     */
    ArrowType int64 = new ArrowType.Int(64, true);

    Field x = Field.nullable("x", int64);
    TreeNode x_node = TreeBuilder.makeField(x);

    // if (x < 100) then 9 else 10
    TreeNode ifLess100 = ifLongLessThanElse(x_node, 100L, 9L, TreeBuilder.makeLiteral(10L), int64);
    // if (x < 90) then 8 else ifLess100
    TreeNode ifLess90 = ifLongLessThanElse(x_node, 90L, 8L, ifLess100, int64);
    // if (x < 80) then 7 else ifLess90
    TreeNode ifLess80 = ifLongLessThanElse(x_node, 80L, 7L, ifLess90, int64);
    // if (x < 70) then 6 else ifLess80
    TreeNode ifLess70 = ifLongLessThanElse(x_node, 70L, 6L, ifLess80, int64);
    // if (x < 60) then 5 else ifLess70
    TreeNode ifLess60 = ifLongLessThanElse(x_node, 60L, 5L, ifLess70, int64);
    // if (x < 50) then 4 else ifLess60
    TreeNode ifLess50 = ifLongLessThanElse(x_node, 50L, 4L, ifLess60, int64);
    // if (x < 40) then 3 else ifLess50
    TreeNode ifLess40 = ifLongLessThanElse(x_node, 40L, 3L, ifLess50, int64);
    // if (x < 30) then 2 else ifLess40
    TreeNode ifLess30 = ifLongLessThanElse(x_node, 30L, 2L, ifLess40, int64);
    // if (x < 20) then 1 else ifLess30
    TreeNode ifLess20 = ifLongLessThanElse(x_node, 20L, 1L, ifLess30, int64);
    // if (x < 10) then 0 else ifLess20
    TreeNode ifLess10 = ifLongLessThanElse(x_node, 10L, 0L, ifLess20, int64);

    ExpressionTree expr = TreeBuilder.makeExpression(ifLess10, x);
    Schema schema = new Schema(Lists.newArrayList(x));
    NativeEvaluator eval = NativeEvaluator.makeProjector(schema, Lists.newArrayList(expr));

    int numRows = 16;
    byte[] validity = new byte[]{(byte) 255, (byte) 255};
    long[] values_x = new long[]{9, 15, 21, 32, 43, 54, 65, 76, 87, 98, 109, 200, -10, 60, 77, 80};
    long[] expected = new long[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 10, 0, 6, 7, 8};

    ArrowBuf validity_buf = buf(validity);
    ArrowBuf data_x = longBuf(values_x);

    ArrowFieldNode fieldNode = new ArrowFieldNode(numRows, 0);
    ArrowRecordBatch batch = new ArrowRecordBatch(
            numRows,
            Lists.newArrayList(fieldNode),
            Lists.newArrayList(validity_buf, data_x));

    BigIntVector bigIntVector = new BigIntVector(EMPTY_SCHEMA_PATH, allocator);
    bigIntVector.allocateNew(numRows);

    List<ValueVector> output = new ArrayList<ValueVector>();
    output.add(bigIntVector);
    eval.evaluate(batch, output);

    for (int i = 0; i < numRows; i++) {
      assertFalse(bigIntVector.isNull(i));
      assertEquals(expected[i], bigIntVector.get(i));
    }

    eval.close();
  }

  @Test
  public void testIsNull() throws GandivaException, Exception {
    ArrowType float64 = new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
    Field x = Field.nullable("x", float64);

    TreeNode x_node = TreeBuilder.makeField(x);
    TreeNode isNull = TreeBuilder.makeFunction("isnull", Lists.newArrayList(x_node), boolType);
    ExpressionTree expr = TreeBuilder.makeExpression(isNull, Field.nullable("result", boolType));
    Schema schema = new Schema(Lists.newArrayList(x));
    NativeEvaluator eval = NativeEvaluator.makeProjector(schema, Lists.newArrayList(expr));

    int numRows = 16;
    byte[] validity = new byte[]{(byte) 255, 0};
    double[] values_x = new double[]{1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0};

    ArrowBuf validity_buf = buf(validity);
    ArrowBuf data_x = doubleBuf(values_x);

    ArrowFieldNode fieldNode = new ArrowFieldNode(numRows, 0);
    ArrowRecordBatch batch = new ArrowRecordBatch(
            numRows,
            Lists.newArrayList(fieldNode),
            Lists.newArrayList(validity_buf, data_x));

    BitVector bitVector = new BitVector(EMPTY_SCHEMA_PATH, allocator);
    bitVector.allocateNew(numRows);

    List<ValueVector> output = new ArrayList<ValueVector>();
    output.add(bitVector);
    eval.evaluate(batch, output);

    for (int i = 0; i < 8; i++) {
      assertFalse(bitVector.getObject(i).booleanValue());
    }
    for (int i = 8; i < numRows; i++) {
      assertTrue(bitVector.getObject(i).booleanValue());
    }

    eval.close();
  }

  @Test
  public void testEquals() throws GandivaException, Exception {
    ArrowType int32 = new ArrowType.Int(32, true);
    Field c1 = Field.nullable("c1", int32);
    Field c2 = Field.nullable("c2", int32);

    TreeNode c1_Node = TreeBuilder.makeField(c1);
    TreeNode c2_Node = TreeBuilder.makeField(c2);
    TreeNode equals = TreeBuilder.makeFunction("equal", Lists.newArrayList(c1_Node, c2_Node), boolType);
    ExpressionTree expr = TreeBuilder.makeExpression(equals, Field.nullable("result", boolType));
    Schema schema = new Schema(Lists.newArrayList(c1, c2));
    NativeEvaluator eval = NativeEvaluator.makeProjector(schema, Lists.newArrayList(expr));

    int numRows = 16;
    byte[] validity = new byte[]{(byte) 255, 0};
    int[] values_c1 = new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};
    int[] values_c2 = new int[]{1, 2, 3, 4, 8, 7, 6, 5, 16, 15, 14, 13, 12, 11, 10, 9};

    ArrowBuf validity_buf = buf(validity);
    ArrowBuf data_c1 = intBuf(values_c1);
    ArrowBuf data_c2 = intBuf(values_c2);

    ArrowFieldNode fieldNode = new ArrowFieldNode(numRows, 0);
    ArrowRecordBatch batch = new ArrowRecordBatch(
            numRows,
            Lists.newArrayList(fieldNode, fieldNode),
            Lists.newArrayList(validity_buf, data_c1, validity_buf, data_c2));

    BitVector bitVector = new BitVector(EMPTY_SCHEMA_PATH, allocator);
    bitVector.allocateNew(numRows);

    List<ValueVector> output = new ArrayList<ValueVector>();
    output.add(bitVector);
    eval.evaluate(batch, output);

    for (int i = 0; i < 4; i++) {
      assertTrue(bitVector.getObject(i).booleanValue());
    }
    for (int i = 4; i < 8; i++) {
      assertFalse(bitVector.getObject(i).booleanValue());
    }
    for (int i = 8; i < 16; i++) {
      assertTrue(bitVector.isNull(i));
    }

    eval.close();
  }

  @Test
  public void testSmallOutputVectors() throws GandivaException, Exception {
    ArrowType int32 = new ArrowType.Int(32, true);
    Field a = Field.nullable("a", int32);
    Field b = Field.nullable("b", int32);
    List<Field> args = Lists.newArrayList(a, b);

    Field retType = Field.nullable("c", int32);
    ExpressionTree root = TreeBuilder.makeExpression("add", args, retType);

    List<ExpressionTree> exprs = Lists.newArrayList(root);

    Schema schema = new Schema(args);
    NativeEvaluator eval = NativeEvaluator.makeProjector(schema, exprs);

    int numRows = 16;
    byte[] validity = new byte[]{(byte) 255, 0};
    // second half is "undefined"
    int[] values_a = new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};
    int[] values_b = new int[]{16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1};

    ArrowBuf validity_a = buf(validity);
    ArrowBuf data_a = intBuf(values_a);
    ArrowBuf validity_b = buf(validity);
    ArrowBuf data_b = intBuf(values_b);
    ArrowRecordBatch batch = new ArrowRecordBatch(
            numRows,
            Lists.newArrayList(new ArrowFieldNode(numRows, 8), new ArrowFieldNode(numRows, 8)),
            Lists.newArrayList(validity_a, data_a, validity_b, data_b));

    IntVector intVector = new IntVector(EMPTY_SCHEMA_PATH, allocator);

    List<ValueVector> output = new ArrayList<ValueVector>();
    output.add(intVector);
    try {
      eval.evaluate(batch, output);
    } catch (Throwable t) {
      intVector.allocateNew(numRows);
      eval.evaluate(batch, output);
    }

    for (int i = 0; i < 8; i++) {
      assertFalse(intVector.isNull(i));
      assertEquals(17, intVector.get(i));
    }
    for (int i = 8; i < 16; i++) {
      assertTrue(intVector.isNull(i));
    }
    eval.close();
  }

  // This test is ignored until the cpp layer handles errors gracefully
  @Ignore
  @Test
  public void testUnknownFunction() {
    ArrowType int8 = new ArrowType.Int(8, true);
    Field c1 = Field.nullable("c1", int8);
    Field c2 = Field.nullable("c2", int8);

    TreeNode c1_Node = TreeBuilder.makeField(c1);
    TreeNode c2_Node = TreeBuilder.makeField(c2);

    TreeNode unknown = TreeBuilder.makeFunction("xxx_yyy", Lists.newArrayList(c1_Node, c2_Node), int8);
    ExpressionTree expr = TreeBuilder.makeExpression(unknown, Field.nullable("result", int8));
    Schema schema = new Schema(Lists.newArrayList(c1, c2));
    boolean caughtException = false;
    try {
      NativeEvaluator eval = NativeEvaluator.makeProjector(schema, Lists.newArrayList(expr));
    } catch (GandivaException ge) {
      caughtException = true;
    }

    assertTrue(caughtException);
  }
}
