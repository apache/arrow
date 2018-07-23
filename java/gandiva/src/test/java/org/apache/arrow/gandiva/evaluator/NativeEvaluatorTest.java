/*
 * Copyright (C) 2017-2018 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.arrow.gandiva.ipc.GandivaTypes;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import org.joda.time.Instant;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class NativeEvaluatorTest extends BaseNativeEvaluatorTest {
  private Charset utf8Charset = Charset.forName("UTF-8");
  private Charset utf16Charset = Charset.forName("UTF-16");

  List<ArrowBuf> varBufs(String[] strings, Charset charset) {
    ArrowBuf offsetsBuffer = allocator.buffer((strings.length + 1) * 4);
    ArrowBuf dataBuffer = allocator.buffer(strings.length * 8);

    int startOffset = 0;
    for (int i = 0; i < strings.length; i++) {
      offsetsBuffer.writeInt(startOffset);

      final byte[] bytes = strings[i].getBytes(charset);
      dataBuffer = dataBuffer.reallocIfNeeded(dataBuffer.writerIndex() + bytes.length);
      dataBuffer.setBytes(startOffset, bytes, 0, bytes.length);
      startOffset += bytes.length;
    }
    offsetsBuffer.writeInt(startOffset); // offset for the last element

    return Arrays.asList(offsetsBuffer, dataBuffer);
  }

  List<ArrowBuf> stringBufs(String[] strings) {
    return varBufs(strings, utf8Charset);
  }

  List<ArrowBuf> binaryBufs(String[] strings) {
    return varBufs(strings, utf16Charset);
  }

  @Test
  public void testMakeProjector() throws GandivaException {
    Field a = Field.nullable("a", int64);
    Field b = Field.nullable("b", int64);
    TreeNode aNode = TreeBuilder.makeField(a);
    TreeNode bNode = TreeBuilder.makeField(b);
    List<TreeNode> args = Lists.newArrayList(aNode, bNode);

    List<Field> cols = Lists.newArrayList(a, b);
    Schema schema = new Schema(cols);

    TreeNode cond = TreeBuilder.makeFunction("greater_than", args, boolType);
    TreeNode ifNode = TreeBuilder.makeIf(cond, aNode, bNode, int64);

    ExpressionTree expr = TreeBuilder.makeExpression(ifNode, Field.nullable("c", int64));
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

    // free buffers
    releaseRecordBatch(batch);
    releaseValueVectors(output);
    eval.close();
  }

  @Test
  public void testAdd3() throws GandivaException, Exception {
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

    ArrowBuf validity_x = buf(validity);
    ArrowBuf data_x = intBuf(values_x);
    ArrowBuf validity_N2x = buf(validity);
    ArrowBuf data_N2x = intBuf(values_N2x);
    ArrowBuf validity_N3x = buf(validity);
    ArrowBuf data_N3x = intBuf(values_N3x);

    ArrowFieldNode fieldNode = new ArrowFieldNode(numRows, 8);
    ArrowRecordBatch batch = new ArrowRecordBatch(
            numRows,
            Lists.newArrayList(fieldNode, fieldNode, fieldNode),
            Lists.newArrayList(validity_x, data_x, validity_N2x, data_N2x, validity_N3x, data_N3x));

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

    releaseRecordBatch(batch);
    releaseValueVectors(output);
    eval.close();
  }

  @Test
  public void testStringFields() throws GandivaException {
    /*
     * when x < "hello" then octet_length(x) + a
     * else octet_length(x) + b
     */

    Field x = Field.nullable("x", new ArrowType.Utf8());
    Field a = Field.nullable("a", new ArrowType.Int(32, true));
    Field b = Field.nullable("b", new ArrowType.Int(32, true));

    ArrowType retType = new ArrowType.Int(32, true);

    TreeNode cond = TreeBuilder.makeFunction("less_than",
      Lists.newArrayList(TreeBuilder.makeField(x), TreeBuilder.makeStringLiteral("hello")),
      boolType);
    TreeNode octetLenFuncNode = TreeBuilder.makeFunction("octet_length",
      Lists.newArrayList(TreeBuilder.makeField(x)),
      retType);
    TreeNode octetLenPlusANode = TreeBuilder.makeFunction("add",
      Lists.newArrayList(TreeBuilder.makeField(a), octetLenFuncNode),
      retType);
    TreeNode octetLenPlusBNode = TreeBuilder.makeFunction("add",
      Lists.newArrayList(TreeBuilder.makeField(b), octetLenFuncNode),
      retType);

    TreeNode ifHello = TreeBuilder.makeIf(cond, octetLenPlusANode, octetLenPlusBNode, retType);

    ExpressionTree expr = TreeBuilder.makeExpression(ifHello, Field.nullable("res", retType));
    Schema schema = new Schema(Lists.newArrayList(a, x, b));
    NativeEvaluator eval = NativeEvaluator.makeProjector(schema, Lists.newArrayList(expr));

    int numRows = 5;
    byte[] validity = new byte[]{(byte) 255, 0};
    // "A função" means "The function" in portugese
    String[] valuesX = new String[]{"hell", "abc", "hellox", "ijk", "A função" };
    int[] valuesA = new int[]{10, 20, 30, 40, 50};
    int[] valuesB = new int[]{110, 120, 130, 140, 150};
    int[] expected = new int[]{14, 23, 136, 143, 60};

    ArrowBuf validityX = buf(validity);
    List<ArrowBuf> dataBufsX = stringBufs(valuesX);
    ArrowBuf validityA = buf(validity);
    ArrowBuf dataA = intBuf(valuesA);
    ArrowBuf validityB = buf(validity);
    ArrowBuf dataB = intBuf(valuesB);

    ArrowRecordBatch batch = new ArrowRecordBatch(
            numRows,
            Lists.newArrayList(new ArrowFieldNode(numRows, 0), new ArrowFieldNode(numRows, 0)),
            Lists.newArrayList(validityA, dataA, validityX, dataBufsX.get(0), dataBufsX.get(1), validityB, dataB));

    IntVector intVector = new IntVector(EMPTY_SCHEMA_PATH, allocator);
    intVector.allocateNew(numRows);

    List<ValueVector> output = new ArrayList<ValueVector>();
    output.add(intVector);
    eval.evaluate(batch, output);

    for (int i = 0; i < numRows; i++) {
      assertFalse(intVector.isNull(i));
      assertEquals(expected[i], intVector.get(i));
    }

    releaseRecordBatch(batch);
    releaseValueVectors(output);
    eval.close();
  }

  @Test
  public void testBinaryFields() throws GandivaException {
    Field a = Field.nullable("a", new ArrowType.Binary());
    Field b = Field.nullable("b", new ArrowType.Binary());
    List<Field> args = Lists.newArrayList(a, b);

    ArrowType retType = new ArrowType.Bool();
    ExpressionTree expr = TreeBuilder.makeExpression("equal", args,
      Field.nullable("res", retType));

    Schema schema = new Schema(Lists.newArrayList(args));
    NativeEvaluator eval = NativeEvaluator.makeProjector(schema, Lists.newArrayList(expr));

    int numRows = 5;
    byte[] validity = new byte[]{(byte) 255, 0};
    String[] valuesA = new String[]{"a", "aa", "aaa", "aaaa", "A função"};
    String[] valuesB = new String[]{"a", "bb", "aaa", "bbbbb", "A função"};
    boolean[] expected = new boolean[]{true, false, true, false, true};

    ArrowBuf validitya = buf(validity);
    ArrowBuf validityb = buf(validity);
    List<ArrowBuf> inBufsA = binaryBufs(valuesA);
    List<ArrowBuf> inBufsB = binaryBufs(valuesB);

    ArrowRecordBatch batch = new ArrowRecordBatch(
            numRows,
            Lists.newArrayList(new ArrowFieldNode(numRows, 8), new ArrowFieldNode(numRows, 8)),
            Lists.newArrayList(validitya, inBufsA.get(0), inBufsA.get(1), validityb, inBufsB.get(0), inBufsB.get(1)));

    BitVector bitVector = new BitVector(EMPTY_SCHEMA_PATH, allocator);
    bitVector.allocateNew(numRows);

    List<ValueVector> output = new ArrayList<ValueVector>();
    output.add(bitVector);
    eval.evaluate(batch, output);

    for (int i = 0; i < numRows; i++) {
      assertFalse(bitVector.isNull(i));
      assertEquals(expected[i], bitVector.getObject(i).booleanValue());
    }

    releaseRecordBatch(batch);
    releaseValueVectors(output);
    eval.close();
  }

  private TreeNode makeLongLessThanCond(TreeNode arg, long value) {
    return TreeBuilder.makeFunction("less_than",
      Lists.newArrayList(arg, TreeBuilder.makeLiteral(value)),
      boolType);
  }

  private TreeNode makeLongGreaterThanCond(TreeNode arg, long value) {
    return TreeBuilder.makeFunction("greater_than",
      Lists.newArrayList(arg, TreeBuilder.makeLiteral(value)),
      boolType);
  }

  private TreeNode ifLongLessThanElse(TreeNode arg, long value, long then_value, TreeNode elseNode, ArrowType type) {
    return TreeBuilder.makeIf(
            makeLongLessThanCond(arg, value),
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

    releaseRecordBatch(batch);
    releaseValueVectors(output);
    eval.close();
  }

  @Test
  public void testAnd() throws GandivaException, Exception {
    /*
     * x > 10 AND x < 20
     */
    ArrowType int64 = new ArrowType.Int(64, true);

    Field x = Field.nullable("x", int64);
    TreeNode x_node = TreeBuilder.makeField(x);
    TreeNode gt10 = makeLongGreaterThanCond(x_node, 10);
    TreeNode lt20 = makeLongLessThanCond(x_node, 20);
    TreeNode and = TreeBuilder.makeAnd(Lists.newArrayList(gt10, lt20));

    Field res = Field.nullable("res", boolType);

    ExpressionTree expr = TreeBuilder.makeExpression(and, res);
    Schema schema = new Schema(Lists.newArrayList(x));
    NativeEvaluator eval = NativeEvaluator.makeProjector(schema, Lists.newArrayList(expr));

    int numRows = 4;
    byte[] validity = new byte[]{(byte) 255};
    long[] values_x = new long[]{9, 15, 17, 25};
    boolean[] expected = new boolean[]{false, true, true, false};

    ArrowBuf validity_buf = buf(validity);
    ArrowBuf data_x = longBuf(values_x);

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

    for (int i = 0; i < numRows; i++) {
      assertFalse(bitVector.isNull(i));
      assertEquals(expected[i], bitVector.getObject(i).booleanValue());
    }

    releaseRecordBatch(batch);
    releaseValueVectors(output);
    eval.close();
  }

  @Test
  public void testOr() throws GandivaException, Exception {
    /*
     * x > 10 OR x < 5
     */
    ArrowType int64 = new ArrowType.Int(64, true);

    Field x = Field.nullable("x", int64);
    TreeNode x_node = TreeBuilder.makeField(x);
    TreeNode gt10 = makeLongGreaterThanCond(x_node, 10);
    TreeNode lt5 = makeLongLessThanCond(x_node, 5);
    TreeNode or = TreeBuilder.makeOr(Lists.newArrayList(gt10, lt5));

    Field res = Field.nullable("res", boolType);

    ExpressionTree expr = TreeBuilder.makeExpression(or, res);
    Schema schema = new Schema(Lists.newArrayList(x));
    NativeEvaluator eval = NativeEvaluator.makeProjector(schema, Lists.newArrayList(expr));

    int numRows = 4;
    byte[] validity = new byte[]{(byte) 255};
    long[] values_x = new long[]{4, 9, 15, 17};
    boolean[] expected = new boolean[]{true, false, true, true};

    ArrowBuf validity_buf = buf(validity);
    ArrowBuf data_x = longBuf(values_x);

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

    for (int i = 0; i < numRows; i++) {
      assertFalse(bitVector.isNull(i));
      assertEquals(expected[i], bitVector.getObject(i).booleanValue());
    }

    releaseRecordBatch(batch);
    releaseValueVectors(output);
    eval.close();
  }

  @Test
  public void testNull() throws GandivaException, Exception {
    /*
     * when x < 10 then 1
     * else null
     */
    ArrowType int64 = new ArrowType.Int(64, true);

    Field x = Field.nullable("x", int64);
    TreeNode x_node = TreeBuilder.makeField(x);

    // if (x < 10) then 1 else null
    TreeNode ifLess10 = ifLongLessThanElse(x_node, 10L, 1L, TreeBuilder.makeNull(int64), int64);

    ExpressionTree expr = TreeBuilder.makeExpression(ifLess10, x);
    Schema schema = new Schema(Lists.newArrayList(x));
    NativeEvaluator eval = NativeEvaluator.makeProjector(schema, Lists.newArrayList(expr));

    int numRows = 2;
    byte[] validity = new byte[]{(byte) 255};
    long[] values_x = new long[]{5, 32};
    long[] expected = new long[]{1, 0};

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

    // first element should be 1
    assertFalse(bigIntVector.isNull(0));
    assertEquals(expected[0], bigIntVector.get(0));

    // second element should be null
    assertTrue(bigIntVector.isNull(1));

    releaseRecordBatch(batch);
    releaseValueVectors(output);
    eval.close();
  }

  @Test
  public void testTimeNull() throws GandivaException, Exception {

    ArrowType time64 = new ArrowType.Time(TimeUnit.MICROSECOND, 64);

    Field x = Field.nullable("x", time64);
    TreeNode x_node = TreeBuilder.makeNull(time64);

    ExpressionTree expr = TreeBuilder.makeExpression(x_node, x);
    Schema schema = new Schema(Lists.newArrayList(x));
    NativeEvaluator eval = NativeEvaluator.makeProjector(schema, Lists.newArrayList(expr));

    int numRows = 2;
    byte[] validity = new byte[]{(byte) 255};
    int[] values_x = new int[]{5, 32};

    ArrowBuf validity_buf = buf(validity);
    ArrowBuf data_x = intBuf(values_x);

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

    assertTrue(bigIntVector.isNull(0));
    assertTrue(bigIntVector.isNull(1));

    releaseRecordBatch(batch);
    releaseValueVectors(output);
    eval.close();
  }

  @Test
  public void testIsNull() throws GandivaException, Exception {
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

    releaseRecordBatch(batch);
    releaseValueVectors(output);
    eval.close();
  }

  @Test
  public void testEquals() throws GandivaException, Exception {
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

    ArrowBuf validity_c1 = buf(validity);
    ArrowBuf data_c1 = intBuf(values_c1);
    ArrowBuf validity_c2 = buf(validity);
    ArrowBuf data_c2 = intBuf(values_c2);

    ArrowFieldNode fieldNode = new ArrowFieldNode(numRows, 0);
    ArrowRecordBatch batch = new ArrowRecordBatch(
            numRows,
            Lists.newArrayList(fieldNode, fieldNode),
            Lists.newArrayList(validity_c1, data_c1, validity_c2, data_c2));

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

    releaseRecordBatch(batch);
    releaseValueVectors(output);
    eval.close();
  }

  @Test
  public void testSmallOutputVectors() throws GandivaException, Exception {
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

    releaseRecordBatch(batch);
    releaseValueVectors(output);
    eval.close();
  }

  @Test
  public void testDateTime() throws GandivaException, Exception {
    ArrowType date64 = new ArrowType.Date(DateUnit.MILLISECOND);
    //ArrowType time32 = new ArrowType.Time(TimeUnit.MILLISECOND, 32);
    ArrowType timeStamp = new ArrowType.Timestamp(TimeUnit.MILLISECOND, "TZ");

    Field dateField = Field.nullable("date", date64);
    //Field timeField = Field.nullable("time", time32);
    Field tsField = Field.nullable("timestamp", timeStamp);

    TreeNode dateNode = TreeBuilder.makeField(dateField);
    TreeNode tsNode = TreeBuilder.makeField(tsField);

    List<TreeNode> dateArgs = Lists.newArrayList(dateNode);
    TreeNode dateToYear = TreeBuilder.makeFunction("extractYear", dateArgs, int64);
    TreeNode dateToMonth = TreeBuilder.makeFunction("extractMonth", dateArgs, int64);
    TreeNode dateToDay = TreeBuilder.makeFunction("extractDay", dateArgs, int64);
    TreeNode dateToHour = TreeBuilder.makeFunction("extractHour", dateArgs, int64);
    TreeNode dateToMin = TreeBuilder.makeFunction("extractMinute", dateArgs, int64);

    List<TreeNode> tsArgs = Lists.newArrayList(tsNode);
    TreeNode tsToYear = TreeBuilder.makeFunction("extractYear", tsArgs, int64);
    TreeNode tsToMonth = TreeBuilder.makeFunction("extractMonth", tsArgs, int64);
    TreeNode tsToDay = TreeBuilder.makeFunction("extractDay", tsArgs, int64);
    TreeNode tsToHour = TreeBuilder.makeFunction("extractHour", tsArgs, int64);
    TreeNode tsToMin = TreeBuilder.makeFunction("extractMinute", tsArgs, int64);

    Field resultField = Field.nullable("result", int64);
    List<ExpressionTree> exprs = Lists.newArrayList(
            TreeBuilder.makeExpression(dateToYear, resultField),
            TreeBuilder.makeExpression(dateToMonth, resultField),
            TreeBuilder.makeExpression(dateToDay, resultField),
            TreeBuilder.makeExpression(dateToHour, resultField),
            TreeBuilder.makeExpression(dateToMin, resultField),
            TreeBuilder.makeExpression(tsToYear, resultField),
            TreeBuilder.makeExpression(tsToMonth, resultField),
            TreeBuilder.makeExpression(tsToDay, resultField),
            TreeBuilder.makeExpression(tsToHour, resultField),
            TreeBuilder.makeExpression(tsToMin, resultField)
    );

    Schema schema = new Schema(Lists.newArrayList(dateField, tsField));
    NativeEvaluator eval = NativeEvaluator.makeProjector(schema, exprs);

    int numRows = 8;
    byte[] validity = new byte[]{(byte) 255};
    String[] values = new String[]{
            "2007-01-01T01:00:00.00Z",
            "2007-03-05T03:40:00.00Z",
            "2008-05-31T13:55:00.00Z",
            "2000-06-30T23:20:00.00Z",
            "2000-07-10T20:30:00.00Z",
            "2000-08-20T00:14:00.00Z",
            "2000-09-30T02:29:00.00Z",
            "2000-10-31T05:33:00.00Z"
    };
    long[] expYearFromDate = new long[]{2007, 2007, 2008, 2000, 2000, 2000, 2000, 2000};
    long[] expMonthFromDate = new long[]{1, 3, 5, 6, 7, 8, 9, 10};
    long[] expDayFromDate = new long[]{1, 5, 31, 30, 10, 20, 30, 31};
    long[] expHourFromDate = new long[]{1, 3, 13, 23, 20, 0, 2, 5};
    long[] expMinFromDate = new long[]{0, 40, 55, 20, 30, 14, 29, 33};

    long[][] expValues = new long[][]{
            expYearFromDate,
            expMonthFromDate,
            expDayFromDate,
            expHourFromDate,
            expMinFromDate
    };

    ArrowBuf validity_buf = buf(validity);
    ArrowBuf data_millis = stringToMillis(values);
    ArrowBuf validity_buf2 = buf(validity);
    ArrowBuf data_millis2 = stringToMillis(values);

    ArrowFieldNode fieldNode = new ArrowFieldNode(numRows, 0);
    ArrowRecordBatch batch = new ArrowRecordBatch(
            numRows,
            Lists.newArrayList(fieldNode, fieldNode),
            Lists.newArrayList(validity_buf, data_millis, validity_buf2, data_millis2));

    List<ValueVector> output = new ArrayList<ValueVector>();
    for(int i = 0; i < exprs.size(); i++) {
      BigIntVector bigIntVector = new BigIntVector(EMPTY_SCHEMA_PATH, allocator);
      bigIntVector.allocateNew(numRows);
      output.add(bigIntVector);
    }
    eval.evaluate(batch, output);
    eval.close();

    for(int i = 0; i < output.size(); i++) {
      long[] expected = expValues[i % 5];
      BigIntVector bigIntVector = (BigIntVector)output.get(i);

      for (int j = 0; j < numRows; j++) {
        assertFalse(bigIntVector.isNull(j));
        assertEquals(expected[j], bigIntVector.get(j));
      }
    }

    releaseRecordBatch(batch);
    releaseValueVectors(output);
  }

  // This test is ignored until the cpp layer handles errors gracefully
  @Ignore
  @Test
  public void testUnknownFunction() {
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
