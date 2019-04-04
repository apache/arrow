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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import org.apache.arrow.gandiva.exceptions.GandivaException;
import org.apache.arrow.gandiva.expression.ExpressionTree;
import org.apache.arrow.gandiva.expression.TreeBuilder;
import org.apache.arrow.gandiva.expression.TreeNode;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.Lists;

import io.netty.buffer.ArrowBuf;

public class ProjectorTest extends BaseEvaluatorTest {

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
  public void testMakeProjectorParallel() throws GandivaException, InterruptedException {
    List<Schema> schemas = Lists.newArrayList();
    Field a = Field.nullable("a", int64);
    Field b = Field.nullable("b", int64);
    IntStream.range(0, 1000)
        .forEach(
            i -> {
              Field c = Field.nullable("" + i, int64);
              List<Field> cols = Lists.newArrayList(a, b, c);
              schemas.add(new Schema(cols));
            });

    TreeNode aNode = TreeBuilder.makeField(a);
    TreeNode bNode = TreeBuilder.makeField(b);
    List<TreeNode> args = Lists.newArrayList(aNode, bNode);

    TreeNode cond = TreeBuilder.makeFunction("greater_than", args, boolType);
    TreeNode ifNode = TreeBuilder.makeIf(cond, aNode, bNode, int64);

    ExpressionTree expr = TreeBuilder.makeExpression(ifNode, Field.nullable("c", int64));
    List<ExpressionTree> exprs = Lists.newArrayList(expr);

    // build projectors in parallel choosing schema at random
    // this should hit the same cache entry thus exposing
    // any threading issues.
    ExecutorService executors = Executors.newFixedThreadPool(16);

    IntStream.range(0, 1000)
        .forEach(
            i -> {
              executors.submit(
                  () -> {
                    try {
                      Projector evaluator =
                          Projector.make(schemas.get((int) (Math.random() * 100)), exprs);
                      evaluator.close();
                    } catch (GandivaException e) {
                      e.printStackTrace();
                    }
                  });
            });
    executors.shutdown();
    executors.awaitTermination(100, java.util.concurrent.TimeUnit.SECONDS);
  }

  // Will be fixed by https://issues.apache.org/jira/browse/ARROW-4371
  @Ignore
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

    long startTime = System.currentTimeMillis();
    Projector evaluator1 = Projector.make(schema, exprs);
    System.out.println(
        "Projector build: iteration 1 took " + (System.currentTimeMillis() - startTime) + " ms");
    startTime = System.currentTimeMillis();
    Projector evaluator2 = Projector.make(schema, exprs);
    System.out.println(
        "Projector build: iteration 2 took " + (System.currentTimeMillis() - startTime) + " ms");
    startTime = System.currentTimeMillis();
    Projector evaluator3 = Projector.make(schema, exprs);
    long timeToMakeProjector = (System.currentTimeMillis() - startTime);
    // should be getting the projector from the cache;
    // giving 5ms for varying system load.
    Assert.assertTrue(timeToMakeProjector < 5L);

    evaluator1.close();
    evaluator2.close();
    evaluator3.close();
  }

  @Test
  public void testMakeProjectorValidationError() throws InterruptedException {

    Field a = Field.nullable("a", int64);
    TreeNode aNode = TreeBuilder.makeField(a);
    List<TreeNode> args = Lists.newArrayList(aNode);

    List<Field> cols = Lists.newArrayList(a);
    Schema schema = new Schema(cols);

    TreeNode cond = TreeBuilder.makeFunction("non_existent_fn", args, boolType);

    ExpressionTree expr = TreeBuilder.makeExpression(cond, Field.nullable("c", int64));
    List<ExpressionTree> exprs = Lists.newArrayList(expr);

    boolean exceptionThrown = false;
    try {
      Projector evaluator1 = Projector.make(schema, exprs);
    } catch (GandivaException e) {
      exceptionThrown = true;
    }

    Assert.assertTrue(exceptionThrown);

    // allow GC to collect any temp resources.
    Thread.sleep(1000);

    // try again to ensure no temporary resources.
    exceptionThrown = false;
    try {
      Projector evaluator1 = Projector.make(schema, exprs);
    } catch (GandivaException e) {
      exceptionThrown = true;
    }

    Assert.assertTrue(exceptionThrown);
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
    Projector eval = Projector.make(schema, exprs);

    int numRows = 16;
    byte[] validity = new byte[]{(byte) 255, 0};
    // second half is "undefined"
    int[] aValues = new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};
    int[] bValues = new int[]{16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1};

    ArrowBuf validitya = buf(validity);
    ArrowBuf valuesa = intBuf(aValues);
    ArrowBuf validityb = buf(validity);
    ArrowBuf valuesb = intBuf(bValues);
    ArrowRecordBatch batch =
        new ArrowRecordBatch(
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
  public void testEvaluateDivZero() throws GandivaException, Exception {
    Field a = Field.nullable("a", int32);
    Field b = Field.nullable("b", int32);
    List<Field> args = Lists.newArrayList(a, b);

    Field retType = Field.nullable("c", int32);
    ExpressionTree root = TreeBuilder.makeExpression("divide", args, retType);

    List<ExpressionTree> exprs = Lists.newArrayList(root);

    Schema schema = new Schema(args);
    Projector eval = Projector.make(schema, exprs);

    int numRows = 2;
    byte[] validity = new byte[]{(byte) 255};
    // second half is "undefined"
    int[] aValues = new int[]{2, 2};
    int[] bValues = new int[]{1, 0};

    ArrowBuf validitya = buf(validity);
    ArrowBuf valuesa = intBuf(aValues);
    ArrowBuf validityb = buf(validity);
    ArrowBuf valuesb = intBuf(bValues);
    ArrowRecordBatch batch = new ArrowRecordBatch(
        numRows,
        Lists.newArrayList(new ArrowFieldNode(numRows, 0), new ArrowFieldNode(numRows, 0)),
        Lists.newArrayList(validitya, valuesa, validityb, valuesb));

    IntVector intVector = new IntVector(EMPTY_SCHEMA_PATH, allocator);
    intVector.allocateNew(numRows);

    List<ValueVector> output = new ArrayList<ValueVector>();
    output.add(intVector);
    boolean exceptionThrown = false;
    try {
      eval.evaluate(batch, output);
    } catch (GandivaException e) {
      Assert.assertTrue(e.getMessage().contains("divide by zero"));
      exceptionThrown = true;
    }
    Assert.assertTrue(exceptionThrown);

    // free buffers
    releaseRecordBatch(batch);
    releaseValueVectors(output);
    eval.close();
  }

  @Test
  public void testDivZeroParallel() throws GandivaException, InterruptedException {
    Field a = Field.nullable("a", int32);
    Field b = Field.nullable("b", int32);
    Field c = Field.nullable("c", int32);
    List<Field> cols = Lists.newArrayList(a, b);
    Schema s = new Schema(cols);

    List<Field> args = Lists.newArrayList(a, b);

    ExpressionTree expr = TreeBuilder.makeExpression("divide", args, c);
    List<ExpressionTree> exprs = Lists.newArrayList(expr);

    ExecutorService executors = Executors.newFixedThreadPool(16);

    AtomicInteger errorCount = new AtomicInteger(0);
    AtomicInteger errorCountExp = new AtomicInteger(0);
    // pre-build the projector so that same projector is used for all executions.
    Projector test = Projector.make(s, exprs);

    IntStream.range(0, 1000).forEach(i -> {
      executors.submit(() -> {
        try {
          Projector evaluator = Projector.make(s, exprs);
          int numRows = 2;
          byte[] validity = new byte[]{(byte) 255};
          int[] aValues = new int[]{2, 2};
          int[] bValues;
          if (i % 2 == 0) {
            errorCountExp.incrementAndGet();
            bValues = new int[]{1, 0};
          } else {
            bValues = new int[]{1, 1};
          }

          ArrowBuf validitya = buf(validity);
          ArrowBuf valuesa = intBuf(aValues);
          ArrowBuf validityb = buf(validity);
          ArrowBuf valuesb = intBuf(bValues);
          ArrowRecordBatch batch = new ArrowRecordBatch(
              numRows,
              Lists.newArrayList(new ArrowFieldNode(numRows, 0), new ArrowFieldNode(numRows,
                  0)),
              Lists.newArrayList(validitya, valuesa, validityb, valuesb));

          IntVector intVector = new IntVector(EMPTY_SCHEMA_PATH, allocator);
          intVector.allocateNew(numRows);

          List<ValueVector> output = new ArrayList<ValueVector>();
          output.add(intVector);
          try {
            evaluator.evaluate(batch, output);
          } catch (GandivaException e) {
            errorCount.incrementAndGet();
          }
          // free buffers
          releaseRecordBatch(batch);
          releaseValueVectors(output);
          evaluator.close();
        } catch (GandivaException ignore) {
        }
      });
    });
    executors.shutdown();
    executors.awaitTermination(100, java.util.concurrent.TimeUnit.SECONDS);
    test.close();
    Assert.assertEquals(errorCountExp.intValue(), errorCount.intValue());
  }

  @Test
  public void testAdd3() throws GandivaException, Exception {
    Field x = Field.nullable("x", int32);
    Field n2x = Field.nullable("n2x", int32);
    Field n3x = Field.nullable("n3x", int32);

    List<TreeNode> args = new ArrayList<TreeNode>();

    // x + n2x + n3x
    TreeNode add1 =
        TreeBuilder.makeFunction(
            "add", Lists.newArrayList(TreeBuilder.makeField(x), TreeBuilder.makeField(n2x)), int32);
    TreeNode add =
        TreeBuilder.makeFunction(
            "add", Lists.newArrayList(add1, TreeBuilder.makeField(n3x)), int32);
    ExpressionTree expr = TreeBuilder.makeExpression(add, x);

    List<Field> cols = Lists.newArrayList(x, n2x, n3x);
    Schema schema = new Schema(cols);

    Projector eval = Projector.make(schema, Lists.newArrayList(expr));

    int numRows = 16;
    byte[] validity = new byte[]{(byte) 255, 0};
    // second half is "undefined"
    int[] xValues = new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};
    int[] n2xValues = new int[]{16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1};
    int[] n3xValues = new int[]{1, 2, 3, 4, 4, 3, 2, 1, 5, 6, 7, 8, 8, 7, 6, 5};

    int[] expected = new int[]{18, 19, 20, 21, 21, 20, 19, 18, 18, 19, 20, 21, 21, 20, 19, 18};

    ArrowBuf xValidity = buf(validity);
    ArrowBuf xData = intBuf(xValues);
    ArrowBuf n2xValidity = buf(validity);
    ArrowBuf n2xData = intBuf(n2xValues);
    ArrowBuf n3xValidity = buf(validity);
    ArrowBuf n3xData = intBuf(n3xValues);

    ArrowFieldNode fieldNode = new ArrowFieldNode(numRows, 8);
    ArrowRecordBatch batch =
        new ArrowRecordBatch(
            numRows,
            Lists.newArrayList(fieldNode, fieldNode, fieldNode),
            Lists.newArrayList(xValidity, xData, n2xValidity, n2xData, n3xValidity, n3xData));

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

    TreeNode cond =
        TreeBuilder.makeFunction(
            "less_than",
            Lists.newArrayList(TreeBuilder.makeField(x), TreeBuilder.makeStringLiteral("hello")),
            boolType);
    TreeNode octetLenFuncNode =
        TreeBuilder.makeFunction(
            "octet_length", Lists.newArrayList(TreeBuilder.makeField(x)), retType);
    TreeNode octetLenPlusANode =
        TreeBuilder.makeFunction(
            "add", Lists.newArrayList(TreeBuilder.makeField(a), octetLenFuncNode), retType);
    TreeNode octetLenPlusBNode =
        TreeBuilder.makeFunction(
            "add", Lists.newArrayList(TreeBuilder.makeField(b), octetLenFuncNode), retType);

    TreeNode ifHello = TreeBuilder.makeIf(cond, octetLenPlusANode, octetLenPlusBNode, retType);

    ExpressionTree expr = TreeBuilder.makeExpression(ifHello, Field.nullable("res", retType));
    Schema schema = new Schema(Lists.newArrayList(a, x, b));
    Projector eval = Projector.make(schema, Lists.newArrayList(expr));

    int numRows = 5;
    byte[] validity = new byte[]{(byte) 255, 0};
    // "A função" means "The function" in portugese
    String[] valuesX = new String[]{"hell", "abc", "hellox", "ijk", "A função"};
    int[] valuesA = new int[]{10, 20, 30, 40, 50};
    int[] valuesB = new int[]{110, 120, 130, 140, 150};
    int[] expected = new int[]{14, 23, 136, 143, 60};

    ArrowBuf validityX = buf(validity);
    List<ArrowBuf> dataBufsX = stringBufs(valuesX);
    ArrowBuf validityA = buf(validity);
    ArrowBuf dataA = intBuf(valuesA);
    ArrowBuf validityB = buf(validity);
    ArrowBuf dataB = intBuf(valuesB);

    ArrowRecordBatch batch =
        new ArrowRecordBatch(
            numRows,
            Lists.newArrayList(new ArrowFieldNode(numRows, 0), new ArrowFieldNode(numRows, 0)),
            Lists.newArrayList(
                validityA, dataA, validityX, dataBufsX.get(0), dataBufsX.get(1), validityB, dataB));

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
  public void testRegex() throws GandivaException {
    /*
     * like "%map%"
     */

    Field x = Field.nullable("x", new ArrowType.Utf8());

    TreeNode cond =
        TreeBuilder.makeFunction(
            "like",
            Lists.newArrayList(TreeBuilder.makeField(x), TreeBuilder.makeStringLiteral("%map%")),
            boolType);
    ExpressionTree expr = TreeBuilder.makeExpression(cond, Field.nullable("res", boolType));
    Schema schema = new Schema(Lists.newArrayList(x));
    Projector eval = Projector.make(schema, Lists.newArrayList(expr));

    int numRows = 5;
    byte[] validity = new byte[]{(byte) 255, 0};
    String[] valuesX = new String[]{"mapD", "maps", "google maps", "map", "MapR"};
    boolean[] expected = new boolean[]{true, true, true, true, false};

    ArrowBuf validityX = buf(validity);
    List<ArrowBuf> dataBufsX = stringBufs(valuesX);

    ArrowRecordBatch batch =
        new ArrowRecordBatch(
            numRows,
            Lists.newArrayList(new ArrowFieldNode(numRows, 0)),
            Lists.newArrayList(validityX, dataBufsX.get(0), dataBufsX.get(1)));

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
  public void testBinaryFields() throws GandivaException {
    Field a = Field.nullable("a", new ArrowType.Binary());
    Field b = Field.nullable("b", new ArrowType.Binary());
    List<Field> args = Lists.newArrayList(a, b);

    ArrowType retType = new ArrowType.Bool();
    ExpressionTree expr = TreeBuilder.makeExpression("equal", args, Field.nullable("res", retType));

    Schema schema = new Schema(Lists.newArrayList(args));
    Projector eval = Projector.make(schema, Lists.newArrayList(expr));

    int numRows = 5;
    byte[] validity = new byte[]{(byte) 255, 0};
    String[] valuesA = new String[]{"a", "aa", "aaa", "aaaa", "A função"};
    String[] valuesB = new String[]{"a", "bb", "aaa", "bbbbb", "A função"};
    boolean[] expected = new boolean[]{true, false, true, false, true};

    ArrowBuf validitya = buf(validity);
    ArrowBuf validityb = buf(validity);
    List<ArrowBuf> inBufsA = binaryBufs(valuesA);
    List<ArrowBuf> inBufsB = binaryBufs(valuesB);

    ArrowRecordBatch batch =
        new ArrowRecordBatch(
            numRows,
            Lists.newArrayList(new ArrowFieldNode(numRows, 8), new ArrowFieldNode(numRows, 8)),
            Lists.newArrayList(
                validitya,
                inBufsA.get(0),
                inBufsA.get(1),
                validityb,
                inBufsB.get(0),
                inBufsB.get(1)));

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
    return TreeBuilder.makeFunction(
        "less_than", Lists.newArrayList(arg, TreeBuilder.makeLiteral(value)), boolType);
  }

  private TreeNode makeLongGreaterThanCond(TreeNode arg, long value) {
    return TreeBuilder.makeFunction(
        "greater_than", Lists.newArrayList(arg, TreeBuilder.makeLiteral(value)), boolType);
  }

  private TreeNode ifLongLessThanElse(
      TreeNode arg, long value, long thenValue, TreeNode elseNode, ArrowType type) {
    return TreeBuilder.makeIf(
        makeLongLessThanCond(arg, value), TreeBuilder.makeLiteral(thenValue), elseNode, type);
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
    TreeNode xNode = TreeBuilder.makeField(x);

    // if (x < 100) then 9 else 10
    TreeNode ifLess100 = ifLongLessThanElse(xNode, 100L, 9L, TreeBuilder.makeLiteral(10L), int64);
    // if (x < 90) then 8 else ifLess100
    TreeNode ifLess90 = ifLongLessThanElse(xNode, 90L, 8L, ifLess100, int64);
    // if (x < 80) then 7 else ifLess90
    TreeNode ifLess80 = ifLongLessThanElse(xNode, 80L, 7L, ifLess90, int64);
    // if (x < 70) then 6 else ifLess80
    TreeNode ifLess70 = ifLongLessThanElse(xNode, 70L, 6L, ifLess80, int64);
    // if (x < 60) then 5 else ifLess70
    TreeNode ifLess60 = ifLongLessThanElse(xNode, 60L, 5L, ifLess70, int64);
    // if (x < 50) then 4 else ifLess60
    TreeNode ifLess50 = ifLongLessThanElse(xNode, 50L, 4L, ifLess60, int64);
    // if (x < 40) then 3 else ifLess50
    TreeNode ifLess40 = ifLongLessThanElse(xNode, 40L, 3L, ifLess50, int64);
    // if (x < 30) then 2 else ifLess40
    TreeNode ifLess30 = ifLongLessThanElse(xNode, 30L, 2L, ifLess40, int64);
    // if (x < 20) then 1 else ifLess30
    TreeNode ifLess20 = ifLongLessThanElse(xNode, 20L, 1L, ifLess30, int64);
    // if (x < 10) then 0 else ifLess20
    TreeNode ifLess10 = ifLongLessThanElse(xNode, 10L, 0L, ifLess20, int64);

    ExpressionTree expr = TreeBuilder.makeExpression(ifLess10, x);
    Schema schema = new Schema(Lists.newArrayList(x));
    Projector eval = Projector.make(schema, Lists.newArrayList(expr));

    int numRows = 16;
    byte[] validity = new byte[]{(byte) 255, (byte) 255};
    long[] xValues = new long[]{9, 15, 21, 32, 43, 54, 65, 76, 87, 98, 109, 200, -10, 60, 77, 80};
    long[] expected = new long[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 10, 0, 6, 7, 8};

    ArrowBuf bufValidity = buf(validity);
    ArrowBuf xData = longBuf(xValues);

    ArrowFieldNode fieldNode = new ArrowFieldNode(numRows, 0);
    ArrowRecordBatch batch =
        new ArrowRecordBatch(
            numRows, Lists.newArrayList(fieldNode), Lists.newArrayList(bufValidity, xData));

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
    TreeNode xNode = TreeBuilder.makeField(x);
    TreeNode gt10 = makeLongGreaterThanCond(xNode, 10);
    TreeNode lt20 = makeLongLessThanCond(xNode, 20);
    TreeNode and = TreeBuilder.makeAnd(Lists.newArrayList(gt10, lt20));

    Field res = Field.nullable("res", boolType);

    ExpressionTree expr = TreeBuilder.makeExpression(and, res);
    Schema schema = new Schema(Lists.newArrayList(x));
    Projector eval = Projector.make(schema, Lists.newArrayList(expr));

    int numRows = 4;
    byte[] validity = new byte[]{(byte) 255};
    long[] xValues = new long[]{9, 15, 17, 25};
    boolean[] expected = new boolean[]{false, true, true, false};

    ArrowBuf bufValidity = buf(validity);
    ArrowBuf xData = longBuf(xValues);

    ArrowFieldNode fieldNode = new ArrowFieldNode(numRows, 0);
    ArrowRecordBatch batch =
        new ArrowRecordBatch(
            numRows, Lists.newArrayList(fieldNode), Lists.newArrayList(bufValidity, xData));

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
    TreeNode xNode = TreeBuilder.makeField(x);
    TreeNode gt10 = makeLongGreaterThanCond(xNode, 10);
    TreeNode lt5 = makeLongLessThanCond(xNode, 5);
    TreeNode or = TreeBuilder.makeOr(Lists.newArrayList(gt10, lt5));

    Field res = Field.nullable("res", boolType);

    ExpressionTree expr = TreeBuilder.makeExpression(or, res);
    Schema schema = new Schema(Lists.newArrayList(x));
    Projector eval = Projector.make(schema, Lists.newArrayList(expr));

    int numRows = 4;
    byte[] validity = new byte[]{(byte) 255};
    long[] xValues = new long[]{4, 9, 15, 17};
    boolean[] expected = new boolean[]{true, false, true, true};

    ArrowBuf bufValidity = buf(validity);
    ArrowBuf xData = longBuf(xValues);

    ArrowFieldNode fieldNode = new ArrowFieldNode(numRows, 0);
    ArrowRecordBatch batch =
        new ArrowRecordBatch(
            numRows, Lists.newArrayList(fieldNode), Lists.newArrayList(bufValidity, xData));

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
    TreeNode xNode = TreeBuilder.makeField(x);

    // if (x < 10) then 1 else null
    TreeNode ifLess10 = ifLongLessThanElse(xNode, 10L, 1L, TreeBuilder.makeNull(int64), int64);

    ExpressionTree expr = TreeBuilder.makeExpression(ifLess10, x);
    Schema schema = new Schema(Lists.newArrayList(x));
    Projector eval = Projector.make(schema, Lists.newArrayList(expr));

    int numRows = 2;
    byte[] validity = new byte[]{(byte) 255};
    long[] xValues = new long[]{5, 32};
    long[] expected = new long[]{1, 0};

    ArrowBuf bufValidity = buf(validity);
    ArrowBuf xData = longBuf(xValues);

    ArrowFieldNode fieldNode = new ArrowFieldNode(numRows, 0);
    ArrowRecordBatch batch =
        new ArrowRecordBatch(
            numRows, Lists.newArrayList(fieldNode), Lists.newArrayList(bufValidity, xData));

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
    TreeNode xNode = TreeBuilder.makeNull(time64);

    ExpressionTree expr = TreeBuilder.makeExpression(xNode, x);
    Schema schema = new Schema(Lists.newArrayList(x));
    Projector eval = Projector.make(schema, Lists.newArrayList(expr));

    int numRows = 2;
    byte[] validity = new byte[]{(byte) 255};
    int[] xValues = new int[]{5, 32};

    ArrowBuf bufValidity = buf(validity);
    ArrowBuf xData = intBuf(xValues);

    ArrowFieldNode fieldNode = new ArrowFieldNode(numRows, 0);
    ArrowRecordBatch batch =
        new ArrowRecordBatch(
            numRows, Lists.newArrayList(fieldNode), Lists.newArrayList(bufValidity, xData));

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
  public void testTimeEquals() throws GandivaException, Exception {    /*
   * when isnotnull(x) then x
   * else y
   */
    Field x = Field.nullable("x", new ArrowType.Time(TimeUnit.MILLISECOND, 32));
    TreeNode xNode = TreeBuilder.makeField(x);

    Field y = Field.nullable("y", new ArrowType.Time(TimeUnit.MILLISECOND, 32));
    TreeNode yNode = TreeBuilder.makeField(y);

    // if isnotnull(x) then x else y
    TreeNode condition = TreeBuilder.makeFunction("isnotnull", Lists.newArrayList(xNode),
        boolType);
    TreeNode ifCoalesce = TreeBuilder.makeIf(
        condition,
        xNode,
        yNode,
        new ArrowType.Time(TimeUnit.MILLISECOND, 32));

    ExpressionTree expr = TreeBuilder.makeExpression(ifCoalesce, x);
    Schema schema = new Schema(Lists.newArrayList(x, y));
    Projector eval = Projector.make(schema, Lists.newArrayList(expr));

    int numRows = 2;
    byte[] validity = new byte[]{(byte) 1};
    byte[] yValidity = new byte[]{(byte) 3};
    int[] xValues = new int[]{5, 1};
    int[] yValues = new int[]{10, 2};
    int[] expected = new int[]{5, 2};

    ArrowBuf bufValidity = buf(validity);
    ArrowBuf xData = intBuf(xValues);

    ArrowBuf yBufValidity = buf(yValidity);
    ArrowBuf yData = intBuf(yValues);

    ArrowFieldNode fieldNode = new ArrowFieldNode(numRows, 0);
    ArrowRecordBatch batch = new ArrowRecordBatch(
        numRows,
        Lists.newArrayList(fieldNode),
        Lists.newArrayList(bufValidity, xData, yBufValidity, yData));

    IntVector intVector = new IntVector(EMPTY_SCHEMA_PATH, allocator);
    intVector.allocateNew(numRows);

    List<ValueVector> output = new ArrayList<ValueVector>();
    output.add(intVector);
    eval.evaluate(batch, output);

    // output should be 5 and 2
    assertFalse(intVector.isNull(0));
    assertEquals(expected[0], intVector.get(0));
    assertEquals(expected[1], intVector.get(1));

    releaseRecordBatch(batch);
    releaseValueVectors(output);
    eval.close();
  }

  @Test
  public void testIsNull() throws GandivaException, Exception {
    Field x = Field.nullable("x", float64);

    TreeNode xNode = TreeBuilder.makeField(x);
    TreeNode isNull = TreeBuilder.makeFunction("isnull", Lists.newArrayList(xNode), boolType);
    ExpressionTree expr = TreeBuilder.makeExpression(isNull, Field.nullable("result", boolType));
    Schema schema = new Schema(Lists.newArrayList(x));
    Projector eval = Projector.make(schema, Lists.newArrayList(expr));

    int numRows = 16;
    byte[] validity = new byte[]{(byte) 255, 0};
    double[] xValues =
        new double[]{
            1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0
        };

    ArrowBuf bufValidity = buf(validity);
    ArrowBuf xData = doubleBuf(xValues);

    ArrowFieldNode fieldNode = new ArrowFieldNode(numRows, 0);
    ArrowRecordBatch batch =
        new ArrowRecordBatch(
            numRows, Lists.newArrayList(fieldNode), Lists.newArrayList(bufValidity, xData));

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

    TreeNode c1Node = TreeBuilder.makeField(c1);
    TreeNode c2Node = TreeBuilder.makeField(c2);
    TreeNode equals =
        TreeBuilder.makeFunction("equal", Lists.newArrayList(c1Node, c2Node), boolType);
    ExpressionTree expr = TreeBuilder.makeExpression(equals, Field.nullable("result", boolType));
    Schema schema = new Schema(Lists.newArrayList(c1, c2));
    Projector eval = Projector.make(schema, Lists.newArrayList(expr));

    int numRows = 16;
    byte[] validity = new byte[]{(byte) 255, 0};
    int[] c1Values = new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};
    int[] c2Values = new int[]{1, 2, 3, 4, 8, 7, 6, 5, 16, 15, 14, 13, 12, 11, 10, 9};

    ArrowBuf c1Validity = buf(validity);
    ArrowBuf c1Data = intBuf(c1Values);
    ArrowBuf c2Validity = buf(validity);
    ArrowBuf c2Data = intBuf(c2Values);

    ArrowFieldNode fieldNode = new ArrowFieldNode(numRows, 0);
    ArrowRecordBatch batch =
        new ArrowRecordBatch(
            numRows,
            Lists.newArrayList(fieldNode, fieldNode),
            Lists.newArrayList(c1Validity, c1Data, c2Validity, c2Data));

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
    Projector eval = Projector.make(schema, exprs);

    int numRows = 16;
    byte[] validity = new byte[]{(byte) 255, 0};
    // second half is "undefined"
    int[] aValues = new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};
    int[] bValues = new int[]{16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1};

    ArrowBuf aValidity = buf(validity);
    ArrowBuf aData = intBuf(aValues);
    ArrowBuf bValidity = buf(validity);
    ArrowBuf bData = intBuf(bValues);
    ArrowRecordBatch batch =
        new ArrowRecordBatch(
            numRows,
            Lists.newArrayList(new ArrowFieldNode(numRows, 8), new ArrowFieldNode(numRows, 8)),
            Lists.newArrayList(aValidity, aData, bValidity, bData));

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
    // ArrowType time32 = new ArrowType.Time(TimeUnit.MILLISECOND, 32);
    ArrowType timeStamp = new ArrowType.Timestamp(TimeUnit.MILLISECOND, "TZ");

    Field dateField = Field.nullable("date", date64);
    // Field timeField = Field.nullable("time", time32);
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
    List<ExpressionTree> exprs =
        Lists.newArrayList(
            TreeBuilder.makeExpression(dateToYear, resultField),
            TreeBuilder.makeExpression(dateToMonth, resultField),
            TreeBuilder.makeExpression(dateToDay, resultField),
            TreeBuilder.makeExpression(dateToHour, resultField),
            TreeBuilder.makeExpression(dateToMin, resultField),
            TreeBuilder.makeExpression(tsToYear, resultField),
            TreeBuilder.makeExpression(tsToMonth, resultField),
            TreeBuilder.makeExpression(tsToDay, resultField),
            TreeBuilder.makeExpression(tsToHour, resultField),
            TreeBuilder.makeExpression(tsToMin, resultField));

    Schema schema = new Schema(Lists.newArrayList(dateField, tsField));
    Projector eval = Projector.make(schema, exprs);

    int numRows = 8;
    byte[] validity = new byte[]{(byte) 255};
    String[] values =
        new String[]{
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

    long[][] expValues =
        new long[][]{
            expYearFromDate, expMonthFromDate, expDayFromDate, expHourFromDate, expMinFromDate
        };

    ArrowBuf bufValidity = buf(validity);
    ArrowBuf millisData = stringToMillis(values);
    ArrowBuf buf2Validity = buf(validity);
    ArrowBuf millis2Data = stringToMillis(values);

    ArrowFieldNode fieldNode = new ArrowFieldNode(numRows, 0);
    ArrowRecordBatch batch =
        new ArrowRecordBatch(
            numRows,
            Lists.newArrayList(fieldNode, fieldNode),
            Lists.newArrayList(bufValidity, millisData, buf2Validity, millis2Data));

    List<ValueVector> output = new ArrayList<ValueVector>();
    for (int i = 0; i < exprs.size(); i++) {
      BigIntVector bigIntVector = new BigIntVector(EMPTY_SCHEMA_PATH, allocator);
      bigIntVector.allocateNew(numRows);
      output.add(bigIntVector);
    }
    eval.evaluate(batch, output);
    eval.close();

    for (int i = 0; i < output.size(); i++) {
      long[] expected = expValues[i % 5];
      BigIntVector bigIntVector = (BigIntVector) output.get(i);

      for (int j = 0; j < numRows; j++) {
        assertFalse(bigIntVector.isNull(j));
        assertEquals(expected[j], bigIntVector.get(j));
      }
    }

    releaseRecordBatch(batch);
    releaseValueVectors(output);
  }

  @Test
  public void testUnknownFunction() {
    Field c1 = Field.nullable("c1", int8);
    Field c2 = Field.nullable("c2", int8);

    TreeNode c1Node = TreeBuilder.makeField(c1);
    TreeNode c2Node = TreeBuilder.makeField(c2);

    TreeNode unknown =
        TreeBuilder.makeFunction("xxx_yyy", Lists.newArrayList(c1Node, c2Node), int8);
    ExpressionTree expr = TreeBuilder.makeExpression(unknown, Field.nullable("result", int8));
    Schema schema = new Schema(Lists.newArrayList(c1, c2));
    boolean caughtException = false;
    try {
      Projector eval = Projector.make(schema, Lists.newArrayList(expr));
    } catch (GandivaException ge) {
      caughtException = true;
    }

    assertTrue(caughtException);
  }
}
