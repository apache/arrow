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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import org.apache.arrow.gandiva.exceptions.GandivaException;
import org.apache.arrow.gandiva.expression.ExpressionTree;
import org.apache.arrow.gandiva.expression.TreeBuilder;
import org.apache.arrow.gandiva.expression.TreeNode;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.IntervalDayVector;
import org.apache.arrow.vector.IntervalYearVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.holders.NullableIntervalDayHolder;
import org.apache.arrow.vector.holders.NullableIntervalYearHolder;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.IntervalUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class ProjectorTest extends BaseEvaluatorTest {

  private Charset utf8Charset = Charset.forName("UTF-8");
  private Charset utf16Charset = Charset.forName("UTF-16");

  List<ArrowBuf> varBufs(String[] strings, Charset charset) {
    ArrowBuf offsetsBuffer = allocator.buffer((strings.length + 1) * 4);

    long dataBufferSize = 0L;
    for (String string : strings) {
      dataBufferSize += string.getBytes(charset).length;
    }

    ArrowBuf dataBuffer = allocator.buffer(dataBufferSize);

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

  private void testMakeProjectorParallel(ConfigurationBuilder.ConfigOptions configOptions)
      throws InterruptedException {
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
                          configOptions == null
                              ? Projector.make(schemas.get((int) (Math.random() * 100)), exprs)
                              : Projector.make(
                                  schemas.get((int) (Math.random() * 100)), exprs, configOptions);
                      evaluator.close();
                    } catch (GandivaException e) {
                      e.printStackTrace();
                    }
                  });
            });
    executors.shutdown();
    executors.awaitTermination(100, java.util.concurrent.TimeUnit.SECONDS);
  }

  @Test
  public void testMakeProjectorParallel() throws Exception {
    testMakeProjectorParallel(null);
    testMakeProjectorParallel(new ConfigurationBuilder.ConfigOptions().withTargetCPU(false));
    testMakeProjectorParallel(
        new ConfigurationBuilder.ConfigOptions().withTargetCPU(false).withOptimize(false));
  }

  // Will be fixed by https://issues.apache.org/jira/browse/ARROW-4371
  @Disabled
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
    assertTrue(timeToMakeProjector < 5L);

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

    assertTrue(exceptionThrown);

    // allow GC to collect any temp resources.
    Thread.sleep(1000);

    // try again to ensure no temporary resources.
    exceptionThrown = false;
    try {
      Projector evaluator1 = Projector.make(schema, exprs);
    } catch (GandivaException e) {
      exceptionThrown = true;
    }

    assertTrue(exceptionThrown);
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
    byte[] validity = new byte[] {(byte) 255, 0};
    // second half is "undefined"
    int[] aValues = new int[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};
    int[] bValues = new int[] {16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1};

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
    byte[] validity = new byte[] {(byte) 255};
    // second half is "undefined"
    int[] aValues = new int[] {2, 2};
    int[] bValues = new int[] {1, 0};

    ArrowBuf validitya = buf(validity);
    ArrowBuf valuesa = intBuf(aValues);
    ArrowBuf validityb = buf(validity);
    ArrowBuf valuesb = intBuf(bValues);
    ArrowRecordBatch batch =
        new ArrowRecordBatch(
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
      assertTrue(e.getMessage().contains("divide by zero"));
      exceptionThrown = true;
    }
    assertTrue(exceptionThrown);

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

    IntStream.range(0, 1000)
        .forEach(
            i -> {
              executors.submit(
                  () -> {
                    try {
                      Projector evaluator = Projector.make(s, exprs);
                      int numRows = 2;
                      byte[] validity = new byte[] {(byte) 255};
                      int[] aValues = new int[] {2, 2};
                      int[] bValues;
                      if (i % 2 == 0) {
                        errorCountExp.incrementAndGet();
                        bValues = new int[] {1, 0};
                      } else {
                        bValues = new int[] {1, 1};
                      }

                      ArrowBuf validitya = buf(validity);
                      ArrowBuf valuesa = intBuf(aValues);
                      ArrowBuf validityb = buf(validity);
                      ArrowBuf valuesb = intBuf(bValues);
                      ArrowRecordBatch batch =
                          new ArrowRecordBatch(
                              numRows,
                              Lists.newArrayList(
                                  new ArrowFieldNode(numRows, 0), new ArrowFieldNode(numRows, 0)),
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
    assertEquals(errorCountExp.intValue(), errorCount.intValue());
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
    byte[] validity = new byte[] {(byte) 255, 0};
    // second half is "undefined"
    int[] xValues = new int[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};
    int[] n2xValues = new int[] {16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1};
    int[] n3xValues = new int[] {1, 2, 3, 4, 4, 3, 2, 1, 5, 6, 7, 8, 8, 7, 6, 5};

    int[] expected = new int[] {18, 19, 20, 21, 21, 20, 19, 18, 18, 19, 20, 21, 21, 20, 19, 18};

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
    byte[] validity = new byte[] {(byte) 255, 0};
    // "A função" means "The function" in portugese
    String[] valuesX = new String[] {"hell", "abc", "hellox", "ijk", "A função"};
    int[] valuesA = new int[] {10, 20, 30, 40, 50};
    int[] valuesB = new int[] {110, 120, 130, 140, 150};
    int[] expected = new int[] {14, 23, 136, 143, 60};

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
  public void testStringOutput() throws GandivaException {
    /*
     * if (x >= 0) "hi" else "bye"
     */

    Field x = Field.nullable("x", new ArrowType.Int(32, true));

    ArrowType retType = new ArrowType.Utf8();

    TreeNode ifHiBye =
        TreeBuilder.makeIf(
            TreeBuilder.makeFunction(
                "greater_than_or_equal_to",
                Lists.newArrayList(TreeBuilder.makeField(x), TreeBuilder.makeLiteral(0)),
                boolType),
            TreeBuilder.makeStringLiteral("hi"),
            TreeBuilder.makeStringLiteral("bye"),
            retType);

    ExpressionTree expr = TreeBuilder.makeExpression(ifHiBye, Field.nullable("res", retType));
    Schema schema = new Schema(Lists.newArrayList(x));
    Projector eval = Projector.make(schema, Lists.newArrayList(expr));

    // fill up input record batch
    int numRows = 4;
    byte[] validity = new byte[] {(byte) 255, 0};
    int[] xValues = new int[] {10, -10, 20, -20};
    String[] expected = new String[] {"hi", "bye", "hi", "bye"};
    ArrowBuf validityX = buf(validity);
    ArrowBuf dataX = intBuf(xValues);
    ArrowRecordBatch batch =
        new ArrowRecordBatch(
            numRows,
            Lists.newArrayList(new ArrowFieldNode(numRows, 0)),
            Lists.newArrayList(validityX, dataX));

    // allocate data for output vector.
    VarCharVector outVector = new VarCharVector(EMPTY_SCHEMA_PATH, allocator);
    outVector.allocateNew(64, numRows);

    // evaluate expression
    List<ValueVector> output = new ArrayList<>();
    output.add(outVector);
    eval.evaluate(batch, output);

    // match expected output.
    for (int i = 0; i < numRows; i++) {
      assertFalse(outVector.isNull(i));
      assertEquals(expected[i], new String(outVector.get(i)));
    }

    // test with insufficient data buffer.
    try {
      outVector.allocateNew(4, numRows);
      eval.evaluate(batch, output);
    } finally {
      releaseRecordBatch(batch);
      releaseValueVectors(output);
      eval.close();
    }
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
    byte[] validity = new byte[] {(byte) 255, 0};
    String[] valuesX = new String[] {"mapD", "maps", "google maps", "map", "MapR"};
    boolean[] expected = new boolean[] {true, true, true, true, false};

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
  public void testRegexpReplace() throws GandivaException {

    Field x = Field.nullable("x", new ArrowType.Utf8());
    Field replaceString = Field.nullable("replaceString", new ArrowType.Utf8());

    Field retType = Field.nullable("c", new ArrowType.Utf8());

    TreeNode cond =
        TreeBuilder.makeFunction(
            "regexp_replace",
            Lists.newArrayList(
                TreeBuilder.makeField(x),
                TreeBuilder.makeStringLiteral("ana"),
                TreeBuilder.makeField(replaceString)),
            new ArrowType.Utf8());
    ExpressionTree expr = TreeBuilder.makeExpression(cond, retType);
    Schema schema = new Schema(Lists.newArrayList(x, replaceString));
    Projector eval = Projector.make(schema, Lists.newArrayList(expr));

    int numRows = 5;
    byte[] validity = new byte[] {(byte) 15, 0};
    String[] valuesX = new String[] {"banana", "bananaana", "bananana", "anaana", "anaana"};
    String[] valuesReplace = new String[] {"ue", "", "", "c", ""};
    String[] expected = new String[] {"buena", "bna", "bn", "cc", null};

    ArrowBuf validityX = buf(validity);
    ArrowBuf validityReplace = buf(validity);
    List<ArrowBuf> dataBufsX = stringBufs(valuesX);
    List<ArrowBuf> dataBufsReplace = stringBufs(valuesReplace);

    ArrowFieldNode fieldNode = new ArrowFieldNode(numRows, 0);

    ArrowRecordBatch batch =
        new ArrowRecordBatch(
            numRows,
            Lists.newArrayList(fieldNode, fieldNode),
            Lists.newArrayList(
                validityX,
                dataBufsX.get(0),
                dataBufsX.get(1),
                validityReplace,
                dataBufsReplace.get(0),
                dataBufsReplace.get(1)));

    // allocate data for output vector.
    VarCharVector outVector = new VarCharVector(EMPTY_SCHEMA_PATH, allocator);
    outVector.allocateNew(numRows * 15, numRows);

    // evaluate expression
    List<ValueVector> output = new ArrayList<>();
    output.add(outVector);
    eval.evaluate(batch, output);
    eval.close();

    // match expected output.
    for (int i = 0; i < numRows - 1; i++) {
      assertFalse(outVector.isNull(i), "Expect none value equals null");
      assertEquals(expected[i], new String(outVector.get(i)));
    }

    assertTrue(outVector.isNull(numRows - 1), "Last value must be null");

    releaseRecordBatch(batch);
    releaseValueVectors(output);
  }

  @Test
  public void testCastIntervalDay() throws GandivaException {

    Field x = Field.nullable("x", new ArrowType.Utf8());

    Field retType = Field.nullable("c", new ArrowType.Interval(IntervalUnit.DAY_TIME));

    TreeNode cond =
        TreeBuilder.makeFunction(
            "castintervalday",
            Lists.newArrayList(TreeBuilder.makeField(x)),
            new ArrowType.Interval(IntervalUnit.DAY_TIME));
    ExpressionTree expr = TreeBuilder.makeExpression(cond, retType);
    Schema schema = new Schema(Lists.newArrayList(x));
    Projector eval = Projector.make(schema, Lists.newArrayList(expr));

    int numRows = 4;
    byte[] validity = new byte[] {(byte) 7, 0};
    String[] valuesX = new String[] {"1742461111", "P1Y1M1DT1H1M1S", "PT48H1M1S", "test"};
    int[][] expected =
        new int[][] { // day and millis
          {20, 14461111}, {1, 3661000}, {2, 61000}, null
        };

    ArrowBuf validityX = buf(validity);
    List<ArrowBuf> dataBufsX = stringBufs(valuesX);

    ArrowFieldNode fieldNode = new ArrowFieldNode(numRows, 0);

    ArrowRecordBatch batch =
        new ArrowRecordBatch(
            numRows,
            Lists.newArrayList(fieldNode, fieldNode),
            Lists.newArrayList(validityX, dataBufsX.get(0), dataBufsX.get(1)));

    // allocate data for output vector.
    IntervalDayVector outVector = new IntervalDayVector(EMPTY_SCHEMA_PATH, allocator);
    outVector.allocateNew();

    // evaluate expression
    List<ValueVector> output = new ArrayList<>();
    output.add(outVector);
    eval.evaluate(batch, output);
    eval.close();

    // match expected output.
    NullableIntervalDayHolder holder = new NullableIntervalDayHolder();
    for (int i = 0; i < numRows - 1; i++) {
      assertFalse(outVector.isNull(i), "Expect none value equals null");
      outVector.get(i, holder);

      assertEquals(expected[i][0], holder.days);
      assertEquals(expected[i][1], holder.milliseconds);
    }

    assertTrue(outVector.isNull(numRows - 1), "Last value must be null");

    releaseRecordBatch(batch);
    releaseValueVectors(output);
  }

  @Test
  public void testCastIntervalYear() throws GandivaException {

    Field x = Field.nullable("x", new ArrowType.Utf8());

    Field retType = Field.nullable("c", new ArrowType.Interval(IntervalUnit.YEAR_MONTH));

    TreeNode cond =
        TreeBuilder.makeFunction(
            "castintervalyear",
            Lists.newArrayList(TreeBuilder.makeField(x)),
            new ArrowType.Interval(IntervalUnit.YEAR_MONTH));
    ExpressionTree expr = TreeBuilder.makeExpression(cond, retType);
    Schema schema = new Schema(Lists.newArrayList(x));
    Projector eval = Projector.make(schema, Lists.newArrayList(expr));

    int numRows = 4;
    byte[] validity = new byte[] {(byte) 7, 0};
    String[] valuesX = new String[] {"65851111", "P1Y1M1DT1H1M1S", "P1Y", "test"};
    int[][] expected =
        new int[][] { // year and month
          {0, 65851111}, {1, 1}, {1, 0}, null
        };

    ArrowBuf validityX = buf(validity);
    List<ArrowBuf> dataBufsX = stringBufs(valuesX);

    ArrowFieldNode fieldNode = new ArrowFieldNode(numRows, 0);

    ArrowRecordBatch batch =
        new ArrowRecordBatch(
            numRows,
            Lists.newArrayList(fieldNode, fieldNode),
            Lists.newArrayList(validityX, dataBufsX.get(0), dataBufsX.get(1)));

    // allocate data for output vector.
    IntervalYearVector outVector = new IntervalYearVector(EMPTY_SCHEMA_PATH, allocator);
    outVector.allocateNew();

    // evaluate expression
    List<ValueVector> output = new ArrayList<>();
    output.add(outVector);
    eval.evaluate(batch, output);
    eval.close();

    // match expected output.
    NullableIntervalYearHolder holder = new NullableIntervalYearHolder();
    for (int i = 0; i < numRows - 1; i++) {
      assertFalse(outVector.isNull(i), "Expect none value equals null");
      outVector.get(i, holder);

      int numberMonths =
          expected[i][0] * 12
              + // number of years
              expected[i][1]; // number of months

      assertEquals(numberMonths, holder.value);
    }

    assertTrue(outVector.isNull(numRows - 1), "Last value must be null");

    releaseRecordBatch(batch);
    releaseValueVectors(output);
  }

  @Test
  public void testRand() throws GandivaException {

    TreeNode randWithSeed =
        TreeBuilder.makeFunction("rand", Lists.newArrayList(TreeBuilder.makeLiteral(12)), float64);
    TreeNode rand = TreeBuilder.makeFunction("rand", Lists.newArrayList(), float64);
    ExpressionTree exprWithSeed =
        TreeBuilder.makeExpression(randWithSeed, Field.nullable("res", float64));
    ExpressionTree expr = TreeBuilder.makeExpression(rand, Field.nullable("res2", float64));
    Field x = Field.nullable("x", new ArrowType.Utf8());
    Schema schema = new Schema(Lists.newArrayList(x));
    Projector evalWithSeed = Projector.make(schema, Lists.newArrayList(exprWithSeed));
    Projector eval = Projector.make(schema, Lists.newArrayList(expr));

    int numRows = 5;
    byte[] validity = new byte[] {(byte) 255, 0};
    String[] valuesX = new String[] {"mapD", "maps", "google maps", "map", "MapR"};
    double[] expected =
        new double[] {
          0.1597116001879662D,
          0.7347813877263527D,
          0.6069965050584282D,
          0.7240285696335824D,
          0.09975540272957834D
        };

    ArrowBuf validityX = buf(validity);
    List<ArrowBuf> dataBufsX = stringBufs(valuesX);

    ArrowRecordBatch batch =
        new ArrowRecordBatch(
            numRows,
            Lists.newArrayList(new ArrowFieldNode(numRows, 0)),
            Lists.newArrayList(validityX, dataBufsX.get(0), dataBufsX.get(1)));

    Float8Vector float8Vector = new Float8Vector(EMPTY_SCHEMA_PATH, allocator);
    float8Vector.allocateNew(numRows);

    List<ValueVector> output = new ArrayList<ValueVector>();
    output.add(float8Vector);
    evalWithSeed.evaluate(batch, output);

    for (int i = 0; i < numRows; i++) {
      assertFalse(float8Vector.isNull(i));
      assertEquals(expected[i], float8Vector.getObject(i), 0.000000001);
    }

    eval.evaluate(batch, output); // without seed
    assertNotEquals(float8Vector.getObject(0), float8Vector.getObject(1), 0.000000001);

    releaseRecordBatch(batch);
    releaseValueVectors(output);
    eval.close();
    evalWithSeed.close();
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
    byte[] validity = new byte[] {(byte) 255, 0};
    String[] valuesA = new String[] {"a", "aa", "aaa", "aaaa", "A função"};
    String[] valuesB = new String[] {"a", "bb", "aaa", "bbbbb", "A função"};
    boolean[] expected = new boolean[] {true, false, true, false, true};

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
    byte[] validity = new byte[] {(byte) 255, (byte) 255};
    long[] xValues = new long[] {9, 15, 21, 32, 43, 54, 65, 76, 87, 98, 109, 200, -10, 60, 77, 80};
    long[] expected = new long[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 10, 0, 6, 7, 8};

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
    byte[] validity = new byte[] {(byte) 255};
    long[] xValues = new long[] {9, 15, 17, 25};
    boolean[] expected = new boolean[] {false, true, true, false};

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
    byte[] validity = new byte[] {(byte) 255};
    long[] xValues = new long[] {4, 9, 15, 17};
    boolean[] expected = new boolean[] {true, false, true, true};

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
    byte[] validity = new byte[] {(byte) 255};
    long[] xValues = new long[] {5, 32};
    long[] expected = new long[] {1, 0};

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
    byte[] validity = new byte[] {(byte) 255};
    int[] xValues = new int[] {5, 32};

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
  public void testTimeEquals() throws GandivaException, Exception {
    /*
     * when isnotnull(x) then x
     * else y
     */
    Field x = Field.nullable("x", new ArrowType.Time(TimeUnit.MILLISECOND, 32));
    TreeNode xNode = TreeBuilder.makeField(x);

    Field y = Field.nullable("y", new ArrowType.Time(TimeUnit.MILLISECOND, 32));
    TreeNode yNode = TreeBuilder.makeField(y);

    // if isnotnull(x) then x else y
    TreeNode condition = TreeBuilder.makeFunction("isnotnull", Lists.newArrayList(xNode), boolType);
    TreeNode ifCoalesce =
        TreeBuilder.makeIf(condition, xNode, yNode, new ArrowType.Time(TimeUnit.MILLISECOND, 32));

    ExpressionTree expr = TreeBuilder.makeExpression(ifCoalesce, x);
    Schema schema = new Schema(Lists.newArrayList(x, y));
    Projector eval = Projector.make(schema, Lists.newArrayList(expr));

    int numRows = 2;
    byte[] validity = new byte[] {(byte) 1};
    byte[] yValidity = new byte[] {(byte) 3};
    int[] xValues = new int[] {5, 1};
    int[] yValues = new int[] {10, 2};
    int[] expected = new int[] {5, 2};

    ArrowBuf bufValidity = buf(validity);
    ArrowBuf xData = intBuf(xValues);

    ArrowBuf yBufValidity = buf(yValidity);
    ArrowBuf yData = intBuf(yValues);

    ArrowFieldNode fieldNode = new ArrowFieldNode(numRows, 0);
    ArrowRecordBatch batch =
        new ArrowRecordBatch(
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
    byte[] validity = new byte[] {(byte) 255, 0};
    double[] xValues =
        new double[] {
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
    byte[] validity = new byte[] {(byte) 255, 0};
    int[] c1Values = new int[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};
    int[] c2Values = new int[] {1, 2, 3, 4, 8, 7, 6, 5, 16, 15, 14, 13, 12, 11, 10, 9};

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
  public void testInExpr() throws GandivaException, Exception {
    Field c1 = Field.nullable("c1", int32);

    TreeNode inExpr =
        TreeBuilder.makeInExpressionInt32(
            TreeBuilder.makeField(c1), Sets.newHashSet(1, 2, 3, 4, 5, 15, 16));
    ExpressionTree expr = TreeBuilder.makeExpression(inExpr, Field.nullable("result", boolType));
    Schema schema = new Schema(Lists.newArrayList(c1));
    Projector eval = Projector.make(schema, Lists.newArrayList(expr));

    int numRows = 16;
    byte[] validity = new byte[] {(byte) 255, 0};
    int[] c1Values = new int[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};

    ArrowBuf c1Validity = buf(validity);
    ArrowBuf c1Data = intBuf(c1Values);
    ArrowBuf c2Validity = buf(validity);

    ArrowFieldNode fieldNode = new ArrowFieldNode(numRows, 0);
    ArrowRecordBatch batch =
        new ArrowRecordBatch(
            numRows,
            Lists.newArrayList(fieldNode, fieldNode),
            Lists.newArrayList(c1Validity, c1Data, c2Validity));

    BitVector bitVector = new BitVector(EMPTY_SCHEMA_PATH, allocator);
    bitVector.allocateNew(numRows);

    List<ValueVector> output = new ArrayList<ValueVector>();
    output.add(bitVector);
    eval.evaluate(batch, output);

    for (int i = 0; i < 5; i++) {
      assertTrue(bitVector.getObject(i).booleanValue());
    }
    for (int i = 5; i < 16; i++) {
      assertFalse(bitVector.getObject(i).booleanValue());
    }

    releaseRecordBatch(batch);
    releaseValueVectors(output);
    eval.close();
  }

  @Test
  public void testInExprDecimal() throws GandivaException, Exception {
    Integer precision = 26;
    Integer scale = 5;
    ArrowType.Decimal decimal = new ArrowType.Decimal(precision, scale, 128);
    Field c1 = Field.nullable("c1", decimal);

    String[] values = new String[] {"1", "2", "3", "4"};
    Set<BigDecimal> decimalSet = decimalSet(values, scale);
    decimalSet.add(new BigDecimal(-0.0));
    decimalSet.add(new BigDecimal(Long.MAX_VALUE));
    decimalSet.add(new BigDecimal(Long.MIN_VALUE));
    TreeNode inExpr =
        TreeBuilder.makeInExpressionDecimal(
            TreeBuilder.makeField(c1), decimalSet, precision, scale);
    ExpressionTree expr = TreeBuilder.makeExpression(inExpr, Field.nullable("result", boolType));
    Schema schema = new Schema(Lists.newArrayList(c1));
    Projector eval = Projector.make(schema, Lists.newArrayList(expr));

    int numRows = 16;
    byte[] validity = new byte[] {(byte) 255, 0};
    String[] c1Values =
        new String[] {
          "1",
          "2",
          "3",
          "4",
          "-0.0",
          "6",
          "7",
          "8",
          "9",
          "10",
          "11",
          "12",
          "13",
          "14",
          String.valueOf(Long.MAX_VALUE),
          String.valueOf(Long.MIN_VALUE)
        };

    DecimalVector c1Data = decimalVector(c1Values, precision, scale);
    ArrowBuf c1Validity = buf(validity);

    ArrowFieldNode fieldNode = new ArrowFieldNode(numRows, 0);
    ArrowRecordBatch batch =
        new ArrowRecordBatch(
            numRows,
            Lists.newArrayList(fieldNode, fieldNode),
            Lists.newArrayList(c1Validity, c1Data.getDataBuffer(), c1Data.getValidityBuffer()));

    BitVector bitVector = new BitVector(EMPTY_SCHEMA_PATH, allocator);
    bitVector.allocateNew(numRows);

    List<ValueVector> output = new ArrayList<ValueVector>();
    output.add(bitVector);
    eval.evaluate(batch, output);

    for (int i = 0; i < 5; i++) {
      assertTrue(bitVector.getObject(i).booleanValue());
    }
    for (int i = 5; i < 16; i++) {
      assertFalse(bitVector.getObject(i).booleanValue());
    }

    releaseRecordBatch(batch);
    releaseValueVectors(output);
    eval.close();
  }

  @Test
  public void testInExprDouble() throws GandivaException, Exception {
    Field c1 = Field.nullable("c1", float64);

    TreeNode inExpr =
        TreeBuilder.makeInExpressionDouble(
            TreeBuilder.makeField(c1),
            Sets.newHashSet(
                1.0,
                -0.0,
                3.0,
                4.0,
                Double.NaN,
                Double.POSITIVE_INFINITY,
                Double.NEGATIVE_INFINITY));
    ExpressionTree expr = TreeBuilder.makeExpression(inExpr, Field.nullable("result", boolType));
    Schema schema = new Schema(Lists.newArrayList(c1));
    Projector eval = Projector.make(schema, Lists.newArrayList(expr));

    // Create a row-batch with some sample data to look for
    int numRows = 16;
    // Only the first 8 values will be valid.
    byte[] validity = new byte[] {(byte) 255, 0};
    double[] c1Values =
        new double[] {
          1,
          -0.0,
          Double.NEGATIVE_INFINITY,
          Double.POSITIVE_INFINITY,
          Double.NaN,
          6,
          7,
          8,
          9,
          10,
          11,
          12,
          13,
          14,
          4,
          3
        };

    ArrowBuf c1Validity = buf(validity);
    ArrowBuf c1Data = doubleBuf(c1Values);
    ArrowBuf c2Validity = buf(validity);

    ArrowFieldNode fieldNode = new ArrowFieldNode(numRows, 0);
    ArrowRecordBatch batch =
        new ArrowRecordBatch(
            numRows,
            Lists.newArrayList(fieldNode, fieldNode),
            Lists.newArrayList(c1Validity, c1Data, c2Validity));

    BitVector bitVector = new BitVector(EMPTY_SCHEMA_PATH, allocator);
    bitVector.allocateNew(numRows);

    List<ValueVector> output = new ArrayList<ValueVector>();
    output.add(bitVector);
    eval.evaluate(batch, output);

    // The first four values in the vector must match the expression, but not the other ones.
    for (int i = 0; i < 4; i++) {
      assertTrue(bitVector.getObject(i).booleanValue());
    }
    for (int i = 4; i < 16; i++) {
      assertFalse(bitVector.getObject(i).booleanValue());
    }

    releaseRecordBatch(batch);
    releaseValueVectors(output);
    eval.close();
  }

  @Test
  public void testInExprStrings() throws GandivaException, Exception {
    Field c1 = Field.nullable("c1", new ArrowType.Utf8());

    TreeNode l1 = TreeBuilder.makeLiteral(1L);
    TreeNode l2 = TreeBuilder.makeLiteral(3L);
    List<TreeNode> args = Lists.newArrayList(TreeBuilder.makeField(c1), l1, l2);
    TreeNode substr = TreeBuilder.makeFunction("substr", args, new ArrowType.Utf8());
    TreeNode inExpr =
        TreeBuilder.makeInExpressionString(substr, Sets.newHashSet("one", "two", "thr", "fou"));
    ExpressionTree expr = TreeBuilder.makeExpression(inExpr, Field.nullable("result", boolType));
    Schema schema = new Schema(Lists.newArrayList(c1));
    Projector eval = Projector.make(schema, Lists.newArrayList(expr));

    int numRows = 16;
    byte[] validity = new byte[] {(byte) 255, 0};
    String[] c1Values =
        new String[] {
          "one",
          "two",
          "three",
          "four",
          "five",
          "six",
          "seven",
          "eight",
          "nine",
          "ten",
          "eleven",
          "twelve",
          "thirteen",
          "fourteen",
          "fifteen",
          "sixteen"
        };

    ArrowBuf c1Validity = buf(validity);
    List<ArrowBuf> dataBufsX = stringBufs(c1Values);
    ArrowBuf c2Validity = buf(validity);

    ArrowFieldNode fieldNode = new ArrowFieldNode(numRows, 0);
    ArrowRecordBatch batch =
        new ArrowRecordBatch(
            numRows,
            Lists.newArrayList(fieldNode, fieldNode),
            Lists.newArrayList(c1Validity, dataBufsX.get(0), dataBufsX.get(1), c2Validity));

    BitVector bitVector = new BitVector(EMPTY_SCHEMA_PATH, allocator);
    bitVector.allocateNew(numRows);

    List<ValueVector> output = new ArrayList<ValueVector>();
    output.add(bitVector);
    eval.evaluate(batch, output);

    for (int i = 0; i < 4; i++) {
      assertTrue(bitVector.getObject(i).booleanValue());
    }
    for (int i = 5; i < 16; i++) {
      assertFalse(bitVector.getObject(i).booleanValue());
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
    byte[] validity = new byte[] {(byte) 255, 0};
    // second half is "undefined"
    int[] aValues = new int[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};
    int[] bValues = new int[] {16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1};

    ArrowBuf aValidity = buf(validity);
    ArrowBuf aData = intBuf(aValues);
    ArrowBuf bValidity = buf(validity);
    ArrowBuf b2Validity = buf(validity);
    ArrowBuf bData = intBuf(bValues);
    ArrowRecordBatch batch =
        new ArrowRecordBatch(
            numRows,
            Lists.newArrayList(new ArrowFieldNode(numRows, 8), new ArrowFieldNode(numRows, 8)),
            Lists.newArrayList(aValidity, aData, bValidity, bData, b2Validity));

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
    byte[] validity = new byte[] {(byte) 255};
    String[] values =
        new String[] {
          "2007-01-01T01:00:00.00Z",
          "2007-03-05T03:40:00.00Z",
          "2008-05-31T13:55:00.00Z",
          "2000-06-30T23:20:00.00Z",
          "2000-07-10T20:30:00.00Z",
          "2000-08-20T00:14:00.00Z",
          "2000-09-30T02:29:00.00Z",
          "2000-10-31T05:33:00.00Z"
        };
    long[] expYearFromDate = new long[] {2007, 2007, 2008, 2000, 2000, 2000, 2000, 2000};
    long[] expMonthFromDate = new long[] {1, 3, 5, 6, 7, 8, 9, 10};
    long[] expDayFromDate = new long[] {1, 5, 31, 30, 10, 20, 30, 31};
    long[] expHourFromDate = new long[] {1, 3, 13, 23, 20, 0, 2, 5};
    long[] expMinFromDate = new long[] {0, 40, 55, 20, 30, 14, 29, 33};

    long[][] expValues =
        new long[][] {
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
  public void testDateTrunc() throws Exception {
    ArrowType date64 = new ArrowType.Date(DateUnit.MILLISECOND);
    Field dateField = Field.nullable("date", date64);

    TreeNode dateNode = TreeBuilder.makeField(dateField);

    List<TreeNode> dateArgs = Lists.newArrayList(dateNode);
    TreeNode dateToYear = TreeBuilder.makeFunction("date_trunc_Year", dateArgs, date64);
    TreeNode dateToMonth = TreeBuilder.makeFunction("date_trunc_Month", dateArgs, date64);

    Field resultField = Field.nullable("result", date64);
    List<ExpressionTree> exprs =
        Lists.newArrayList(
            TreeBuilder.makeExpression(dateToYear, resultField),
            TreeBuilder.makeExpression(dateToMonth, resultField));

    Schema schema = new Schema(Lists.newArrayList(dateField));
    Projector eval = Projector.make(schema, exprs);

    int numRows = 4;
    byte[] validity = new byte[] {(byte) 255};
    String[] values =
        new String[] {
          "2007-01-01T01:00:00.00Z",
          "2007-03-05T03:40:00.00Z",
          "2008-05-31T13:55:00.00Z",
          "2000-06-30T23:20:00.00Z",
        };
    String[] expYearFromDate =
        new String[] {
          "2007-01-01T00:00:00.00Z",
          "2007-01-01T00:00:00.00Z",
          "2008-01-01T00:00:00.00Z",
          "2000-01-01T00:00:00.00Z",
        };
    String[] expMonthFromDate =
        new String[] {
          "2007-01-01T00:00:00.00Z",
          "2007-03-01T00:00:00.00Z",
          "2008-05-01T00:00:00.00Z",
          "2000-06-01T00:00:00.00Z",
        };

    String[][] expValues = new String[][] {expYearFromDate, expMonthFromDate};

    ArrowBuf bufValidity = buf(validity);
    ArrowBuf millisData = stringToMillis(values);

    ArrowFieldNode fieldNode = new ArrowFieldNode(numRows, 0);
    ArrowRecordBatch batch =
        new ArrowRecordBatch(
            numRows, Lists.newArrayList(fieldNode), Lists.newArrayList(bufValidity, millisData));

    List<ValueVector> output = new ArrayList<ValueVector>();
    for (int i = 0; i < exprs.size(); i++) {
      BigIntVector bigIntVector = new BigIntVector(EMPTY_SCHEMA_PATH, allocator);
      bigIntVector.allocateNew(numRows);
      output.add(bigIntVector);
    }
    eval.evaluate(batch, output);
    eval.close();

    for (int i = 0; i < output.size(); i++) {
      String[] expected = expValues[i];
      BigIntVector bigIntVector = (BigIntVector) output.get(i);

      for (int j = 0; j < numRows; j++) {
        assertFalse(bigIntVector.isNull(j));
        assertEquals(Instant.parse(expected[j]).toEpochMilli(), bigIntVector.get(j));
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

  @Test
  public void testCastTimestampToString() throws Exception {
    ArrowType timeStamp = new ArrowType.Timestamp(TimeUnit.MILLISECOND, "TZ");

    Field tsField = Field.nullable("timestamp", timeStamp);
    Field lenField = Field.nullable("outLength", int64);

    TreeNode tsNode = TreeBuilder.makeField(tsField);
    TreeNode lenNode = TreeBuilder.makeField(lenField);

    TreeNode tsToString =
        TreeBuilder.makeFunction(
            "castVARCHAR", Lists.newArrayList(tsNode, lenNode), new ArrowType.Utf8());

    Field resultField = Field.nullable("result", new ArrowType.Utf8());
    List<ExpressionTree> exprs =
        Lists.newArrayList(TreeBuilder.makeExpression(tsToString, resultField));

    Schema schema = new Schema(Lists.newArrayList(tsField, lenField));
    Projector eval = Projector.make(schema, exprs);

    int numRows = 5;
    byte[] validity = new byte[] {(byte) 255};
    String[] values =
        new String[] {
          "0007-01-01T01:00:00Z",
          "2007-03-05T03:40:00Z",
          "2008-05-31T13:55:00Z",
          "2000-06-30T23:20:00Z",
          "2000-07-10T20:30:00Z",
        };
    long[] lenValues = new long[] {23L, 24L, 22L, 0L, 4L};

    String[] expValues =
        new String[] {
          "0007-01-01 01:00:00.000",
          "2007-03-05 03:40:00.000",
          "2008-05-31 13:55:00.00",
          "",
          "2000",
        };

    ArrowBuf bufValidity = buf(validity);
    ArrowBuf millisData = stringToMillis(values);
    ArrowBuf lenValidity = buf(validity);
    ArrowBuf lenData = longBuf(lenValues);

    ArrowFieldNode fieldNode = new ArrowFieldNode(numRows, 0);
    ArrowRecordBatch batch =
        new ArrowRecordBatch(
            numRows,
            Lists.newArrayList(fieldNode, fieldNode),
            Lists.newArrayList(bufValidity, millisData, lenValidity, lenData));

    List<ValueVector> output = new ArrayList<>();
    for (int i = 0; i < exprs.size(); i++) {
      VarCharVector charVector = new VarCharVector(EMPTY_SCHEMA_PATH, allocator);

      charVector.allocateNew(numRows * 23, numRows);
      output.add(charVector);
    }
    eval.evaluate(batch, output);
    eval.close();

    for (ValueVector valueVector : output) {
      VarCharVector charVector = (VarCharVector) valueVector;

      for (int j = 0; j < numRows; j++) {
        assertFalse(charVector.isNull(j));
        assertEquals(expValues[j], new String(charVector.get(j)));
      }
    }

    releaseRecordBatch(batch);
    releaseValueVectors(output);
  }

  @Test
  public void testCastDayIntervalToBigInt() throws Exception {
    ArrowType dayIntervalType = new ArrowType.Interval(IntervalUnit.DAY_TIME);

    Field dayIntervalField = Field.nullable("dayInterval", dayIntervalType);

    TreeNode intervalNode = TreeBuilder.makeField(dayIntervalField);

    TreeNode intervalToBigint =
        TreeBuilder.makeFunction("castBIGINT", Lists.newArrayList(intervalNode), int64);

    Field resultField = Field.nullable("result", int64);
    List<ExpressionTree> exprs =
        Lists.newArrayList(TreeBuilder.makeExpression(intervalToBigint, resultField));

    Schema schema = new Schema(Lists.newArrayList(dayIntervalField));
    Projector eval = Projector.make(schema, exprs);

    int numRows = 5;
    byte[] validity = new byte[] {(byte) 255};
    String[] values =
        new String[] {
          "1 0", // "days millis"
          "2 0",
          "1 1",
          "10 5000",
          "11 86400001",
        };

    Long[] expValues =
        new Long[] {
          86400000L,
          2 * 86400000L,
          86400000L + 1L,
          10 * 86400000L + 5000L,
          11 * 86400000L + 86400001L
        };

    ArrowBuf bufValidity = buf(validity);
    ArrowBuf intervalsData = stringToDayInterval(values);

    ArrowFieldNode fieldNode = new ArrowFieldNode(numRows, 0);
    ArrowRecordBatch batch =
        new ArrowRecordBatch(
            numRows,
            Lists.newArrayList(fieldNode, fieldNode),
            Lists.newArrayList(bufValidity, intervalsData));

    List<ValueVector> output = new ArrayList<>();
    for (int i = 0; i < exprs.size(); i++) {
      BigIntVector bigIntVector = new BigIntVector(EMPTY_SCHEMA_PATH, allocator);
      bigIntVector.allocateNew(numRows);
      output.add(bigIntVector);
    }
    eval.evaluate(batch, output);
    eval.close();

    for (ValueVector valueVector : output) {
      BigIntVector bigintVector = (BigIntVector) valueVector;

      for (int j = 0; j < numRows; j++) {
        assertFalse(bigintVector.isNull(j));
        assertEquals(expValues[j], Long.valueOf(bigintVector.get(j)));
      }
    }

    releaseRecordBatch(batch);
    releaseValueVectors(output);
  }

  @Test
  public void testCaseInsensitiveFunctions() throws Exception {
    ArrowType timeStamp = new ArrowType.Timestamp(TimeUnit.MILLISECOND, "TZ");

    Field tsField = Field.nullable("timestamp", timeStamp);

    TreeNode tsNode = TreeBuilder.makeField(tsField);

    TreeNode extractday = TreeBuilder.makeFunction("extractday", Lists.newArrayList(tsNode), int64);

    ExpressionTree expr = TreeBuilder.makeExpression(extractday, Field.nullable("result", int64));
    Schema schema = new Schema(Lists.newArrayList(tsField));
    Projector eval = Projector.make(schema, Lists.newArrayList(expr));

    int numRows = 5;
    byte[] validity = new byte[] {(byte) 255};
    String[] values =
        new String[] {
          "0007-01-01T01:00:00Z",
          "2007-03-05T03:40:00Z",
          "2008-05-31T13:55:00Z",
          "2000-06-30T23:20:00Z",
          "2000-07-10T20:30:00Z",
        };

    long[] expValues = new long[] {1, 5, 31, 30, 10};

    ArrowBuf bufValidity = buf(validity);
    ArrowBuf millisData = stringToMillis(values);

    ArrowFieldNode fieldNode = new ArrowFieldNode(numRows, 0);
    ArrowRecordBatch batch =
        new ArrowRecordBatch(
            numRows, Lists.newArrayList(fieldNode), Lists.newArrayList(bufValidity, millisData));

    List<ValueVector> output = new ArrayList<>();
    BigIntVector bigIntVector = new BigIntVector(EMPTY_SCHEMA_PATH, allocator);
    bigIntVector.allocateNew(numRows);
    output.add(bigIntVector);

    eval.evaluate(batch, output);
    eval.close();

    for (ValueVector valueVector : output) {
      BigIntVector vector = (BigIntVector) valueVector;

      for (int j = 0; j < numRows; j++) {
        assertFalse(vector.isNull(j));
        assertEquals(expValues[j], vector.get(j));
      }
    }

    releaseRecordBatch(batch);
    releaseValueVectors(output);
  }

  @Test
  public void testCastInt() throws Exception {
    Field inField = Field.nullable("input", new ArrowType.Utf8());
    TreeNode inNode = TreeBuilder.makeField(inField);
    TreeNode castINTFn = TreeBuilder.makeFunction("castINT", Lists.newArrayList(inNode), int32);
    Field resultField = Field.nullable("result", int32);
    List<ExpressionTree> exprs =
        Lists.newArrayList(TreeBuilder.makeExpression(castINTFn, resultField));
    Schema schema = new Schema(Lists.newArrayList(inField));
    Projector eval = Projector.make(schema, exprs);
    int numRows = 5;
    byte[] validity = new byte[] {(byte) 255};
    String[] values = new String[] {"0", "123", "-123", "-1", "1"};
    int[] expValues = new int[] {0, 123, -123, -1, 1};
    ArrowBuf bufValidity = buf(validity);
    List<ArrowBuf> bufData = stringBufs(values);
    ArrowFieldNode fieldNode = new ArrowFieldNode(numRows, 0);
    ArrowRecordBatch batch =
        new ArrowRecordBatch(
            numRows,
            Lists.newArrayList(fieldNode),
            Lists.newArrayList(bufValidity, bufData.get(0), bufData.get(1)));
    List<ValueVector> output = new ArrayList<>();
    for (int i = 0; i < exprs.size(); i++) {
      IntVector intVector = new IntVector(EMPTY_SCHEMA_PATH, allocator);
      intVector.allocateNew(numRows);
      output.add(intVector);
    }
    eval.evaluate(batch, output);
    eval.close();
    for (ValueVector valueVector : output) {
      IntVector intVector = (IntVector) valueVector;
      for (int j = 0; j < numRows; j++) {
        assertFalse(intVector.isNull(j));
        assertTrue(expValues[j] == intVector.get(j));
      }
    }
    releaseRecordBatch(batch);
    releaseValueVectors(output);
  }

  @Test
  public void testCastIntInvalidValue() throws Exception {
    Field inField = Field.nullable("input", new ArrowType.Utf8());
    TreeNode inNode = TreeBuilder.makeField(inField);
    TreeNode castINTFn = TreeBuilder.makeFunction("castINT", Lists.newArrayList(inNode), int32);
    Field resultField = Field.nullable("result", int32);
    List<ExpressionTree> exprs =
        Lists.newArrayList(TreeBuilder.makeExpression(castINTFn, resultField));
    Schema schema = new Schema(Lists.newArrayList(inField));
    Projector eval = Projector.make(schema, exprs);
    int numRows = 1;
    byte[] validity = new byte[] {(byte) 255};
    String[] values = new String[] {"abc"};
    ArrowBuf bufValidity = buf(validity);
    List<ArrowBuf> bufData = stringBufs(values);
    ArrowFieldNode fieldNode = new ArrowFieldNode(numRows, 0);
    ArrowRecordBatch batch =
        new ArrowRecordBatch(
            numRows,
            Lists.newArrayList(fieldNode),
            Lists.newArrayList(bufValidity, bufData.get(0), bufData.get(1)));
    List<ValueVector> output = new ArrayList<>();
    for (int i = 0; i < exprs.size(); i++) {
      IntVector intVector = new IntVector(EMPTY_SCHEMA_PATH, allocator);
      intVector.allocateNew(numRows);
      output.add(intVector);
    }

    assertThrows(
        GandivaException.class,
        () -> {
          try {
            eval.evaluate(batch, output);
          } finally {
            eval.close();
            releaseRecordBatch(batch);
            releaseValueVectors(output);
          }
        });
  }

  @Test
  public void testCastFloat() throws Exception {
    Field inField = Field.nullable("input", new ArrowType.Utf8());
    TreeNode inNode = TreeBuilder.makeField(inField);
    TreeNode castFLOAT8Fn =
        TreeBuilder.makeFunction("castFLOAT8", Lists.newArrayList(inNode), float64);
    Field resultField = Field.nullable("result", float64);
    List<ExpressionTree> exprs =
        Lists.newArrayList(TreeBuilder.makeExpression(castFLOAT8Fn, resultField));
    Schema schema = new Schema(Lists.newArrayList(inField));
    Projector eval = Projector.make(schema, exprs);
    int numRows = 5;
    byte[] validity = new byte[] {(byte) 255};
    String[] values = new String[] {"2.3", "-11.11", "0", "111", "12345.67"};
    double[] expValues = new double[] {2.3, -11.11, 0, 111, 12345.67};
    ArrowBuf bufValidity = buf(validity);
    List<ArrowBuf> bufData = stringBufs(values);
    ArrowFieldNode fieldNode = new ArrowFieldNode(numRows, 0);
    ArrowRecordBatch batch =
        new ArrowRecordBatch(
            numRows,
            Lists.newArrayList(fieldNode),
            Lists.newArrayList(bufValidity, bufData.get(0), bufData.get(1)));
    List<ValueVector> output = new ArrayList<>();
    for (int i = 0; i < exprs.size(); i++) {
      Float8Vector float8Vector = new Float8Vector(EMPTY_SCHEMA_PATH, allocator);
      float8Vector.allocateNew(numRows);
      output.add(float8Vector);
    }
    eval.evaluate(batch, output);
    eval.close();
    for (ValueVector valueVector : output) {
      Float8Vector float8Vector = (Float8Vector) valueVector;
      for (int j = 0; j < numRows; j++) {
        assertFalse(float8Vector.isNull(j));
        assertTrue(expValues[j] == float8Vector.get(j));
      }
    }
    releaseRecordBatch(batch);
    releaseValueVectors(output);
  }

  @Test
  public void testCastFloatVarbinary() throws Exception {
    Field inField = Field.nullable("input", new ArrowType.Binary());
    TreeNode inNode = TreeBuilder.makeField(inField);
    TreeNode castFLOAT8Fn =
        TreeBuilder.makeFunction("castFLOAT8", Lists.newArrayList(inNode), float64);
    Field resultField = Field.nullable("result", float64);
    List<ExpressionTree> exprs =
        Lists.newArrayList(TreeBuilder.makeExpression(castFLOAT8Fn, resultField));
    Schema schema = new Schema(Lists.newArrayList(inField));
    Projector eval = Projector.make(schema, exprs);
    int numRows = 5;
    byte[] validity = new byte[] {(byte) 255};
    String[] values = new String[] {"2.3", "-11.11", "0", "111", "12345.67"};
    double[] expValues = new double[] {2.3, -11.11, 0, 111, 12345.67};
    ArrowBuf bufValidity = buf(validity);
    List<ArrowBuf> bufData = stringBufs(values);
    ArrowFieldNode fieldNode = new ArrowFieldNode(numRows, 0);
    ArrowRecordBatch batch =
        new ArrowRecordBatch(
            numRows,
            Lists.newArrayList(fieldNode),
            Lists.newArrayList(bufValidity, bufData.get(0), bufData.get(1)));
    List<ValueVector> output = new ArrayList<>();
    for (int i = 0; i < exprs.size(); i++) {
      Float8Vector float8Vector = new Float8Vector(EMPTY_SCHEMA_PATH, allocator);
      float8Vector.allocateNew(numRows);
      output.add(float8Vector);
    }
    eval.evaluate(batch, output);
    eval.close();
    for (ValueVector valueVector : output) {
      Float8Vector float8Vector = (Float8Vector) valueVector;
      for (int j = 0; j < numRows; j++) {
        assertFalse(float8Vector.isNull(j));
        assertTrue(expValues[j] == float8Vector.get(j));
      }
    }
    releaseRecordBatch(batch);
    releaseValueVectors(output);
  }

  @Test
  public void testCastFloatInvalidValue() throws Exception {
    Field inField = Field.nullable("input", new ArrowType.Utf8());
    TreeNode inNode = TreeBuilder.makeField(inField);
    TreeNode castFLOAT8Fn =
        TreeBuilder.makeFunction("castFLOAT8", Lists.newArrayList(inNode), float64);
    Field resultField = Field.nullable("result", float64);
    List<ExpressionTree> exprs =
        Lists.newArrayList(TreeBuilder.makeExpression(castFLOAT8Fn, resultField));
    Schema schema = new Schema(Lists.newArrayList(inField));
    Projector eval = Projector.make(schema, exprs);
    int numRows = 5;
    byte[] validity = new byte[] {(byte) 255};
    String[] values = new String[] {"2.3", "-11.11", "abc", "111", "12345.67"};
    ArrowBuf bufValidity = buf(validity);
    List<ArrowBuf> bufData = stringBufs(values);
    ArrowFieldNode fieldNode = new ArrowFieldNode(numRows, 0);
    ArrowRecordBatch batch =
        new ArrowRecordBatch(
            numRows,
            Lists.newArrayList(fieldNode),
            Lists.newArrayList(bufValidity, bufData.get(0), bufData.get(1)));
    List<ValueVector> output = new ArrayList<>();
    for (int i = 0; i < exprs.size(); i++) {
      Float8Vector float8Vector = new Float8Vector(EMPTY_SCHEMA_PATH, allocator);
      float8Vector.allocateNew(numRows);
      output.add(float8Vector);
    }

    assertThrows(
        GandivaException.class,
        () -> {
          try {
            eval.evaluate(batch, output);
          } finally {
            eval.close();
            releaseRecordBatch(batch);
            releaseValueVectors(output);
          }
        });
  }

  @Test
  public void testEvaluateWithUnsetTargetHostCPU() throws Exception {
    Field a = Field.nullable("a", int32);
    Field b = Field.nullable("b", int32);
    List<Field> args = Lists.newArrayList(a, b);

    Field retType = Field.nullable("c", int32);
    ExpressionTree root = TreeBuilder.makeExpression("add", args, retType);

    List<ExpressionTree> exprs = Lists.newArrayList(root);

    Schema schema = new Schema(args);
    Projector eval =
        Projector.make(
            schema, exprs, new ConfigurationBuilder.ConfigOptions().withTargetCPU(false));

    int numRows = 16;
    byte[] validity = new byte[] {(byte) 255, 0};
    // second half is "undefined"
    int[] aValues = new int[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};
    int[] bValues = new int[] {16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1};

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
  public void testCastVarcharFromInteger() throws Exception {
    Field inField = Field.nullable("input", int32);
    Field lenField = Field.nullable("outLength", int64);

    TreeNode inNode = TreeBuilder.makeField(inField);
    TreeNode lenNode = TreeBuilder.makeField(lenField);

    TreeNode tsToString =
        TreeBuilder.makeFunction(
            "castVARCHAR", Lists.newArrayList(inNode, lenNode), new ArrowType.Utf8());

    Field resultField = Field.nullable("result", new ArrowType.Utf8());
    List<ExpressionTree> exprs =
        Lists.newArrayList(TreeBuilder.makeExpression(tsToString, resultField));

    Schema schema = new Schema(Lists.newArrayList(inField, lenField));
    Projector eval = Projector.make(schema, exprs);

    int numRows = 5;
    byte[] validity = new byte[] {(byte) 255};
    int[] values =
        new int[] {
          2345, 2345, 2345, 2345, -2345,
        };
    long[] lenValues = new long[] {0L, 4L, 2L, 6L, 5L};

    String[] expValues =
        new String[] {
          "",
          Integer.toString(2345).substring(0, 4),
          Integer.toString(2345).substring(0, 2),
          Integer.toString(2345),
          Integer.toString(-2345)
        };

    ArrowBuf bufValidity = buf(validity);
    ArrowBuf bufData = intBuf(values);
    ArrowBuf lenValidity = buf(validity);
    ArrowBuf lenData = longBuf(lenValues);

    ArrowFieldNode fieldNode = new ArrowFieldNode(numRows, 0);
    ArrowRecordBatch batch =
        new ArrowRecordBatch(
            numRows,
            Lists.newArrayList(fieldNode, fieldNode),
            Lists.newArrayList(bufValidity, bufData, lenValidity, lenData));

    List<ValueVector> output = new ArrayList<>();
    for (int i = 0; i < exprs.size(); i++) {
      VarCharVector charVector = new VarCharVector(EMPTY_SCHEMA_PATH, allocator);

      charVector.allocateNew(numRows * 5, numRows);
      output.add(charVector);
    }
    eval.evaluate(batch, output);
    eval.close();

    for (ValueVector valueVector : output) {
      VarCharVector charVector = (VarCharVector) valueVector;

      for (int j = 0; j < numRows; j++) {
        assertFalse(charVector.isNull(j));
        assertEquals(expValues[j], new String(charVector.get(j)));
      }
    }

    releaseRecordBatch(batch);
    releaseValueVectors(output);
  }

  @Test
  public void testCastVarcharFromFloat() throws Exception {
    Field inField = Field.nullable("input", float64);
    Field lenField = Field.nullable("outLength", int64);

    TreeNode inNode = TreeBuilder.makeField(inField);
    TreeNode lenNode = TreeBuilder.makeField(lenField);

    TreeNode tsToString =
        TreeBuilder.makeFunction(
            "castVARCHAR", Lists.newArrayList(inNode, lenNode), new ArrowType.Utf8());

    Field resultField = Field.nullable("result", new ArrowType.Utf8());
    List<ExpressionTree> exprs =
        Lists.newArrayList(TreeBuilder.makeExpression(tsToString, resultField));

    Schema schema = new Schema(Lists.newArrayList(inField, lenField));
    Projector eval = Projector.make(schema, exprs);

    int numRows = 5;
    byte[] validity = new byte[] {(byte) 255};
    double[] values =
        new double[] {
          0.0,
          -0.0,
          1.0,
          0.001,
          0.0009,
          0.00099893,
          999999.9999,
          10000000.0,
          23943410000000.343434,
          Double.POSITIVE_INFINITY,
          Double.NEGATIVE_INFINITY,
          Double.NaN,
          23.45,
          23.45,
          -23.45,
        };
    long[] lenValues =
        new long[] {6L, 6L, 6L, 6L, 10L, 15L, 15L, 15L, 30L, 15L, 15L, 15L, 0L, 6L, 6L};

    /* The Java real numbers are represented in two ways and Gandiva must
     * follow the same rules:
     * - If the number is greater or equals than 10^7 and less than 10^(-3)
     *   it will be represented using scientific notation, e.g:
     *       - 0.000012 -> 1.2E-5
     *       - 10000002.3 -> 1.00000023E7
     * - If the numbers are between that interval above, they are showed as is.
     *
     * The test checks if the Gandiva function casts the number with the same notation of the
     * Java.
     * */
    String[] expValues =
        new String[] {
          Double.toString(0.0), // must be cast to -> "0.0"
          Double.toString(-0.0), // must be cast to -> "-0.0"
          Double.toString(1.0), // must be cast to -> "1.0"
          Double.toString(0.001), // must be cast to -> "0.001"
          Double.toString(0.0009), // must be cast to -> "9E-4"
          Double.toString(0.00099893), // must be cast to -> "9E-4"
          Double.toString(999999.9999), // must be cast to -> "999999.9999"
          Double.toString(10000000.0), // must be cast to 1E7
          Double.toString(23943410000000.343434),
          Double.toString(Double.POSITIVE_INFINITY),
          Double.toString(Double.NEGATIVE_INFINITY),
          Double.toString(Double.NaN),
          "",
          Double.toString(23.45),
          Double.toString(-23.45)
        };

    ArrowBuf bufValidity = buf(validity);
    ArrowBuf bufData = doubleBuf(values);
    ArrowBuf lenValidity = buf(validity);
    ArrowBuf lenData = longBuf(lenValues);

    ArrowFieldNode fieldNode = new ArrowFieldNode(numRows, 0);
    ArrowRecordBatch batch =
        new ArrowRecordBatch(
            numRows,
            Lists.newArrayList(fieldNode, fieldNode),
            Lists.newArrayList(bufValidity, bufData, lenValidity, lenData));

    List<ValueVector> output = new ArrayList<>();
    for (int i = 0; i < exprs.size(); i++) {
      VarCharVector charVector = new VarCharVector(EMPTY_SCHEMA_PATH, allocator);

      charVector.allocateNew(numRows * 5, numRows);
      output.add(charVector);
    }
    eval.evaluate(batch, output);
    eval.close();

    for (ValueVector valueVector : output) {
      VarCharVector charVector = (VarCharVector) valueVector;

      for (int j = 0; j < numRows; j++) {
        assertFalse(charVector.isNull(j));
        assertEquals(expValues[j], new String(charVector.get(j)));
      }
    }

    releaseRecordBatch(batch);
    releaseValueVectors(output);
  }

  @Test
  public void testInitCap() throws Exception {

    Field x = Field.nullable("x", new ArrowType.Utf8());

    Field retType = Field.nullable("c", new ArrowType.Utf8());

    TreeNode cond =
        TreeBuilder.makeFunction(
            "initcap", Lists.newArrayList(TreeBuilder.makeField(x)), new ArrowType.Utf8());
    ExpressionTree expr = TreeBuilder.makeExpression(cond, retType);
    Schema schema = new Schema(Lists.newArrayList(x));
    Projector eval = Projector.make(schema, Lists.newArrayList(expr));

    int numRows = 5;
    byte[] validity = new byte[] {(byte) 15, 0};
    String[] valuesX =
        new String[] {
          "  øhpqršvñ  \n\n",
          "möbelträger1füße   \nmöbelträge'rfüße",
          "ÂbĆDËFgh\néll",
          "citroën CaR",
          "kjk"
        };

    String[] expected =
        new String[] {
          "  Øhpqršvñ  \n\n",
          "Möbelträger1füße   \nMöbelträge'Rfüße",
          "Âbćdëfgh\nÉll",
          "Citroën Car",
          null
        };

    ArrowBuf validityX = buf(validity);
    List<ArrowBuf> dataBufsX = stringBufs(valuesX);

    ArrowRecordBatch batch =
        new ArrowRecordBatch(
            numRows,
            Lists.newArrayList(new ArrowFieldNode(numRows, 0)),
            Lists.newArrayList(validityX, dataBufsX.get(0), dataBufsX.get(1)));

    // allocate data for output vector.
    VarCharVector outVector = new VarCharVector(EMPTY_SCHEMA_PATH, allocator);
    outVector.allocateNew(numRows * 100, numRows);

    // evaluate expression
    List<ValueVector> output = new ArrayList<>();
    output.add(outVector);
    eval.evaluate(batch, output);
    eval.close();

    // match expected output.
    for (int i = 0; i < numRows - 1; i++) {
      assertFalse(outVector.isNull(i), "Expect none value equals null");
      assertEquals(expected[i], new String(outVector.get(i)));
    }

    assertTrue(outVector.isNull(numRows - 1), "Last value must be null");

    releaseRecordBatch(batch);
    releaseValueVectors(output);
  }
}
