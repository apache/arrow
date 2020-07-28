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

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

import org.apache.arrow.gandiva.exceptions.GandivaException;
import org.apache.arrow.gandiva.expression.Condition;
import org.apache.arrow.gandiva.expression.TreeBuilder;
import org.apache.arrow.gandiva.expression.TreeNode;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class FilterTest extends BaseEvaluatorTest {

  private int[] selectionVectorToArray(SelectionVector vector) {
    int[] actual = new int[vector.getRecordCount()];
    for (int i = 0; i < vector.getRecordCount(); ++i) {
      actual[i] = vector.getIndex(i);
    }
    return actual;
  }

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

  @Test
  public void testSimpleInString() throws GandivaException, Exception {
    Field c1 = Field.nullable("c1", new ArrowType.Utf8());
    TreeNode l1 = TreeBuilder.makeLiteral(1L);
    TreeNode l2 = TreeBuilder.makeLiteral(3L);

    List<Field> argsSchema = Lists.newArrayList(c1);
    List<TreeNode> args = Lists.newArrayList(TreeBuilder.makeField(c1), l1, l2);
    TreeNode substr = TreeBuilder.makeFunction("substr", args, new ArrowType.Utf8());
    TreeNode inExpr =
            TreeBuilder.makeInExpressionString(substr, Sets.newHashSet("one", "two", "thr", "fou"));

    Condition condition = TreeBuilder.makeCondition(inExpr);

    Schema schema = new Schema(argsSchema);
    Filter filter = Filter.make(schema, condition);

    int numRows = 16;
    byte[] validity = new byte[] {(byte) 255, 0};
    // second half is "undefined"
    String[] c1Values = new String[]{"one", "two", "three", "four", "five", "six", "seven",
      "eight", "nine", "ten", "eleven", "twelve", "thirteen", "fourteen", "fifteen",
      "sixteen"};
    int[] expected = {0, 1, 2, 3};
    ArrowBuf c1Validity = buf(validity);
    ArrowBuf c2Validity = buf(validity);
    List<ArrowBuf> dataBufsX = stringBufs(c1Values);

    ArrowFieldNode fieldNode = new ArrowFieldNode(numRows, 0);
    ArrowRecordBatch batch =
            new ArrowRecordBatch(
                    numRows,
                    Lists.newArrayList(fieldNode),
                    Lists.newArrayList(c1Validity, dataBufsX.get(0), dataBufsX.get(1), c2Validity));

    ArrowBuf selectionBuffer = buf(numRows * 2);
    SelectionVectorInt16 selectionVector = new SelectionVectorInt16(selectionBuffer);

    filter.evaluate(batch, selectionVector);

    int[] actual = selectionVectorToArray(selectionVector);
    releaseRecordBatch(batch);
    selectionBuffer.close();
    filter.close();
    Assert.assertArrayEquals(expected, actual);
  }

  @Test
  public void testSimpleInInt() throws GandivaException, Exception {
    Field c1 = Field.nullable("c1", int32);

    List<Field> argsSchema = Lists.newArrayList(c1);
    TreeNode inExpr =
            TreeBuilder.makeInExpressionInt32(TreeBuilder.makeField(c1), Sets.newHashSet(1, 2, 3, 4));

    Condition condition = TreeBuilder.makeCondition(inExpr);

    Schema schema = new Schema(argsSchema);
    Filter filter = Filter.make(schema, condition);

    int numRows = 16;
    byte[] validity = new byte[] {(byte) 255, 0};
    // second half is "undefined"
    int[] aValues = new int[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};
    int[] expected = {0, 1, 2, 3};

    ArrowBuf validitya = buf(validity);
    ArrowBuf validityb = buf(validity);
    ArrowBuf valuesa = intBuf(aValues);

    ArrowFieldNode fieldNode = new ArrowFieldNode(numRows, 0);
    ArrowRecordBatch batch =
            new ArrowRecordBatch(
                    numRows,
                    Lists.newArrayList(fieldNode),
                    Lists.newArrayList(validitya, valuesa, validityb));

    ArrowBuf selectionBuffer = buf(numRows * 2);
    SelectionVectorInt16 selectionVector = new SelectionVectorInt16(selectionBuffer);

    filter.evaluate(batch, selectionVector);

    // free buffers
    int[] actual = selectionVectorToArray(selectionVector);
    releaseRecordBatch(batch);
    selectionBuffer.close();
    filter.close();
    Assert.assertArrayEquals(expected, actual);
  }

  @Test
  public void testSimpleSV16() throws GandivaException, Exception {
    Field a = Field.nullable("a", int32);
    Field b = Field.nullable("b", int32);
    List<Field> args = Lists.newArrayList(a, b);

    Condition condition = TreeBuilder.makeCondition("less_than", args);

    Schema schema = new Schema(args);
    Filter filter = Filter.make(schema, condition);

    int numRows = 16;
    byte[] validity = new byte[] {(byte) 255, 0};
    // second half is "undefined"
    int[] aValues = new int[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};
    int[] bValues = new int[] {2, 1, 4, 3, 6, 5, 8, 7, 10, 9, 12, 11, 14, 13, 14, 15};
    int[] expected = {0, 2, 4, 6};

    verifyTestCase(filter, numRows, validity, aValues, bValues, expected);
  }

  @Test
  public void testSimpleSV16_AllMatched() throws GandivaException, Exception {
    Field a = Field.nullable("a", int32);
    Field b = Field.nullable("b", int32);
    List<Field> args = Lists.newArrayList(a, b);

    Condition condition = TreeBuilder.makeCondition("less_than", args);

    Schema schema = new Schema(args);
    Filter filter = Filter.make(schema, condition);

    int numRows = 32;

    byte[] validity = new byte[numRows / 8];

    IntStream.range(0, numRows / 8).forEach(i -> validity[i] = (byte) 255);

    int[] aValues = new int[numRows];
    IntStream.range(0, numRows).forEach(i -> aValues[i] = i);

    int[] bValues = new int[numRows];
    IntStream.range(0, numRows).forEach(i -> bValues[i] = i + 1);

    int[] expected = new int[numRows];
    IntStream.range(0, numRows).forEach(i -> expected[i] = i);

    verifyTestCase(filter, numRows, validity, aValues, bValues, expected);
  }

  @Test
  public void testSimpleSV16_GreaterThan64Recs() throws GandivaException, Exception {
    Field a = Field.nullable("a", int32);
    Field b = Field.nullable("b", int32);
    List<Field> args = Lists.newArrayList(a, b);

    Condition condition = TreeBuilder.makeCondition("greater_than", args);

    Schema schema = new Schema(args);
    Filter filter = Filter.make(schema, condition);

    int numRows = 1000;

    byte[] validity = new byte[numRows / 8];

    IntStream.range(0, numRows / 8).forEach(i -> validity[i] = (byte) 255);

    int[] aValues = new int[numRows];
    IntStream.range(0, numRows).forEach(i -> aValues[i] = i);

    int[] bValues = new int[numRows];
    IntStream.range(0, numRows).forEach(i -> bValues[i] = i + 1);

    aValues[0] = 5;
    bValues[0] = 0;

    int[] expected = {0};

    verifyTestCase(filter, numRows, validity, aValues, bValues, expected);
  }

  @Test
  public void testSimpleSV32() throws GandivaException, Exception {
    Field a = Field.nullable("a", int32);
    Field b = Field.nullable("b", int32);
    List<Field> args = Lists.newArrayList(a, b);

    Condition condition = TreeBuilder.makeCondition("less_than", args);

    Schema schema = new Schema(args);
    Filter filter = Filter.make(schema, condition);

    int numRows = 16;
    byte[] validity = new byte[] {(byte) 255, 0};
    // second half is "undefined"
    int[] aValues = new int[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};
    int[] bValues = new int[] {2, 1, 4, 3, 6, 5, 8, 7, 10, 9, 12, 11, 14, 13, 14, 15};
    int[] expected = {0, 2, 4, 6};

    verifyTestCase(filter, numRows, validity, aValues, bValues, expected);
  }

  @Test
  public void testSimpleFilterWithNoOptimisation() throws GandivaException, Exception {
    Field a = Field.nullable("a", int32);
    Field b = Field.nullable("b", int32);
    List<Field> args = Lists.newArrayList(a, b);

    Condition condition = TreeBuilder.makeCondition("less_than", args);

    Schema schema = new Schema(args);
    Filter filter = Filter.make(schema, condition, false);

    int numRows = 16;
    byte[] validity = new byte[] {(byte) 255, 0};
    // second half is "undefined"
    int[] aValues = new int[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};
    int[] bValues = new int[] {2, 1, 4, 3, 6, 5, 8, 7, 10, 9, 12, 11, 14, 13, 14, 15};
    int[] expected = {0, 2, 4, 6};

    verifyTestCase(filter, numRows, validity, aValues, bValues, expected);
  }

  private void verifyTestCase(
      Filter filter, int numRows, byte[] validity, int[] aValues, int[] bValues, int[] expected)
      throws GandivaException {
    ArrowBuf validitya = buf(validity);
    ArrowBuf valuesa = intBuf(aValues);
    ArrowBuf validityb = buf(validity);
    ArrowBuf valuesb = intBuf(bValues);
    ArrowRecordBatch batch =
        new ArrowRecordBatch(
            numRows,
            Lists.newArrayList(new ArrowFieldNode(numRows, 0), new ArrowFieldNode(numRows, 0)),
            Lists.newArrayList(validitya, valuesa, validityb, valuesb));

    ArrowBuf selectionBuffer = buf(numRows * 2);
    SelectionVectorInt16 selectionVector = new SelectionVectorInt16(selectionBuffer);

    filter.evaluate(batch, selectionVector);

    // free buffers
    int[] actual = selectionVectorToArray(selectionVector);
    releaseRecordBatch(batch);
    selectionBuffer.close();
    filter.close();

    Assert.assertArrayEquals(expected, actual);
  }
}
