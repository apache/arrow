/**
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

import org.apache.arrow.gandiva.expression.ExpressionTree;
import org.apache.arrow.gandiva.expression.TreeBuilder;
import org.apache.arrow.gandiva.expression.TreeNode;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Test;

import java.util.List;

public class MicroBenchmarkTest extends BaseEvaluatorTest {
  @Test
  public void testAdd3() throws Exception {
    Field x = Field.nullable("x", int32);
    Field N2x = Field.nullable("N2x", int32);
    Field N3x = Field.nullable("N3x", int32);

    // x + N2x + N3x
    TreeNode add1 = TreeBuilder.makeFunction("add", Lists.newArrayList(TreeBuilder.makeField(x), TreeBuilder.makeField(N2x)), int32);
    TreeNode add = TreeBuilder.makeFunction("add", Lists.newArrayList(add1, TreeBuilder.makeField(N3x)), int32);
    ExpressionTree expr = TreeBuilder.makeExpression(add, x);

    List<Field> cols = Lists.newArrayList(x, N2x, N3x);
    Schema schema = new Schema(cols);

    long timeTaken = timedEvaluate(new Int32DataAndVectorGenerator(allocator),
            schema,
            Lists.newArrayList(expr),
            100 * MILLION, 16 * THOUSAND,
            4);
    System.out.println("Time taken for evaluating 100m records of add3 is " + timeTaken + "ms");
  }

  @Test
  public void testIf() throws Exception {
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
     * when x < 110 then 10
     * when x < 120 then 11
     * when x < 130 then 12
     * when x < 140 then 13
     * when x < 150 then 14
     * when x < 160 then 15
     * when x < 170 then 16
     * when x < 180 then 17
     * when x < 190 then 18
     * when x < 200 then 19
     * else 20
     */
    Field x = Field.nullable("x", int32);
    TreeNode x_node = TreeBuilder.makeField(x);

    // if (x < 100) then 9 else 10
    int returnValue = 20;
    TreeNode topNode = TreeBuilder.makeLiteral(returnValue);
    int compareWith = 200;
    while (compareWith >= 10) {
      // cond (x < compareWith)
      TreeNode condNode = TreeBuilder.makeFunction("less_than",
              Lists.newArrayList(x_node, TreeBuilder.makeLiteral(compareWith)),
              boolType);
      topNode = TreeBuilder.makeIf(
              condNode,                             // cond (x < compareWith)
              TreeBuilder.makeLiteral(returnValue), // then returnValue
              topNode,                              // else topNode
              int32);
      compareWith -= 10;
      returnValue--;
    }

    ExpressionTree expr = TreeBuilder.makeExpression(topNode, x);
    Schema schema = new Schema(Lists.newArrayList(x));

    long timeTaken = timedEvaluate(new BoundedInt32DataAndVectorGenerator(allocator, 250),
            schema,
            Lists.newArrayList(expr),
            100 * MILLION, 16 * THOUSAND,
            4);
    System.out.println("Time taken for evaluating 100m records of nestedIf is " + timeTaken + "ms");
  }
}