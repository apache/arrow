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

package org.apache.arrow.gandiva.expression;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.arrow.gandiva.exceptions.GandivaException;
import org.apache.arrow.gandiva.ipc.GandivaTypes;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.junit.Test;

public class TreeBuilderTest {

  @Test
  public void testMakeLiteral() throws GandivaException {
    TreeNode n = TreeBuilder.makeLiteral(Boolean.TRUE);
    GandivaTypes.TreeNode node = n.toProtobuf();

    assertEquals(true, node.getBooleanNode().getValue());

    n = TreeBuilder.makeLiteral(new Integer(10));
    node = n.toProtobuf();
    assertEquals(10, node.getIntNode().getValue());

    n = TreeBuilder.makeLiteral(new Long(50));
    node = n.toProtobuf();
    assertEquals(50, node.getLongNode().getValue());

    Float f = new Float(2.5);
    n = TreeBuilder.makeLiteral(f);
    node = n.toProtobuf();
    assertEquals(f.floatValue(), node.getFloatNode().getValue(), 0.1);

    Double d = new Double(3.3);
    n = TreeBuilder.makeLiteral(d);
    node = n.toProtobuf();
    assertEquals(d.doubleValue(), node.getDoubleNode().getValue(), 0.1);

    String s = new String("hello");
    n = TreeBuilder.makeStringLiteral(s);
    node = n.toProtobuf();
    assertArrayEquals(s.getBytes(), node.getStringNode().getValue().toByteArray());

    byte[] b = new String("hello").getBytes();
    n = TreeBuilder.makeBinaryLiteral(b);
    node = n.toProtobuf();
    assertArrayEquals(b, node.getBinaryNode().getValue().toByteArray());
  }

  @Test
  public void testMakeNull() throws GandivaException {
    TreeNode n = TreeBuilder.makeNull(new ArrowType.Bool());
    GandivaTypes.TreeNode node = n.toProtobuf();
    assertEquals(
        GandivaTypes.GandivaType.BOOL_VALUE, node.getNullNode().getType().getType().getNumber());

    n = TreeBuilder.makeNull(new ArrowType.Int(32, true));
    node = n.toProtobuf();
    assertEquals(
        GandivaTypes.GandivaType.INT32_VALUE, node.getNullNode().getType().getType().getNumber());

    n = TreeBuilder.makeNull(new ArrowType.Int(64, false));
    node = n.toProtobuf();
    assertEquals(
        GandivaTypes.GandivaType.UINT64_VALUE, node.getNullNode().getType().getType().getNumber());

    n = TreeBuilder.makeNull(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE));
    node = n.toProtobuf();
    assertEquals(
        GandivaTypes.GandivaType.FLOAT_VALUE, node.getNullNode().getType().getType().getNumber());
  }

  @Test
  public void testMakeField() throws GandivaException {
    TreeNode n = TreeBuilder.makeField(Field.nullable("a", new ArrowType.Int(32, true)));
    GandivaTypes.TreeNode node = n.toProtobuf();

    assertEquals("a", node.getFieldNode().getField().getName());
    assertEquals(
        GandivaTypes.GandivaType.INT32_VALUE,
        node.getFieldNode().getField().getType().getType().getNumber());
  }

  @Test
  public void testMakeFunction() throws GandivaException {
    TreeNode a = TreeBuilder.makeField(Field.nullable("a", new ArrowType.Int(64, false)));
    TreeNode b = TreeBuilder.makeField(Field.nullable("b", new ArrowType.Int(64, false)));
    List<TreeNode> args = new ArrayList<TreeNode>(2);
    args.add(a);
    args.add(b);

    TreeNode addNode = TreeBuilder.makeFunction("add", args, new ArrowType.Int(64, false));
    GandivaTypes.TreeNode node = addNode.toProtobuf();

    assertTrue(node.hasFnNode());
    assertEquals("add", node.getFnNode().getFunctionName());
    assertEquals("a", node.getFnNode().getInArgsList().get(0).getFieldNode().getField().getName());
    assertEquals("b", node.getFnNode().getInArgsList().get(1).getFieldNode().getField().getName());
    assertEquals(
        GandivaTypes.GandivaType.UINT64_VALUE,
        node.getFnNode().getReturnType().getType().getNumber());
  }

  @Test
  public void testMakeIf() throws GandivaException {
    Field a = Field.nullable("a", new ArrowType.Int(64, false));
    Field b = Field.nullable("b", new ArrowType.Int(64, false));
    TreeNode aNode = TreeBuilder.makeField(a);
    TreeNode bNode = TreeBuilder.makeField(b);
    List<TreeNode> args = new ArrayList<TreeNode>(2);
    args.add(aNode);
    args.add(bNode);

    ArrowType retType = new ArrowType.Bool();
    TreeNode cond = TreeBuilder.makeFunction("greater_than", args, retType);
    TreeNode ifNode = TreeBuilder.makeIf(cond, aNode, bNode, retType);

    GandivaTypes.TreeNode node = ifNode.toProtobuf();

    assertTrue(node.hasIfNode());
    assertEquals("greater_than", node.getIfNode().getCond().getFnNode().getFunctionName());
    assertEquals(a.getName(), node.getIfNode().getThenNode().getFieldNode().getField().getName());
    assertEquals(b.getName(), node.getIfNode().getElseNode().getFieldNode().getField().getName());
    assertEquals(
        GandivaTypes.GandivaType.BOOL_VALUE,
        node.getIfNode().getReturnType().getType().getNumber());
  }

  @Test
  public void testMakeAnd() throws GandivaException {
    TreeNode a = TreeBuilder.makeField(Field.nullable("a", new ArrowType.Bool()));
    TreeNode b = TreeBuilder.makeField(Field.nullable("b", new ArrowType.Bool()));
    List<TreeNode> args = new ArrayList<TreeNode>(2);
    args.add(a);
    args.add(b);

    TreeNode andNode = TreeBuilder.makeAnd(args);
    GandivaTypes.TreeNode node = andNode.toProtobuf();

    assertTrue(node.hasAndNode());
    assertEquals(2, node.getAndNode().getArgsList().size());
    assertEquals("a", node.getAndNode().getArgsList().get(0).getFieldNode().getField().getName());
    assertEquals("b", node.getAndNode().getArgsList().get(1).getFieldNode().getField().getName());
  }

  @Test
  public void testMakeOr() throws GandivaException {
    TreeNode a = TreeBuilder.makeField(Field.nullable("a", new ArrowType.Bool()));
    TreeNode b = TreeBuilder.makeField(Field.nullable("b", new ArrowType.Bool()));
    List<TreeNode> args = new ArrayList<TreeNode>(2);
    args.add(a);
    args.add(b);

    TreeNode orNode = TreeBuilder.makeOr(args);
    GandivaTypes.TreeNode node = orNode.toProtobuf();

    assertTrue(node.hasOrNode());
    assertEquals(2, node.getOrNode().getArgsList().size());
    assertEquals("a", node.getOrNode().getArgsList().get(0).getFieldNode().getField().getName());
    assertEquals("b", node.getOrNode().getArgsList().get(1).getFieldNode().getField().getName());
  }

  @Test
  public void testExpression() throws GandivaException {
    Field a = Field.nullable("a", new ArrowType.Int(64, false));
    Field b = Field.nullable("b", new ArrowType.Int(64, false));
    TreeNode aNode = TreeBuilder.makeField(a);
    TreeNode bNode = TreeBuilder.makeField(b);
    List<TreeNode> args = new ArrayList<TreeNode>(2);
    args.add(aNode);
    args.add(bNode);

    ArrowType retType = new ArrowType.Bool();
    TreeNode cond = TreeBuilder.makeFunction("greater_than", args, retType);
    TreeNode ifNode = TreeBuilder.makeIf(cond, aNode, bNode, retType);

    ExpressionTree expr = TreeBuilder.makeExpression(ifNode, Field.nullable("c", retType));

    GandivaTypes.ExpressionRoot root = expr.toProtobuf();

    assertTrue(root.getRoot().hasIfNode());
    assertEquals(
        "greater_than", root.getRoot().getIfNode().getCond().getFnNode().getFunctionName());
    assertEquals("c", root.getResultType().getName());
    assertEquals(
        GandivaTypes.GandivaType.BOOL_VALUE, root.getResultType().getType().getType().getNumber());
  }

  @Test
  public void testExpression2() throws GandivaException {
    Field a = Field.nullable("a", new ArrowType.Int(64, false));
    Field b = Field.nullable("b", new ArrowType.Int(64, false));
    List<Field> args = new ArrayList<Field>(2);
    args.add(a);
    args.add(b);

    Field c = Field.nullable("c", new ArrowType.Int(64, false));
    ExpressionTree expr = TreeBuilder.makeExpression("add", args, c);
    GandivaTypes.ExpressionRoot root = expr.toProtobuf();

    GandivaTypes.TreeNode node = root.getRoot();

    assertEquals("c", root.getResultType().getName());
    assertTrue(node.hasFnNode());
    assertEquals("add", node.getFnNode().getFunctionName());
    assertEquals("a", node.getFnNode().getInArgsList().get(0).getFieldNode().getField().getName());
    assertEquals("b", node.getFnNode().getInArgsList().get(1).getFieldNode().getField().getName());
    assertEquals(
        GandivaTypes.GandivaType.UINT64_VALUE,
        node.getFnNode().getReturnType().getType().getNumber());
  }

  @Test
  public void testExpressionWithAnd() throws GandivaException {
    TreeNode a = TreeBuilder.makeField(Field.nullable("a", new ArrowType.Bool()));
    TreeNode b = TreeBuilder.makeField(Field.nullable("b", new ArrowType.Bool()));
    List<TreeNode> args = new ArrayList<TreeNode>(2);
    args.add(a);
    args.add(b);

    TreeNode andNode = TreeBuilder.makeAnd(args);
    ExpressionTree expr =
        TreeBuilder.makeExpression(andNode, Field.nullable("c", new ArrowType.Bool()));
    GandivaTypes.ExpressionRoot root = expr.toProtobuf();

    assertTrue(root.getRoot().hasAndNode());
    assertEquals(
        "a", root.getRoot().getAndNode().getArgsList().get(0).getFieldNode().getField().getName());
    assertEquals(
        "b", root.getRoot().getAndNode().getArgsList().get(1).getFieldNode().getField().getName());
    assertEquals("c", root.getResultType().getName());
    assertEquals(
        GandivaTypes.GandivaType.BOOL_VALUE, root.getResultType().getType().getType().getNumber());
  }

  @Test
  public void testExpressionWithOr() throws GandivaException {
    TreeNode a = TreeBuilder.makeField(Field.nullable("a", new ArrowType.Bool()));
    TreeNode b = TreeBuilder.makeField(Field.nullable("b", new ArrowType.Bool()));
    List<TreeNode> args = new ArrayList<TreeNode>(2);
    args.add(a);
    args.add(b);

    TreeNode orNode = TreeBuilder.makeOr(args);
    ExpressionTree expr =
        TreeBuilder.makeExpression(orNode, Field.nullable("c", new ArrowType.Bool()));
    GandivaTypes.ExpressionRoot root = expr.toProtobuf();

    assertTrue(root.getRoot().hasOrNode());
    assertEquals(
        "a", root.getRoot().getOrNode().getArgsList().get(0).getFieldNode().getField().getName());
    assertEquals(
        "b", root.getRoot().getOrNode().getArgsList().get(1).getFieldNode().getField().getName());
    assertEquals("c", root.getResultType().getName());
    assertEquals(
        GandivaTypes.GandivaType.BOOL_VALUE, root.getResultType().getType().getType().getNumber());
  }

  @Test
  public void testCondition() throws GandivaException {
    Field a = Field.nullable("a", new ArrowType.Int(64, false));
    Field b = Field.nullable("b", new ArrowType.Int(64, false));

    TreeNode aNode = TreeBuilder.makeField(a);
    TreeNode bNode = TreeBuilder.makeField(b);
    List<TreeNode> args = new ArrayList<TreeNode>(2);
    args.add(aNode);
    args.add(bNode);

    TreeNode root = TreeBuilder.makeFunction("greater_than", args, new ArrowType.Bool());
    Condition condition = TreeBuilder.makeCondition(root);

    GandivaTypes.Condition conditionProto = condition.toProtobuf();
    assertTrue(conditionProto.getRoot().hasFnNode());
    assertEquals("greater_than", conditionProto.getRoot().getFnNode().getFunctionName());
    assertEquals(
        "a",
        conditionProto
            .getRoot()
            .getFnNode()
            .getInArgsList()
            .get(0)
            .getFieldNode()
            .getField()
            .getName());
    assertEquals(
        "b",
        conditionProto
            .getRoot()
            .getFnNode()
            .getInArgsList()
            .get(1)
            .getFieldNode()
            .getField()
            .getName());
  }

  @Test
  public void testCondition2() throws GandivaException {
    Field a = Field.nullable("a", new ArrowType.Int(64, false));
    Field b = Field.nullable("b", new ArrowType.Int(64, false));

    Condition condition = TreeBuilder.makeCondition("greater_than", Arrays.asList(a, b));

    GandivaTypes.Condition conditionProto = condition.toProtobuf();
    assertTrue(conditionProto.getRoot().hasFnNode());
    assertEquals("greater_than", conditionProto.getRoot().getFnNode().getFunctionName());
    assertEquals(
        "a",
        conditionProto
            .getRoot()
            .getFnNode()
            .getInArgsList()
            .get(0)
            .getFieldNode()
            .getField()
            .getName());
    assertEquals(
        "b",
        conditionProto
            .getRoot()
            .getFnNode()
            .getInArgsList()
            .get(1)
            .getFieldNode()
            .getField()
            .getName());
  }
}
