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

package org.apache.arrow.gandiva.expression;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;

import java.util.ArrayList;
import java.util.List;

public class TreeBuilder {
  /**
   * Helper functions to create literal constants.
   */
  public static TreeNode makeLiteral(Boolean booleanConstant) {
    return new BooleanNode(booleanConstant);
  }

  public static TreeNode makeLiteral(Float floatConstant) {
    return new FloatNode(floatConstant);
  }

  public static TreeNode makeLiteral(Double doubleConstant) {
    return new DoubleNode(doubleConstant);
  }

  public static TreeNode makeLiteral(Integer integerConstant) {
    return new IntNode(integerConstant);
  }

  public static TreeNode makeLiteral(Long longConstant) {
    return new LongNode(longConstant);
  }

  /**
   * create a null literal.
   */
  public static TreeNode makeNull(ArrowType type) {
    return new NullNode(type);
  }

  /**
   * Invoke this function to create a node representing a field, e.g. a column name.
   *
   * @param field represents the input argument - includes the name and type of the field
   * @return Node representing a field
   */
  public static TreeNode makeField(Field field) {
    return new FieldNode(field);
  }

  /**
   * Invoke this function to create a node representing a function.
   *
   * @param function Name of the function, e.g. add
   * @param children The arguments to the function
   * @param retType  The type of the return value of the operator
   * @return Node representing a function
   */
  public static TreeNode makeFunction(String function,
                                      List<TreeNode> children,
                                      ArrowType retType) {
    return new FunctionNode(function, children, retType);
  }

  /**
   * Invoke this function to create a node representing an if-clause.
   *
   * @param condition Node representing the condition
   * @param thenNode  Node representing the if-block
   * @param elseNode  Node representing the else-block
   * @param retType   Return type of the node
   * @return Node representing an if-clause
   */
  public static TreeNode makeIf(TreeNode condition,
                                TreeNode thenNode,
                                TreeNode elseNode,
                                ArrowType retType) {
    return new IfNode(condition, thenNode, elseNode, retType);
  }

  /**
   * Invoke this function to create a node representing an and-clause.
   *
   * @param nodes  Nodes in the 'and' clause.
   * @return Node representing an and-clause
   */
  public static TreeNode makeAnd(List<TreeNode> nodes) {
    return new AndNode(nodes);
  }

  /**
   * Invoke this function to create a node representing an or-clause.
   *
   * @param nodes  Nodes in the 'or' clause.
   * @return Node representing an or-clause
   */
  public static TreeNode makeOr(List<TreeNode> nodes) {
    return new OrNode(nodes);
  }

  /**
   * Invoke this function to create an expression tree.
   *
   * @param root         is returned by a call to MakeField, MakeFunction, or MakeIf
   * @param resultField represents the return value of the expression
   * @return ExpressionTree referring to the root of an expression tree
   */
  public static ExpressionTree makeExpression(TreeNode root,
                                              Field resultField) {
    return new ExpressionTree(root, resultField);
  }

  /**
   * Short cut to create an expression tree involving a single function, e.g. a+b+c.
   *
   * @param function    Name of the function, e.g. add()
   * @param inFields   In arguments to the function
   * @param resultField represents the return value of the expression
   */
  public static ExpressionTree makeExpression(String function,
                                              List<Field> inFields,
                                              Field resultField) {
    List<TreeNode> children = new ArrayList<TreeNode>(inFields.size());
    for (Field field : inFields) {
      children.add(makeField(field));
    }

    TreeNode root = makeFunction(function, children, resultField.getType());
    return makeExpression(root, resultField);
  }
}
