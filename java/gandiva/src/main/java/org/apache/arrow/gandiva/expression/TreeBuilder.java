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

package org.apache.arrow.gandiva.expression;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class TreeBuilder {
    private static final Logger LOGGER = LoggerFactory.getLogger(TreeBuilder.class);

    /**
     * Invoke this function to create a node representing a field, e.g. a column name
     *
     * @param field represents the input argument - includes the name and type of the field
     * @return Node representing a field
     */
    public static TreeNode MakeField(Field field) {
        return new FieldNode(field);
    }

    /**
     * Invoke this function to create a node representing a function
     *
     * @param function Name of the binary operator, e.g. add
     * @param children The arguments to the function
     * @param retType The type of the return value of the operator
     * @return Node representing a function
     */
    public static TreeNode MakeFunction(String function,
                                        List<TreeNode> children,
                                        ArrowType retType) {
        return new FunctionNode(function, children, retType);
    }

    /**
     * Invoke this function to create a node representing an if-clause
     *
     * @param condition Node representing the condition
     * @param thenNode Node representing the if-block
     * @param elseNode Node representing the else-block
     * @param retType Return type of the node
     * @return Node representing an if-clause
     */
    public static TreeNode MakeIf(TreeNode condition,
                                  TreeNode thenNode,
                                  TreeNode elseNode,
                                  ArrowType retType) {
        return new IfNode(condition, thenNode, elseNode, retType);
    }

    /**
     * Invoke this function to create an expression tree
     *
     * @param root is returned by a call to MakeField, MakeFunction, or MakeIf
     * @param result_field represents the return value of the expression
     * @return ExpressionTree referring to the root of an expression tree
     */
    public static ExpressionTree MakeExpression(TreeNode root,
                                                Field result_field) {
        return new ExpressionTree(root, result_field);
    }

    /**
     * Short cut to create an expression tree involving a single function, e.g. a+b+c
     *
     * @param function: Name of the function, e.g. add()
     * @param in_fields: In arguments to the function
     * @param result_field represents the return value of the expression
     */
    public static ExpressionTree MakeExpression(String function,
                                                List<Field> in_fields,
                                                Field result_field) {
        List<TreeNode> children = new ArrayList<TreeNode>(in_fields.size());
        for(Field field : in_fields) {
            children.add(MakeField(field));
        }

        TreeNode n = MakeFunction(function, children, result_field.getType());
        return MakeExpression(n, result_field);
    }

}
