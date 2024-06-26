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

import org.apache.arrow.gandiva.exceptions.GandivaException;
import org.apache.arrow.gandiva.ipc.GandivaTypes;
import org.apache.arrow.vector.types.pojo.ArrowType;

/** Node representing a if-then-else block expression. */
class IfNode implements TreeNode {
  private final TreeNode condition;
  private final TreeNode thenNode;
  private final TreeNode elseNode;
  private final ArrowType retType;

  IfNode(TreeNode condition, TreeNode thenNode, TreeNode elseNode, ArrowType retType) {
    this.condition = condition;
    this.thenNode = thenNode;
    this.elseNode = elseNode;
    this.retType = retType;
  }

  @Override
  public GandivaTypes.TreeNode toProtobuf() throws GandivaException {
    GandivaTypes.IfNode.Builder ifNodeBuilder = GandivaTypes.IfNode.newBuilder();
    ifNodeBuilder.setCond(condition.toProtobuf());
    ifNodeBuilder.setThenNode(thenNode.toProtobuf());
    ifNodeBuilder.setElseNode(elseNode.toProtobuf());
    ifNodeBuilder.setReturnType(ArrowTypeHelper.arrowTypeToProtobuf(retType));

    GandivaTypes.TreeNode.Builder builder = GandivaTypes.TreeNode.newBuilder();
    builder.setIfNode(ifNodeBuilder.build());
    return builder.build();
  }
}
