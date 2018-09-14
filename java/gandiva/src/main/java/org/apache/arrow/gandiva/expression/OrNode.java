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

package org.apache.arrow.gandiva.expression;

import org.apache.arrow.gandiva.exceptions.GandivaException;
import org.apache.arrow.gandiva.ipc.GandivaTypes;

import java.util.List;

class OrNode implements TreeNode {
  private final List<TreeNode> children;

  OrNode(List<TreeNode> children) {
    this.children = children;
  }

  @Override
  public GandivaTypes.TreeNode toProtobuf() throws GandivaException {
    GandivaTypes.OrNode.Builder orNode = GandivaTypes.OrNode.newBuilder();

    for (TreeNode arg : children) {
      orNode.addArgs(arg.toProtobuf());
    }

    GandivaTypes.TreeNode.Builder builder = GandivaTypes.TreeNode.newBuilder();
    builder.setOrNode(orNode.build());
    return builder.build();
  }
}
