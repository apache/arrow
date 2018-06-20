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

import org.apache.arrow.gandiva.exceptions.GandivaException;
import org.apache.arrow.gandiva.ipc.GandivaTypes;
import org.apache.arrow.vector.types.pojo.Field;

/**
 * Opaque class representing an expression.
 */
public class ExpressionTree {
  private final TreeNode root;
  private final Field resultField;

  ExpressionTree(TreeNode root, Field resultField) {
    this.root = root;
    this.resultField = resultField;
  }

  /**
   * Converts an expression tree into a protobuf.
   * @return A protobuf representing the expression tree
   */
  public GandivaTypes.ExpressionRoot toProtobuf() throws GandivaException {
    GandivaTypes.ExpressionRoot.Builder builder = GandivaTypes.ExpressionRoot.newBuilder();
    builder.setRoot(root.toProtobuf());
    builder.setResultType(ArrowTypeHelper.arrowFieldToProtobuf(resultField));
    return builder.build();
  }
}
