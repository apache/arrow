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

/**
 * Used to represent expression tree nodes representing double constants. Used in the expression (x
 * + 5.0)
 */
class DoubleNode implements TreeNode {
  private final Double value;

  DoubleNode(Double value) {
    this.value = value;
  }

  @Override
  public GandivaTypes.TreeNode toProtobuf() throws GandivaException {
    GandivaTypes.DoubleNode.Builder doubleBuilder = GandivaTypes.DoubleNode.newBuilder();
    doubleBuilder.setValue(value.doubleValue());

    GandivaTypes.TreeNode.Builder builder = GandivaTypes.TreeNode.newBuilder();
    builder.setDoubleNode(doubleBuilder.build());
    return builder.build();
  }
}
