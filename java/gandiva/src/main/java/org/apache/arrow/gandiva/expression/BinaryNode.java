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

import com.google.protobuf.ByteString;

import org.apache.arrow.gandiva.exceptions.GandivaException;
import org.apache.arrow.gandiva.ipc.GandivaTypes;

/**
 * Used to represent expression tree nodes representing binary constants.
 */
class BinaryNode implements TreeNode {
  private final byte[] value;

  public BinaryNode(byte[] value) {
    this.value = value;
  }

  @Override
  public GandivaTypes.TreeNode toProtobuf() throws GandivaException {
    GandivaTypes.BinaryNode binaryNode = GandivaTypes.BinaryNode.newBuilder()
        .setValue(ByteString.copyFrom(value))
        .build();

    return GandivaTypes.TreeNode.newBuilder()
        .setBinaryNode(binaryNode)
        .build();
  }
}
