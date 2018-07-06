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

import java.nio.charset.Charset;

/**
 * Used to represent expression tree nodes representing utf8 constants.
 */
class StringNode implements TreeNode {
  private static final Charset charset = Charset.forName("UTF-8");
  private final String value;

  public StringNode(String value) {
    this.value = value;
  }

  @Override
  public GandivaTypes.TreeNode toProtobuf() throws GandivaException {
    GandivaTypes.StringNode stringNode = GandivaTypes.StringNode.newBuilder()
        .setValue(ByteString.copyFrom(value.getBytes(charset)))
        .build();

    return GandivaTypes.TreeNode.newBuilder()
        .setStringNode(stringNode)
        .build();
  }
}
