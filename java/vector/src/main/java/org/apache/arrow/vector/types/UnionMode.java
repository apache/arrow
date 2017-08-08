/*******************************************************************************
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
 ******************************************************************************/

package org.apache.arrow.vector.types;

public enum UnionMode {
  Sparse(org.apache.arrow.flatbuf.UnionMode.Sparse),
  Dense(org.apache.arrow.flatbuf.UnionMode.Dense);

  private static final UnionMode[] valuesByFlatbufId = new UnionMode[UnionMode.values().length];

  static {
    for (UnionMode v : UnionMode.values()) {
      valuesByFlatbufId[v.flatbufID] = v;
    }
  }

  private final short flatbufID;

  private UnionMode(short flatbufID) {
    this.flatbufID = flatbufID;
  }

  public short getFlatbufID() {
    return flatbufID;
  }

  public static UnionMode fromFlatbufID(short id) {
    return valuesByFlatbufId[id];
  }
}
