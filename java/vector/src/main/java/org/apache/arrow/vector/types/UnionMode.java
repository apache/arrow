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

package org.apache.arrow.vector.types;

/**
 * Different memory layouts for Union Vectors.
 */
public enum UnionMode {
  /**
   * Each child vector is the same length as the overall vector, and there is one 8-bit integer buffer to indicate
   * the index of a child vector to use at any given position.
   */
  Sparse(org.apache.arrow.flatbuf.UnionMode.Sparse),
  /**
   * Each child vector is of variable width.  The parent vector contains both an child index vector (like in
   * {@link #Sparse}) and in addition a slot index buffer to determine the offset into the child vector indicated
   * by the index vector.
   */
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
