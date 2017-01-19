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
package org.apache.arrow.vector.file;

import org.apache.arrow.flatbuf.Block;
import org.apache.arrow.vector.schema.FBSerializable;

import com.google.flatbuffers.FlatBufferBuilder;

public class ArrowBlock implements FBSerializable {

  private final long offset;
  private final int length;

  public ArrowBlock(long offset, int length) {
    super();
    this.offset = offset;
    this.length = length;
  }

  public long getOffset() {
    return offset;
  }

  public int getLength() {
    return length;
  }

  @Override
  public int writeTo(FlatBufferBuilder builder) {
    return Block.createBlock(builder, offset, length);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + (length ^ (length >>> 32));
    result = prime * result + (int) (offset ^ (offset >>> 32));
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    ArrowBlock other = (ArrowBlock) obj;
    if (length != other.length)
      return false;
    if (offset != other.offset)
      return false;
    return true;
  }
}
