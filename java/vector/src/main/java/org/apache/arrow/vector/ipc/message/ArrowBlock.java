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
package org.apache.arrow.vector.ipc.message;

import com.google.flatbuffers.FlatBufferBuilder;
import org.apache.arrow.flatbuf.Block;

/** Metadata for an arrow message in a channel. */
public class ArrowBlock implements FBSerializable {

  private final long offset;
  private final int metadataLength;
  private final long bodyLength;

  /**
   * Constructs a new instance.
   *
   * @param offset The offset into the channel file where the block was written.
   * @param metadataLength The length of the flatbuffer metadata in the block.
   * @param bodyLength The length of data in the block.
   */
  public ArrowBlock(long offset, int metadataLength, long bodyLength) {
    super();
    this.offset = offset;
    this.metadataLength = metadataLength;
    this.bodyLength = bodyLength;
  }

  public long getOffset() {
    return offset;
  }

  public int getMetadataLength() {
    return metadataLength;
  }

  public long getBodyLength() {
    return bodyLength;
  }

  @Override
  public int writeTo(FlatBufferBuilder builder) {
    return Block.createBlock(builder, offset, metadataLength, bodyLength);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + (int) (bodyLength ^ (bodyLength >>> 32));
    result = prime * result + metadataLength;
    result = prime * result + (int) (offset ^ (offset >>> 32));
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    ArrowBlock other = (ArrowBlock) obj;
    if (bodyLength != other.bodyLength) {
      return false;
    }
    if (metadataLength != other.metadataLength) {
      return false;
    }
    if (offset != other.offset) {
      return false;
    }
    return true;
  }
}
