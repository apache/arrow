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
package org.apache.arrow.flatbuf;

import com.google.flatbuffers.BaseVector;
import com.google.flatbuffers.BooleanVector;
import com.google.flatbuffers.ByteVector;
import com.google.flatbuffers.Constants;
import com.google.flatbuffers.DoubleVector;
import com.google.flatbuffers.FlatBufferBuilder;
import com.google.flatbuffers.FloatVector;
import com.google.flatbuffers.IntVector;
import com.google.flatbuffers.LongVector;
import com.google.flatbuffers.ShortVector;
import com.google.flatbuffers.StringVector;
import com.google.flatbuffers.Struct;
import com.google.flatbuffers.Table;
import com.google.flatbuffers.UnionVector;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

@SuppressWarnings("unused")
public final class Block extends Struct {
  public void __init(int _i, ByteBuffer _bb) { __reset(_i, _bb); }
  public Block __assign(int _i, ByteBuffer _bb) { __init(_i, _bb); return this; }

  /**
   * Index to the start of the RecordBlock (note this is past the Message header)
   */
  public long offset() { return bb.getLong(bb_pos + 0); }
  /**
   * Length of the metadata
   */
  public int metaDataLength() { return bb.getInt(bb_pos + 8); }
  /**
   * Length of the data (this is aligned so there can be a gap between this and
   * the metadata).
   */
  public long bodyLength() { return bb.getLong(bb_pos + 16); }

  public static int createBlock(FlatBufferBuilder builder, long offset, int metaDataLength, long bodyLength) {
    builder.prep(8, 24);
    builder.putLong(bodyLength);
    builder.pad(4);
    builder.putInt(metaDataLength);
    builder.putLong(offset);
    return builder.offset();
  }

  public static final class Vector extends BaseVector {
    public Vector __assign(int _vector, int _element_size, ByteBuffer _bb) { __reset(_vector, _element_size, _bb); return this; }

    public Block get(int j) { return get(new Block(), j); }
    public Block get(Block obj, int j) {  return obj.__assign(__element(j), bb); }
  }
}

