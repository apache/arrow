// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
// 
// http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
// 
// automatically generated by the FlatBuffers compiler, do not modify

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

/**
 * ----------------------------------------------------------------------
 * Data structures for dense tensors
 * Shape data for a single axis in a tensor
 */
@SuppressWarnings("unused")
public final class TensorDim extends Table {
  public static void ValidateVersion() { Constants.FLATBUFFERS_24_3_25(); }
  public static TensorDim getRootAsTensorDim(ByteBuffer _bb) { return getRootAsTensorDim(_bb, new TensorDim()); }
  public static TensorDim getRootAsTensorDim(ByteBuffer _bb, TensorDim obj) { _bb.order(ByteOrder.LITTLE_ENDIAN); return (obj.__assign(_bb.getInt(_bb.position()) + _bb.position(), _bb)); }
  public void __init(int _i, ByteBuffer _bb) { __reset(_i, _bb); }
  public TensorDim __assign(int _i, ByteBuffer _bb) { __init(_i, _bb); return this; }

  /**
   * Length of dimension
   */
  public long size() { int o = __offset(4); return o != 0 ? bb.getLong(o + bb_pos) : 0L; }
  /**
   * Name of the dimension, optional
   */
  public String name() { int o = __offset(6); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer nameAsByteBuffer() { return __vector_as_bytebuffer(6, 1); }
  public ByteBuffer nameInByteBuffer(ByteBuffer _bb) { return __vector_in_bytebuffer(_bb, 6, 1); }

  public static int createTensorDim(FlatBufferBuilder builder,
      long size,
      int nameOffset) {
    builder.startTable(2);
    TensorDim.addSize(builder, size);
    TensorDim.addName(builder, nameOffset);
    return TensorDim.endTensorDim(builder);
  }

  public static void startTensorDim(FlatBufferBuilder builder) { builder.startTable(2); }
  public static void addSize(FlatBufferBuilder builder, long size) { builder.addLong(0, size, 0L); }
  public static void addName(FlatBufferBuilder builder, int nameOffset) { builder.addOffset(1, nameOffset, 0); }
  public static int endTensorDim(FlatBufferBuilder builder) {
    int o = builder.endTable();
    return o;
  }

  public static final class Vector extends BaseVector {
    public Vector __assign(int _vector, int _element_size, ByteBuffer _bb) { __reset(_vector, _element_size, _bb); return this; }

    public TensorDim get(int j) { return get(new TensorDim(), j); }
    public TensorDim get(TensorDim obj, int j) {  return obj.__assign(__indirect(__element(j), bb), bb); }
  }
}

