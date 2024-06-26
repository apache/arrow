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
public final class Int extends Table {
  public static void ValidateVersion() { Constants.FLATBUFFERS_24_3_25(); }
  public static Int getRootAsInt(ByteBuffer _bb) { return getRootAsInt(_bb, new Int()); }
  public static Int getRootAsInt(ByteBuffer _bb, Int obj) { _bb.order(ByteOrder.LITTLE_ENDIAN); return (obj.__assign(_bb.getInt(_bb.position()) + _bb.position(), _bb)); }
  public void __init(int _i, ByteBuffer _bb) { __reset(_i, _bb); }
  public Int __assign(int _i, ByteBuffer _bb) { __init(_i, _bb); return this; }

  public int bitWidth() { int o = __offset(4); return o != 0 ? bb.getInt(o + bb_pos) : 0; }
  public boolean isSigned() { int o = __offset(6); return o != 0 ? 0!=bb.get(o + bb_pos) : false; }

  public static int createInt(FlatBufferBuilder builder,
      int bitWidth,
      boolean isSigned) {
    builder.startTable(2);
    Int.addBitWidth(builder, bitWidth);
    Int.addIsSigned(builder, isSigned);
    return Int.endInt(builder);
  }

  public static void startInt(FlatBufferBuilder builder) { builder.startTable(2); }
  public static void addBitWidth(FlatBufferBuilder builder, int bitWidth) { builder.addInt(0, bitWidth, 0); }
  public static void addIsSigned(FlatBufferBuilder builder, boolean isSigned) { builder.addBoolean(1, isSigned, false); }
  public static int endInt(FlatBufferBuilder builder) {
    int o = builder.endTable();
    return o;
  }

  public static final class Vector extends BaseVector {
    public Vector __assign(int _vector, int _element_size, ByteBuffer _bb) { __reset(_vector, _element_size, _bb); return this; }

    public Int get(int j) { return get(new Int(), j); }
    public Int get(Int obj, int j) {  return obj.__assign(__indirect(__element(j), bb), bb); }
  }
}

