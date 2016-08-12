/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.arrow.vector;

import com.google.common.base.Preconditions;
import com.google.flatbuffers.FlatBufferBuilder;
import org.apache.arrow.flatbuf.Field;
import org.apache.arrow.flatbuf.Int;
import org.apache.arrow.flatbuf.Type;

public class Types {
  public static void main(String[] args) {
    FlatBufferBuilder b = new FlatBufferBuilder();
    int nameOffset = b.createString("name");
    int typeOffset = Int.createInt(b, 32, true);
    int[] data = new int[] {};
    int childrenOffset = Field.createChildrenVector(b, data);
    int field = Field.createField(b, nameOffset, true, Type.Int, typeOffset, childrenOffset);
    b.finish(field);

    Field f = Field.getRootAsField(b.dataBuffer());
    assert "name".equals(f.name());
    Int integer = (Int) f.type(new Int());
    Preconditions.checkState(f.typeType() == Type.Int);
    Preconditions.checkState(integer.bitWidth() == 32);
    Preconditions.checkState(f.childrenLength() == 0);
  }
}
