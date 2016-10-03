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
package org.apache.arrow.vector.pojo;

import static org.apache.arrow.flatbuf.Precision.DOUBLE;
import static org.apache.arrow.flatbuf.Precision.SINGLE;
import static org.junit.Assert.assertEquals;

import org.apache.arrow.flatbuf.TimeUnit;
import org.apache.arrow.flatbuf.UnionMode;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.ArrowType.FloatingPoint;
import org.apache.arrow.vector.types.pojo.ArrowType.Int;
import org.apache.arrow.vector.types.pojo.ArrowType.List;
import org.apache.arrow.vector.types.pojo.ArrowType.Struct_;
import org.apache.arrow.vector.types.pojo.ArrowType.Timestamp;
import org.apache.arrow.vector.types.pojo.ArrowType.Union;
import org.apache.arrow.vector.types.pojo.ArrowType.Utf8;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.flatbuffers.FlatBufferBuilder;

/**
 * Test conversion between Flatbuf and Pojo field representations
 */
public class TestConvert {

  @Test
  public void simple() {
    Field initialField = new Field("a", true, new Int(32, true), null);
    run(initialField);
  }

  @Test
  public void complex() {
    ImmutableList.Builder<Field> childrenBuilder = ImmutableList.builder();
    childrenBuilder.add(new Field("child1", true, Utf8.INSTANCE, null));
    childrenBuilder.add(new Field("child2", true, new FloatingPoint(SINGLE), ImmutableList.<Field>of()));

    Field initialField = new Field("a", true, Struct_.INSTANCE, childrenBuilder.build());
    run(initialField);
  }

  @Test
  public void schema() {
    ImmutableList.Builder<Field> childrenBuilder = ImmutableList.builder();
    childrenBuilder.add(new Field("child1", true, Utf8.INSTANCE, null));
    childrenBuilder.add(new Field("child2", true, new FloatingPoint(SINGLE), ImmutableList.<Field>of()));
    Schema initialSchema = new Schema(childrenBuilder.build());
    run(initialSchema);
  }

  @Test
  public void nestedSchema() {
    ImmutableList.Builder<Field> childrenBuilder = ImmutableList.builder();
    childrenBuilder.add(new Field("child1", true, Utf8.INSTANCE, null));
    childrenBuilder.add(new Field("child2", true, new FloatingPoint(SINGLE), ImmutableList.<Field>of()));
    childrenBuilder.add(new Field("child3", true, new Struct_(), ImmutableList.<Field>of(
        new Field("child3.1", true, Utf8.INSTANCE, null),
        new Field("child3.2", true, new FloatingPoint(DOUBLE), ImmutableList.<Field>of())
        )));
    childrenBuilder.add(new Field("child4", true, new List(), ImmutableList.<Field>of(
        new Field("child4.1", true, Utf8.INSTANCE, null)
        )));
    childrenBuilder.add(new Field("child5", true, new Union(UnionMode.Sparse, new int[] { MinorType.TIMESTAMP.ordinal(), MinorType.FLOAT8.ordinal() } ), ImmutableList.<Field>of(
        new Field("child5.1", true, new Timestamp(TimeUnit.MILLISECOND), null),
        new Field("child5.2", true, new FloatingPoint(DOUBLE), ImmutableList.<Field>of())
        )));
    Schema initialSchema = new Schema(childrenBuilder.build());
    run(initialSchema);
  }

  private void run(Field initialField) {
    FlatBufferBuilder builder = new FlatBufferBuilder();
    builder.finish(initialField.getField(builder));
    org.apache.arrow.flatbuf.Field flatBufField = org.apache.arrow.flatbuf.Field.getRootAsField(builder.dataBuffer());
    Field finalField = Field.convertField(flatBufField);
    assertEquals(initialField, finalField);
  }

  private void run(Schema initialSchema) {
    FlatBufferBuilder builder = new FlatBufferBuilder();
    builder.finish(initialSchema.getSchema(builder));
    org.apache.arrow.flatbuf.Schema flatBufSchema = org.apache.arrow.flatbuf.Schema.getRootAsSchema(builder.dataBuffer());
    Schema finalSchema = Schema.convertSchema(flatBufSchema);
    assertEquals(initialSchema, finalSchema);
  }
}
