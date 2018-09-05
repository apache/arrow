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

import static org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE;
import static org.apache.arrow.vector.types.FloatingPointPrecision.SINGLE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.UnionMode;
import org.apache.arrow.vector.types.pojo.ArrowType.FloatingPoint;
import org.apache.arrow.vector.types.pojo.ArrowType.Int;
import org.apache.arrow.vector.types.pojo.ArrowType.List;
import org.apache.arrow.vector.types.pojo.ArrowType.Struct;
import org.apache.arrow.vector.types.pojo.ArrowType.Timestamp;
import org.apache.arrow.vector.types.pojo.ArrowType.Union;
import org.apache.arrow.vector.types.pojo.ArrowType.Utf8;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
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
    Field initialField = new Field("a", FieldType.nullable(new Int(32, true)), null);
    run(initialField);
  }

  @Test
  public void complex() {
    ImmutableList.Builder<Field> childrenBuilder = ImmutableList.builder();
    childrenBuilder.add(new Field("child1", FieldType.nullable(Utf8.INSTANCE), null));
    childrenBuilder.add(new Field("child2", FieldType.nullable(new FloatingPoint(SINGLE)), ImmutableList.<Field>of()));

    Field initialField = new Field("a", FieldType.nullable(Struct.INSTANCE), childrenBuilder.build());
    run(initialField);
  }

  @Test
  public void list() throws Exception {
    ImmutableList.Builder<Field> childrenBuilder = ImmutableList.builder();
    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
         ListVector writeVector = ListVector.empty("list", allocator);
         FixedSizeListVector writeFixedVector = FixedSizeListVector.empty("fixedlist", 5, allocator)) {
      Field listVectorField = writeVector.getField();
      childrenBuilder.add(listVectorField);
      Field listFixedVectorField = writeFixedVector.getField();
      childrenBuilder.add(listFixedVectorField);
    }

    Field initialField = new Field("a", FieldType.nullable(Struct.INSTANCE), childrenBuilder.build());
    ImmutableList.Builder<Field> parentBuilder = ImmutableList.builder();
    parentBuilder.add(initialField);
    FlatBufferBuilder builder = new FlatBufferBuilder();
    builder.finish(initialField.getField(builder));
    org.apache.arrow.flatbuf.Field flatBufField = org.apache.arrow.flatbuf.Field.getRootAsField(builder.dataBuffer());
    Field finalField = Field.convertField(flatBufField);
    assertEquals(initialField, finalField);
    assertFalse(finalField.toString().contains("[DEFAULT]"));

    Schema initialSchema = new Schema(parentBuilder.build());
    String jsonSchema = initialSchema.toJson();
    String modifiedSchema = jsonSchema.replace("$data$", "[DEFAULT]");

    Schema tempSchema = Schema.fromJSON(modifiedSchema);
    FlatBufferBuilder schemaBuilder = new FlatBufferBuilder();
    org.apache.arrow.vector.types.pojo.Schema schema =
        new org.apache.arrow.vector.types.pojo.Schema(tempSchema.getFields());
    schemaBuilder.finish(schema.getSchema(schemaBuilder));
    Schema finalSchema = Schema.deserialize(ByteBuffer.wrap(schemaBuilder.sizedByteArray()));
    assertFalse(finalSchema.toString().contains("[DEFAULT]"));
  }

  @Test
  public void schema() {
    ImmutableList.Builder<Field> childrenBuilder = ImmutableList.builder();
    childrenBuilder.add(new Field("child1", FieldType.nullable(Utf8.INSTANCE), null));
    childrenBuilder.add(new Field("child2", FieldType.nullable(new FloatingPoint(SINGLE)), ImmutableList.<Field>of()));
    Schema initialSchema = new Schema(childrenBuilder.build());
    run(initialSchema);
  }

  @Test
  public void schemaMetadata() {
    ImmutableList.Builder<Field> childrenBuilder = ImmutableList.builder();
    childrenBuilder.add(new Field("child1", FieldType.nullable(Utf8.INSTANCE), null));
    childrenBuilder.add(new Field("child2", FieldType.nullable(new FloatingPoint(SINGLE)), ImmutableList.<Field>of()));
    Map<String, String> metadata = new HashMap<>();
    metadata.put("key1", "value1");
    metadata.put("key2", "value2");
    Schema initialSchema = new Schema(childrenBuilder.build(), metadata);
    run(initialSchema);
  }

  @Test
  public void nestedSchema() {
    ImmutableList.Builder<Field> childrenBuilder = ImmutableList.builder();
    childrenBuilder.add(new Field("child1", FieldType.nullable(Utf8.INSTANCE), null));
    childrenBuilder.add(new Field("child2", FieldType.nullable(new FloatingPoint(SINGLE)), ImmutableList.<Field>of()));
    childrenBuilder.add(new Field("child3", FieldType.nullable(new Struct()), ImmutableList.<Field>of(
        new Field("child3.1", FieldType.nullable(Utf8.INSTANCE), null),
        new Field("child3.2", FieldType.nullable(new FloatingPoint(DOUBLE)), ImmutableList.<Field>of())
    )));
    childrenBuilder.add(new Field("child4", FieldType.nullable(new List()), ImmutableList.<Field>of(
        new Field("child4.1", FieldType.nullable(Utf8.INSTANCE), null)
    )));
    childrenBuilder.add(new Field("child5", FieldType.nullable(
        new Union(UnionMode.Sparse, new int[] {MinorType.TIMESTAMPMILLI.ordinal(), MinorType.FLOAT8.ordinal()})),
          ImmutableList.<Field>of(
            new Field("child5.1", FieldType.nullable(new Timestamp(TimeUnit.MILLISECOND, null)), null),
            new Field("child5.2", FieldType.nullable(new FloatingPoint(DOUBLE)), ImmutableList.<Field>of()),
            new Field("child5.3", true, new Timestamp(TimeUnit.MILLISECOND, "UTC"), null)
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
    org.apache.arrow.flatbuf.Schema flatBufSchema =
        org.apache.arrow.flatbuf.Schema.getRootAsSchema(builder.dataBuffer());
    Schema finalSchema = Schema.convertSchema(flatBufSchema);
    assertEquals(initialSchema, finalSchema);
  }
}
