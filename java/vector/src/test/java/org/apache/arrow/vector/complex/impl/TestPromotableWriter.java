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
package org.apache.arrow.vector.complex.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.DirtyRootAllocator;
import org.apache.arrow.vector.LargeVarBinaryVector;
import org.apache.arrow.vector.LargeVarCharVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.NonNullableStructVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.UnionVector;
import org.apache.arrow.vector.complex.writer.BaseWriter.StructWriter;
import org.apache.arrow.vector.holders.DurationHolder;
import org.apache.arrow.vector.holders.FixedSizeBinaryHolder;
import org.apache.arrow.vector.holders.NullableTimeStampMilliTZHolder;
import org.apache.arrow.vector.holders.TimeStampMilliTZHolder;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.ArrowType.ArrowTypeID;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.Text;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestPromotableWriter {
  private static final String EMPTY_SCHEMA_PATH = "";

  private BufferAllocator allocator;

  @BeforeEach
  public void init() {
    allocator = new DirtyRootAllocator(Long.MAX_VALUE, (byte) 100);
  }

  @AfterEach
  public void terminate() throws Exception {
    allocator.close();
  }

  @Test
  public void testPromoteToUnion() throws Exception {

    try (final NonNullableStructVector container =
            NonNullableStructVector.empty(EMPTY_SCHEMA_PATH, allocator);
        final StructVector v = container.addOrGetStruct("test");
        final PromotableWriter writer = new PromotableWriter(v, container)) {

      container.allocateNew();

      writer.start();

      writer.setPosition(0);
      writer.bit("A").writeBit(0);

      writer.setPosition(1);
      writer.bit("A").writeBit(1);

      writer.decimal("dec", 10, 10);

      writer.setPosition(2);
      writer.integer("A").writeInt(10);

      // we don't write anything in 3

      writer.setPosition(4);
      writer.integer("A").writeInt(100);

      writer.setPosition(5);
      writer.timeStampMilliTZ("A").writeTimeStampMilliTZ(123123);

      // Also try the holder version for timeStampMilliTZ
      writer.setPosition(6);
      TimeStampMilliTZHolder tsmtzHolder = new TimeStampMilliTZHolder();
      // This has to be UTC since the vector above was initialized using the non holder
      // version that defaults to UTC.
      tsmtzHolder.timezone = "UTC";
      tsmtzHolder.value = 12345L;
      writer.timeStampMilliTZ("A").write(tsmtzHolder);

      writer.setPosition(7);
      DurationHolder durationHolder = new DurationHolder();
      durationHolder.unit = TimeUnit.SECOND;
      durationHolder.value = 444413;
      writer.duration("A").write(durationHolder);

      writer.setPosition(8);
      ArrowBuf buf = allocator.buffer(4);
      buf.setInt(0, 18978);
      FixedSizeBinaryHolder binHolder = new FixedSizeBinaryHolder();
      binHolder.byteWidth = 4;
      binHolder.buffer = buf;
      writer.fixedSizeBinary("A", 4).write(binHolder);

      writer.end();

      container.setValueCount(9);

      final UnionVector uv = v.getChild("A", UnionVector.class);

      assertFalse(uv.isNull(0), "0 shouldn't be null");
      assertEquals(false, uv.getObject(0));

      assertFalse(uv.isNull(1), "1 shouldn't be null");
      assertEquals(true, uv.getObject(1));

      assertFalse(uv.isNull(2), "2 shouldn't be null");
      assertEquals(10, uv.getObject(2));

      assertNull(uv.getObject(3), "3 should be null");

      assertFalse(uv.isNull(4), "4 shouldn't be null");
      assertEquals(100, uv.getObject(4));

      assertFalse(uv.isNull(5), "5 shouldn't be null");
      assertEquals(123123L, uv.getObject(5));

      assertFalse(uv.isNull(6), "6 shouldn't be null");
      NullableTimeStampMilliTZHolder readBackHolder = new NullableTimeStampMilliTZHolder();
      uv.getTimeStampMilliTZVector().get(6, readBackHolder);
      assertEquals(12345L, readBackHolder.value);
      assertEquals("UTC", readBackHolder.timezone);

      assertFalse(uv.isNull(7), "7 shouldn't be null");
      assertEquals(444413L, ((java.time.Duration) uv.getObject(7)).getSeconds());

      assertFalse(uv.isNull(8), "8 shouldn't be null");
      assertEquals(
          18978,
          ByteBuffer.wrap(uv.getFixedSizeBinaryVector().get(8))
              .order(ByteOrder.nativeOrder())
              .getInt());

      container.clear();
      container.allocateNew();

      ComplexWriterImpl newWriter = new ComplexWriterImpl(EMPTY_SCHEMA_PATH, container);

      StructWriter newStructWriter = newWriter.rootAsStruct();

      newStructWriter.start();

      newStructWriter.setPosition(2);
      newStructWriter.integer("A").writeInt(10);

      Field childField1 = container.getField().getChildren().get(0).getChildren().get(0);
      Field childField2 = container.getField().getChildren().get(0).getChildren().get(1);
      assertEquals(
          ArrowTypeID.Union,
          childField1.getType().getTypeID(),
          "Child field should be union type: " + childField1.getName());
      assertEquals(
          ArrowTypeID.Decimal,
          childField2.getType().getTypeID(),
          "Child field should be decimal type: " + childField2.getName());

      buf.close();
    }
  }

  @Test
  public void testNoPromoteFloat4ToUnionWithNull() throws Exception {

    try (final NonNullableStructVector container =
            NonNullableStructVector.empty(EMPTY_SCHEMA_PATH, allocator);
        final StructVector v = container.addOrGetStruct("test");
        final PromotableWriter writer = new PromotableWriter(v, container)) {

      container.allocateNew();

      writer.start();
      writer.list("list").startList();
      writer.list("list").endList();
      writer.end();

      FieldType childTypeOfListInContainer =
          container
              .getField()
              .getChildren()
              .get(0)
              .getChildren()
              .get(0)
              .getChildren()
              .get(0)
              .getFieldType();

      // create a listvector with same type as list in container to, say, hold a copy
      // this will be a nullvector
      ListVector lv = ListVector.empty("name", allocator);
      lv.addOrGetVector(childTypeOfListInContainer);
      assertEquals(childTypeOfListInContainer.getType(), Types.MinorType.NULL.getType());
      assertEquals(
          lv.getChildrenFromFields().get(0).getMinorType().getType(),
          Types.MinorType.NULL.getType());

      writer.start();
      writer.list("list").startList();
      writer.list("list").float4().writeFloat4(1.36f);
      writer.list("list").endList();
      writer.end();

      container.setValueCount(2);

      childTypeOfListInContainer =
          container
              .getField()
              .getChildren()
              .get(0)
              .getChildren()
              .get(0)
              .getChildren()
              .get(0)
              .getFieldType();

      // repeat but now the type in container has been changed from null to float
      // we expect same behaviour from listvector
      lv.addOrGetVector(childTypeOfListInContainer);
      assertEquals(childTypeOfListInContainer.getType(), Types.MinorType.FLOAT4.getType());
      assertEquals(
          lv.getChildrenFromFields().get(0).getMinorType().getType(),
          Types.MinorType.FLOAT4.getType());

      lv.close();
    }
  }

  @Test
  public void testNoPromoteTimeStampMilliTZToUnionWithNull() throws Exception {

    try (final NonNullableStructVector container =
            NonNullableStructVector.empty(EMPTY_SCHEMA_PATH, allocator);
        final StructVector v = container.addOrGetStruct("test");
        final PromotableWriter writer = new PromotableWriter(v, container)) {

      container.allocateNew();

      writer.start();
      writer.list("list").startList();
      writer.list("list").endList();
      writer.end();

      FieldType childTypeOfListInContainer =
          container
              .getField()
              .getChildren()
              .get(0)
              .getChildren()
              .get(0)
              .getChildren()
              .get(0)
              .getFieldType();

      // create a listvector with same type as list in container to, say, hold a copy
      // this will be a nullvector
      ListVector lv = ListVector.empty("name", allocator);
      lv.addOrGetVector(childTypeOfListInContainer);
      assertEquals(childTypeOfListInContainer.getType(), Types.MinorType.NULL.getType());
      assertEquals(
          lv.getChildrenFromFields().get(0).getMinorType().getType(),
          Types.MinorType.NULL.getType());

      writer.start();
      writer.list("list").startList();
      TimeStampMilliTZHolder holder = new TimeStampMilliTZHolder();
      holder.value = 12341234L;
      holder.timezone = "FakeTimeZone";
      writer.list("list").timeStampMilliTZ().write(holder);

      // Test that we get an exception when the timezone doesn't match
      holder.timezone = "SomeTimeZone";
      IllegalArgumentException ex =
          assertThrows(
              IllegalArgumentException.class,
              () -> writer.list("list").timeStampMilliTZ().write(holder));
      assertEquals(
          "holder.timezone: SomeTimeZone not equal to vector timezone: FakeTimeZone",
          ex.getMessage());

      writer.list("list").endList();
      writer.end();

      container.setValueCount(2);

      childTypeOfListInContainer =
          container
              .getField()
              .getChildren()
              .get(0)
              .getChildren()
              .get(0)
              .getChildren()
              .get(0)
              .getFieldType();

      // repeat but now the type in container has been changed from null to float
      // we expect same behaviour from listvector
      lv.addOrGetVector(childTypeOfListInContainer);
      assertEquals(
          childTypeOfListInContainer.getType(),
          new ArrowType.Timestamp(TimeUnit.MILLISECOND, "FakeTimeZone"));
      assertEquals(
          lv.getChildrenFromFields().get(0).getField().getType(),
          new ArrowType.Timestamp(TimeUnit.MILLISECOND, "FakeTimeZone"));

      lv.close();
    }
  }

  @Test
  public void testNoPromoteDurationToUnionWithNull() throws Exception {

    try (final NonNullableStructVector container =
            NonNullableStructVector.empty(EMPTY_SCHEMA_PATH, allocator);
        final StructVector v = container.addOrGetStruct("test");
        final PromotableWriter writer = new PromotableWriter(v, container)) {

      container.allocateNew();

      writer.start();
      writer.list("list").startList();
      writer.list("list").endList();
      writer.end();

      FieldType childTypeOfListInContainer =
          container
              .getField()
              .getChildren()
              .get(0)
              .getChildren()
              .get(0)
              .getChildren()
              .get(0)
              .getFieldType();

      // create a listvector with same type as list in container to, say, hold a copy
      // this will be a nullvector
      ListVector lv = ListVector.empty("name", allocator);
      lv.addOrGetVector(childTypeOfListInContainer);
      assertEquals(childTypeOfListInContainer.getType(), Types.MinorType.NULL.getType());
      assertEquals(
          lv.getChildrenFromFields().get(0).getMinorType().getType(),
          Types.MinorType.NULL.getType());

      writer.start();
      writer.list("list").startList();
      DurationHolder holder = new DurationHolder();
      holder.unit = TimeUnit.NANOSECOND;
      holder.value = 567657L;
      writer.list("list").duration().write(holder);

      // Test that we get an exception when the unit doesn't match
      holder.unit = TimeUnit.MICROSECOND;
      IllegalArgumentException ex =
          assertThrows(
              IllegalArgumentException.class, () -> writer.list("list").duration().write(holder));
      assertEquals(
          "holder.unit: MICROSECOND not equal to vector unit: NANOSECOND", ex.getMessage());

      writer.list("list").endList();
      writer.end();

      container.setValueCount(2);

      childTypeOfListInContainer =
          container
              .getField()
              .getChildren()
              .get(0)
              .getChildren()
              .get(0)
              .getChildren()
              .get(0)
              .getFieldType();

      // repeat but now the type in container has been changed from null to float
      // we expect same behaviour from listvector
      lv.addOrGetVector(childTypeOfListInContainer);
      assertEquals(
          childTypeOfListInContainer.getType(), new ArrowType.Duration(TimeUnit.NANOSECOND));
      assertEquals(
          lv.getChildrenFromFields().get(0).getField().getType(),
          new ArrowType.Duration(TimeUnit.NANOSECOND));

      lv.close();
    }
  }

  @Test
  public void testNoPromoteFixedSizeBinaryToUnionWithNull() throws Exception {

    try (final NonNullableStructVector container =
            NonNullableStructVector.empty(EMPTY_SCHEMA_PATH, allocator);
        final StructVector v = container.addOrGetStruct("test");
        final PromotableWriter writer = new PromotableWriter(v, container)) {

      container.allocateNew();

      writer.start();
      writer.list("list").startList();
      writer.list("list").endList();
      writer.end();

      FieldType childTypeOfListInContainer =
          container
              .getField()
              .getChildren()
              .get(0)
              .getChildren()
              .get(0)
              .getChildren()
              .get(0)
              .getFieldType();

      // create a listvector with same type as list in container to, say, hold a copy
      // this will be a nullvector
      ListVector lv = ListVector.empty("name", allocator);
      lv.addOrGetVector(childTypeOfListInContainer);
      assertEquals(childTypeOfListInContainer.getType(), Types.MinorType.NULL.getType());
      assertEquals(
          lv.getChildrenFromFields().get(0).getMinorType().getType(),
          Types.MinorType.NULL.getType());

      writer.start();
      writer.list("list").startList();
      ArrowBuf buf = allocator.buffer(4);
      buf.setInt(0, 22222);
      FixedSizeBinaryHolder holder = new FixedSizeBinaryHolder();
      holder.byteWidth = 4;
      holder.buffer = buf;
      writer.list("list").fixedSizeBinary().write(holder);

      // Test that we get an exception when the unit doesn't match
      holder.byteWidth = 7;
      IllegalArgumentException ex =
          assertThrows(
              IllegalArgumentException.class,
              () -> writer.list("list").fixedSizeBinary().write(holder));
      assertEquals("holder.byteWidth: 7 not equal to vector byteWidth: 4", ex.getMessage());

      writer.list("list").endList();
      writer.end();

      container.setValueCount(2);

      childTypeOfListInContainer =
          container
              .getField()
              .getChildren()
              .get(0)
              .getChildren()
              .get(0)
              .getChildren()
              .get(0)
              .getFieldType();

      // repeat but now the type in container has been changed from null to float
      // we expect same behaviour from listvector
      lv.addOrGetVector(childTypeOfListInContainer);
      assertEquals(childTypeOfListInContainer.getType(), new ArrowType.FixedSizeBinary(4));
      assertEquals(
          lv.getChildrenFromFields().get(0).getField().getType(), new ArrowType.FixedSizeBinary(4));

      lv.close();
      buf.close();
    }
  }

  @Test
  public void testPromoteLargeVarCharHelpersOnStruct() throws Exception {
    try (final NonNullableStructVector container =
            NonNullableStructVector.empty(EMPTY_SCHEMA_PATH, allocator);
        final StructVector v = container.addOrGetStruct("test");
        final PromotableWriter writer = new PromotableWriter(v, container)) {
      container.allocateNew();

      writer.start();
      writer.setPosition(0);
      writer.largeVarChar("c").writeLargeVarChar(new Text("foo"));
      writer.setPosition(1);
      writer.largeVarChar("c").writeLargeVarChar("foo2");
      writer.end();

      final LargeVarCharVector uv = v.getChild("c", LargeVarCharVector.class);
      assertEquals("foo", Objects.requireNonNull(uv.getObject(0)).toString());
      assertEquals("foo2", Objects.requireNonNull(uv.getObject(1)).toString());
    }
  }

  @Test
  public void testPromoteVarCharHelpersOnStruct() throws Exception {
    try (final NonNullableStructVector container =
            NonNullableStructVector.empty(EMPTY_SCHEMA_PATH, allocator);
        final StructVector v = container.addOrGetStruct("test");
        final PromotableWriter writer = new PromotableWriter(v, container)) {
      container.allocateNew();

      writer.start();
      writer.setPosition(0);
      writer.varChar("c").writeVarChar(new Text("foo"));
      writer.setPosition(1);
      writer.varChar("c").writeVarChar("foo2");
      writer.end();

      final VarCharVector uv = v.getChild("c", VarCharVector.class);
      assertEquals("foo", Objects.requireNonNull(uv.getObject(0)).toString());
      assertEquals("foo2", Objects.requireNonNull(uv.getObject(1)).toString());
    }
  }

  @Test
  public void testPromoteVarCharHelpersDirect() throws Exception {
    try (final NonNullableStructVector container =
            NonNullableStructVector.empty(EMPTY_SCHEMA_PATH, allocator);
        final StructVector v = container.addOrGetStruct("test");
        final PromotableWriter writer = new PromotableWriter(v, container)) {
      container.allocateNew();

      writer.start();
      writer.setPosition(0);
      writer.writeVarChar(new Text("foo"));
      writer.setPosition(1);
      writer.writeVarChar("foo2");
      writer.end();

      // The "test" vector in the parent container should have been replaced with a UnionVector.
      UnionVector promotedVector = container.getChild("test", UnionVector.class);
      VarCharVector vector = promotedVector.getVarCharVector();
      assertEquals("foo", Objects.requireNonNull(vector.getObject(0)).toString());
      assertEquals("foo2", Objects.requireNonNull(vector.getObject(1)).toString());
    }
  }

  @Test
  public void testPromoteLargeVarCharHelpersDirect() throws Exception {
    try (final NonNullableStructVector container =
            NonNullableStructVector.empty(EMPTY_SCHEMA_PATH, allocator);
        final StructVector v = container.addOrGetStruct("test");
        final PromotableWriter writer = new PromotableWriter(v, container)) {
      container.allocateNew();

      writer.start();
      writer.setPosition(0);
      writer.writeLargeVarChar(new Text("foo"));
      writer.setPosition(1);
      writer.writeLargeVarChar("foo2");
      writer.end();

      // The "test" vector in the parent container should have been replaced with a UnionVector.
      UnionVector promotedVector = container.getChild("test", UnionVector.class);
      LargeVarCharVector vector = promotedVector.getLargeVarCharVector();
      assertEquals("foo", Objects.requireNonNull(vector.getObject(0)).toString());
      assertEquals("foo2", Objects.requireNonNull(vector.getObject(1)).toString());
    }
  }

  @Test
  public void testPromoteVarBinaryHelpersOnStruct() throws Exception {
    try (final NonNullableStructVector container =
            NonNullableStructVector.empty(EMPTY_SCHEMA_PATH, allocator);
        final StructVector v = container.addOrGetStruct("test");
        final PromotableWriter writer = new PromotableWriter(v, container)) {
      container.allocateNew();

      writer.start();
      writer.setPosition(0);
      writer.varBinary("c").writeVarBinary("row1".getBytes(StandardCharsets.UTF_8));
      writer.setPosition(1);
      writer
          .varBinary("c")
          .writeVarBinary(
              "row2".getBytes(StandardCharsets.UTF_8),
              0,
              "row2".getBytes(StandardCharsets.UTF_8).length);
      writer.setPosition(2);
      writer
          .varBinary("c")
          .writeVarBinary(ByteBuffer.wrap("row3".getBytes(StandardCharsets.UTF_8)));
      writer.setPosition(3);
      writer
          .varBinary("c")
          .writeVarBinary(
              ByteBuffer.wrap("row4".getBytes(StandardCharsets.UTF_8)),
              0,
              "row4".getBytes(StandardCharsets.UTF_8).length);
      writer.end();

      final VarBinaryVector uv = v.getChild("c", VarBinaryVector.class);
      assertEquals("row1", new String(Objects.requireNonNull(uv.get(0)), StandardCharsets.UTF_8));
      assertEquals("row2", new String(Objects.requireNonNull(uv.get(1)), StandardCharsets.UTF_8));
      assertEquals("row3", new String(Objects.requireNonNull(uv.get(2)), StandardCharsets.UTF_8));
      assertEquals("row4", new String(Objects.requireNonNull(uv.get(3)), StandardCharsets.UTF_8));
    }
  }

  @Test
  public void testPromoteVarBinaryHelpersDirect() throws Exception {
    try (final NonNullableStructVector container =
            NonNullableStructVector.empty(EMPTY_SCHEMA_PATH, allocator);
        final StructVector v = container.addOrGetStruct("test");
        final PromotableWriter writer = new PromotableWriter(v, container)) {
      container.allocateNew();

      writer.start();
      writer.setPosition(0);
      writer.writeVarBinary("row1".getBytes(StandardCharsets.UTF_8));
      writer.setPosition(1);
      writer.writeVarBinary(
          "row2".getBytes(StandardCharsets.UTF_8),
          0,
          "row2".getBytes(StandardCharsets.UTF_8).length);
      writer.setPosition(2);
      writer.writeVarBinary(ByteBuffer.wrap("row3".getBytes(StandardCharsets.UTF_8)));
      writer.setPosition(3);
      writer.writeVarBinary(
          ByteBuffer.wrap("row4".getBytes(StandardCharsets.UTF_8)),
          0,
          "row4".getBytes(StandardCharsets.UTF_8).length);
      writer.end();

      // The "test" vector in the parent container should have been replaced with a UnionVector.
      UnionVector promotedVector = container.getChild("test", UnionVector.class);
      VarBinaryVector uv = promotedVector.getVarBinaryVector();
      assertEquals("row1", new String(Objects.requireNonNull(uv.get(0)), StandardCharsets.UTF_8));
      assertEquals("row2", new String(Objects.requireNonNull(uv.get(1)), StandardCharsets.UTF_8));
      assertEquals("row3", new String(Objects.requireNonNull(uv.get(2)), StandardCharsets.UTF_8));
      assertEquals("row4", new String(Objects.requireNonNull(uv.get(3)), StandardCharsets.UTF_8));
    }
  }

  @Test
  public void testPromoteLargeVarBinaryHelpersOnStruct() throws Exception {
    try (final NonNullableStructVector container =
            NonNullableStructVector.empty(EMPTY_SCHEMA_PATH, allocator);
        final StructVector v = container.addOrGetStruct("test");
        final PromotableWriter writer = new PromotableWriter(v, container)) {
      container.allocateNew();

      writer.start();
      writer.setPosition(0);
      writer.largeVarBinary("c").writeLargeVarBinary("row1".getBytes(StandardCharsets.UTF_8));
      writer.setPosition(1);
      writer
          .largeVarBinary("c")
          .writeLargeVarBinary(
              "row2".getBytes(StandardCharsets.UTF_8),
              0,
              "row2".getBytes(StandardCharsets.UTF_8).length);
      writer.setPosition(2);
      writer
          .largeVarBinary("c")
          .writeLargeVarBinary(ByteBuffer.wrap("row3".getBytes(StandardCharsets.UTF_8)));
      writer.setPosition(3);
      writer
          .largeVarBinary("c")
          .writeLargeVarBinary(
              ByteBuffer.wrap("row4".getBytes(StandardCharsets.UTF_8)),
              0,
              "row4".getBytes(StandardCharsets.UTF_8).length);
      writer.end();

      final LargeVarBinaryVector uv = v.getChild("c", LargeVarBinaryVector.class);
      assertEquals("row1", new String(Objects.requireNonNull(uv.get(0)), StandardCharsets.UTF_8));
      assertEquals("row2", new String(Objects.requireNonNull(uv.get(1)), StandardCharsets.UTF_8));
      assertEquals("row3", new String(Objects.requireNonNull(uv.get(2)), StandardCharsets.UTF_8));
      assertEquals("row4", new String(Objects.requireNonNull(uv.get(3)), StandardCharsets.UTF_8));
    }
  }

  @Test
  public void testPromoteLargeVarBinaryHelpersDirect() throws Exception {
    try (final NonNullableStructVector container =
            NonNullableStructVector.empty(EMPTY_SCHEMA_PATH, allocator);
        final StructVector v = container.addOrGetStruct("test");
        final PromotableWriter writer = new PromotableWriter(v, container)) {
      container.allocateNew();

      writer.start();
      writer.setPosition(0);
      writer.writeLargeVarBinary("row1".getBytes(StandardCharsets.UTF_8));
      writer.setPosition(1);
      writer.writeLargeVarBinary(
          "row2".getBytes(StandardCharsets.UTF_8),
          0,
          "row2".getBytes(StandardCharsets.UTF_8).length);
      writer.setPosition(2);
      writer.writeLargeVarBinary(ByteBuffer.wrap("row3".getBytes(StandardCharsets.UTF_8)));
      writer.setPosition(3);
      writer.writeLargeVarBinary(
          ByteBuffer.wrap("row4".getBytes(StandardCharsets.UTF_8)),
          0,
          "row4".getBytes(StandardCharsets.UTF_8).length);
      writer.end();

      // The "test" vector in the parent container should have been replaced with a UnionVector.
      UnionVector promotedVector = container.getChild("test", UnionVector.class);
      LargeVarBinaryVector uv = promotedVector.getLargeVarBinaryVector();
      assertEquals("row1", new String(Objects.requireNonNull(uv.get(0)), StandardCharsets.UTF_8));
      assertEquals("row2", new String(Objects.requireNonNull(uv.get(1)), StandardCharsets.UTF_8));
      assertEquals("row3", new String(Objects.requireNonNull(uv.get(2)), StandardCharsets.UTF_8));
      assertEquals("row4", new String(Objects.requireNonNull(uv.get(3)), StandardCharsets.UTF_8));
    }
  }
}
