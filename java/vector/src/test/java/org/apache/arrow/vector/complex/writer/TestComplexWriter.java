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
package org.apache.arrow.vector.complex.writer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.LargeVarBinaryVector;
import org.apache.arrow.vector.LargeVarCharVector;
import org.apache.arrow.vector.SchemaChangeCallBack;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.ViewVarCharVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.ListViewVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.NonNullableStructVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.UnionVector;
import org.apache.arrow.vector.complex.impl.ComplexWriterImpl;
import org.apache.arrow.vector.complex.impl.NullableStructReaderImpl;
import org.apache.arrow.vector.complex.impl.NullableStructWriter;
import org.apache.arrow.vector.complex.impl.SingleStructReaderImpl;
import org.apache.arrow.vector.complex.impl.SingleStructWriter;
import org.apache.arrow.vector.complex.impl.UnionListReader;
import org.apache.arrow.vector.complex.impl.UnionListViewReader;
import org.apache.arrow.vector.complex.impl.UnionListViewWriter;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.complex.impl.UnionMapReader;
import org.apache.arrow.vector.complex.impl.UnionReader;
import org.apache.arrow.vector.complex.impl.UnionWriter;
import org.apache.arrow.vector.complex.reader.BaseReader.StructReader;
import org.apache.arrow.vector.complex.reader.BigIntReader;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.complex.reader.Float4Reader;
import org.apache.arrow.vector.complex.reader.Float8Reader;
import org.apache.arrow.vector.complex.reader.IntReader;
import org.apache.arrow.vector.complex.writer.BaseWriter.ComplexWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter.ListWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter.MapWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter.StructWriter;
import org.apache.arrow.vector.holders.DecimalHolder;
import org.apache.arrow.vector.holders.DurationHolder;
import org.apache.arrow.vector.holders.FixedSizeBinaryHolder;
import org.apache.arrow.vector.holders.IntHolder;
import org.apache.arrow.vector.holders.NullableDurationHolder;
import org.apache.arrow.vector.holders.NullableFixedSizeBinaryHolder;
import org.apache.arrow.vector.holders.NullableTimeStampMilliTZHolder;
import org.apache.arrow.vector.holders.NullableTimeStampNanoTZHolder;
import org.apache.arrow.vector.holders.TimeStampMilliTZHolder;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.ArrowType.ArrowTypeID;
import org.apache.arrow.vector.types.pojo.ArrowType.Int;
import org.apache.arrow.vector.types.pojo.ArrowType.Struct;
import org.apache.arrow.vector.types.pojo.ArrowType.Timestamp;
import org.apache.arrow.vector.types.pojo.ArrowType.Union;
import org.apache.arrow.vector.types.pojo.ArrowType.Utf8;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.CallBack;
import org.apache.arrow.vector.util.DecimalUtility;
import org.apache.arrow.vector.util.JsonStringArrayList;
import org.apache.arrow.vector.util.JsonStringHashMap;
import org.apache.arrow.vector.util.Text;
import org.apache.arrow.vector.util.TransferPair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestComplexWriter {

  private BufferAllocator allocator;

  private static final int COUNT = 100;

  @BeforeEach
  public void init() {
    allocator = new RootAllocator(Integer.MAX_VALUE);
  }

  @AfterEach
  public void terminate() throws Exception {
    allocator.close();
  }

  /* Test Utils */

  private void checkNullableStruct(NonNullableStructVector structVector) {
    StructReader rootReader = new SingleStructReaderImpl(structVector).reader("root");
    for (int i = 0; i < COUNT; i++) {
      rootReader.setPosition(i);
      assertTrue(rootReader.isSet(), "index is set: " + i);
      FieldReader struct = rootReader.reader("struct");
      if (i % 2 == 0) {
        assertTrue(struct.isSet(), "index is set: " + i);
        assertNotNull(struct.readObject(), "index is set: " + i);
        assertEquals(i, struct.reader("nested").readLong().longValue());
      } else {
        assertFalse(struct.isSet(), "index is not set: " + i);
        assertNull(struct.readObject(), "index is not set: " + i);
      }
    }
  }

  private void createListTypeVectorWithScalarType(FieldWriter writer) {
    for (int i = 0; i < COUNT; i++) {
      writer.startList();
      for (int j = 0; j < i % 7; j++) {
        if (j % 2 == 0) {
          writer.writeInt(j);
        } else {
          IntHolder holder = new IntHolder();
          holder.value = j;
          writer.write(holder);
        }
      }
      writer.endList();
    }
  }

  private void checkListTypeVectorWithScalarType(FieldReader reader) {
    for (int i = 0; i < COUNT; i++) {
      reader.setPosition(i);
      for (int j = 0; j < i % 7; j++) {
        reader.next();
        assertEquals(j, reader.reader().readInteger().intValue());
      }
    }
  }

  private void createListTypeVectorWithScalarNull(FieldWriter writer) {
    for (int i = 0; i < COUNT; i++) {
      writer.startList();
      for (int j = 0; j < i % 7; j++) {
        if (j % 2 == 0) {
          writer.writeNull();
        } else {
          IntHolder holder = new IntHolder();
          holder.value = j;
          writer.write(holder);
        }
      }
      writer.endList();
    }
  }

  private void checkListTypeVectorWithScalarNull(FieldReader reader) {
    for (int i = 0; i < COUNT; i++) {
      reader.setPosition(i);
      for (int j = 0; j < i % 7; j++) {
        reader.next();
        if (j % 2 == 0) {
          assertFalse(reader.reader().isSet(), "index is set: " + j);
        } else {
          assertTrue(reader.reader().isSet(), "index is not set: " + j);
          assertEquals(j, reader.reader().readInteger().intValue());
        }
      }
    }
  }

  private void createListTypeVectorWithDecimalType(FieldWriter writer, DecimalHolder holder) {
    holder.buffer = allocator.buffer(DecimalVector.TYPE_WIDTH);
    ArrowType arrowType = new ArrowType.Decimal(10, 0, 128);
    for (int i = 0; i < COUNT; i++) {
      writer.startList();
      for (int j = 0; j < i % 7; j++) {
        if (j % 4 == 0) {
          writer.writeDecimal(new BigDecimal(j));
        } else if (j % 4 == 1) {
          DecimalUtility.writeBigDecimalToArrowBuf(
              new BigDecimal(j), holder.buffer, 0, DecimalVector.TYPE_WIDTH);
          holder.start = 0;
          holder.scale = 0;
          holder.precision = 10;
          writer.write(holder);
        } else if (j % 4 == 2) {
          DecimalUtility.writeBigDecimalToArrowBuf(
              new BigDecimal(j), holder.buffer, 0, DecimalVector.TYPE_WIDTH);
          writer.writeDecimal(0, holder.buffer, arrowType);
        } else {
          byte[] value = BigDecimal.valueOf(j).unscaledValue().toByteArray();
          writer.writeBigEndianBytesToDecimal(value, arrowType);
        }
      }
      writer.endList();
    }
  }

  private void checkListTypeVectorWithDecimalType(FieldReader reader) {
    for (int i = 0; i < COUNT; i++) {
      reader.setPosition(i);
      for (int j = 0; j < i % 7; j++) {
        reader.next();
        Object expected = new BigDecimal(j);
        Object actual = reader.reader().readBigDecimal();
        assertEquals(expected, actual);
      }
    }
  }

  private void createListTypeVectorWithTimeStampMilliTZType(FieldWriter writer) {
    for (int i = 0; i < COUNT; i++) {
      writer.startList();
      for (int j = 0; j < i % 7; j++) {
        if (j % 2 == 0) {
          writer.writeNull();
        } else {
          TimeStampMilliTZHolder holder = new TimeStampMilliTZHolder();
          holder.timezone = "FakeTimeZone";
          holder.value = j;
          writer.timeStampMilliTZ().write(holder);
        }
      }
      writer.endList();
    }
  }

  private void checkListTypeVectorWithTimeStampMilliTZType(FieldReader reader) {
    for (int i = 0; i < COUNT; i++) {
      reader.setPosition(i);
      for (int j = 0; j < i % 7; j++) {
        reader.next();
        if (j % 2 == 0) {
          assertFalse(reader.reader().isSet(), "index is set: " + j);
        } else {
          NullableTimeStampMilliTZHolder actual = new NullableTimeStampMilliTZHolder();
          reader.reader().read(actual);
          assertEquals(j, actual.value);
          assertEquals("FakeTimeZone", actual.timezone);
        }
      }
    }
  }

  private void createNullsWithListWriters(FieldWriter writer) {
    for (int i = 0; i < COUNT; i++) {
      writer.setPosition(i);
      if (i % 2 == 0) {
        writer.startList();
        if (i % 4 == 0) {
          writer.integer().writeNull();
        } else {
          writer.integer().writeInt(i);
          writer.integer().writeInt(i * 2);
        }
        writer.endList();
      } else {
        writer.writeNull();
      }
    }
  }

  private void checkNullsWithListWriters(FieldReader reader) {
    for (int i = 0; i < COUNT; i++) {
      reader.setPosition(i);
      if (i % 2 == 0) {
        assertTrue(reader.isSet());
        reader.next();
        if (i % 4 == 0) {
          assertNull(reader.reader().readInteger());
        } else {
          assertEquals(i, reader.reader().readInteger().intValue());
          reader.next();
          assertEquals(i * 2, reader.reader().readInteger().intValue());
        }
      } else {
        assertFalse(reader.isSet());
      }
    }
  }

  /* Test Cases */

  @Test
  public void simpleNestedTypes() {
    NonNullableStructVector parent = populateStructVector(null);
    StructReader rootReader = new SingleStructReaderImpl(parent).reader("root");
    for (int i = 0; i < COUNT; i++) {
      rootReader.setPosition(i);
      assertEquals(i, rootReader.reader("int").readInteger().intValue());
      assertEquals(i, rootReader.reader("bigInt").readLong().longValue());
    }

    parent.close();
  }

  @Test
  public void transferPairSchemaChange() {
    SchemaChangeCallBack callBack1 = new SchemaChangeCallBack();
    try (NonNullableStructVector parent = populateStructVector(callBack1)) {

      ComplexWriter writer = new ComplexWriterImpl("newWriter", parent);
      StructWriter rootWriter = writer.rootAsStruct();
      IntWriter intWriter = rootWriter.integer("newInt");
      intWriter.writeInt(1);
      writer.setValueCount(1);

      assertTrue(callBack1.getSchemaChangedAndReset());
      // The second vector should not have registered a schema change
      assertFalse(callBack1.getSchemaChangedAndReset());
    }
  }

  private NonNullableStructVector populateStructVector(CallBack callBack) {
    NonNullableStructVector parent =
        new NonNullableStructVector(
            "parent", allocator, new FieldType(false, Struct.INSTANCE, null, null), callBack);
    ComplexWriter writer = new ComplexWriterImpl("root", parent);
    StructWriter rootWriter = writer.rootAsStruct();
    IntWriter intWriter = rootWriter.integer("int");
    BigIntWriter bigIntWriter = rootWriter.bigInt("bigInt");
    for (int i = 0; i < COUNT; i++) {
      rootWriter.start();
      intWriter.writeInt(i);
      bigIntWriter.writeBigInt(i);
      rootWriter.end();
    }
    writer.setValueCount(COUNT);
    return parent;
  }

  @Test
  public void nullableStruct() {
    try (NonNullableStructVector structVector =
        NonNullableStructVector.empty("parent", allocator)) {
      ComplexWriter writer = new ComplexWriterImpl("root", structVector);
      StructWriter rootWriter = writer.rootAsStruct();
      for (int i = 0; i < COUNT; i++) {
        rootWriter.start();
        if (i % 2 == 0) {
          StructWriter structWriter = rootWriter.struct("struct");
          structWriter.setPosition(i);
          structWriter.start();
          structWriter.bigInt("nested").writeBigInt(i);
          structWriter.end();
        }
        rootWriter.end();
      }
      writer.setValueCount(COUNT);
      checkNullableStruct(structVector);
    }
  }

  /**
   * This test is similar to {@link #nullableStruct()} ()} but we get the inner struct writer once
   * at the beginning.
   */
  @Test
  public void nullableStruct2() {
    try (NonNullableStructVector structVector =
        NonNullableStructVector.empty("parent", allocator)) {
      ComplexWriter writer = new ComplexWriterImpl("root", structVector);
      StructWriter rootWriter = writer.rootAsStruct();
      StructWriter structWriter = rootWriter.struct("struct");

      for (int i = 0; i < COUNT; i++) {
        rootWriter.start();
        if (i % 2 == 0) {
          structWriter.setPosition(i);
          structWriter.start();
          structWriter.bigInt("nested").writeBigInt(i);
          structWriter.end();
        }
        rootWriter.end();
      }
      writer.setValueCount(COUNT);
      checkNullableStruct(structVector);
    }
  }

  @Test
  public void testList() {
    try (NonNullableStructVector parent = NonNullableStructVector.empty("parent", allocator)) {
      ComplexWriter writer = new ComplexWriterImpl("root", parent);
      StructWriter rootWriter = writer.rootAsStruct();

      rootWriter.start();
      rootWriter.bigInt("int").writeBigInt(0);
      rootWriter.list("list").startList();
      rootWriter.list("list").bigInt().writeBigInt(0);
      rootWriter.list("list").endList();
      rootWriter.end();

      rootWriter.start();
      rootWriter.bigInt("int").writeBigInt(1);
      rootWriter.end();

      writer.setValueCount(2);

      StructReader rootReader = new SingleStructReaderImpl(parent).reader("root");

      rootReader.setPosition(0);
      assertTrue(rootReader.reader("list").isSet(), "row 0 list is not set");
      assertEquals(Long.valueOf(0), rootReader.reader("list").reader().readLong());

      rootReader.setPosition(1);
      assertFalse(rootReader.reader("list").isSet(), "row 1 list is set");
    }
  }

  private void createListTypeVectorWithDurationType(FieldWriter writer) {
    for (int i = 0; i < COUNT; i++) {
      writer.startList();
      for (int j = 0; j < i % 7; j++) {
        if (j % 2 == 0) {
          writer.writeNull();
        } else {
          DurationHolder holder = new DurationHolder();
          holder.unit = TimeUnit.MICROSECOND;
          holder.value = j;
          writer.duration().write(holder);
        }
      }
      writer.endList();
    }
  }

  private void checkListTypeVectorWithDurationType(FieldReader reader) {
    for (int i = 0; i < COUNT; i++) {
      reader.setPosition(i);
      for (int j = 0; j < i % 7; j++) {
        reader.next();
        if (j % 2 == 0) {
          assertFalse(reader.reader().isSet(), "index is set: " + j);
        } else {
          NullableDurationHolder actual = new NullableDurationHolder();
          reader.reader().read(actual);
          assertEquals(TimeUnit.MICROSECOND, actual.unit);
          assertEquals(j, actual.value);
        }
      }
    }
  }

  private void createScalarTypeVectorWithNullableType(FieldWriter writer) {
    for (int i = 0; i < COUNT; i++) {
      if (i % 2 == 0) {
        writer.setPosition(i);
        writer.startList();
        for (int j = 0; j < i % 7; j++) {
          writer.writeInt(j);
        }
        writer.endList();
      }
    }
  }

  private void checkScalarTypeVectorWithNullableType(FieldReader reader) {
    for (int i = 0; i < COUNT; i++) {
      reader.setPosition(i);
      if (i % 2 == 0) {
        assertTrue(reader.isSet(), "index is set: " + i);
        assertEquals(i % 7, ((List<?>) reader.readObject()).size(), "correct length at: " + i);
      } else {
        assertFalse(reader.isSet(), "index is not set: " + i);
        assertNull(reader.readObject(), "index is not set: " + i);
      }
    }
  }

  private void createListTypeVectorWithStructType(
      FieldWriter fieldWriter, StructWriter structWriter) {
    for (int i = 0; i < COUNT; i++) {
      fieldWriter.startList();
      for (int j = 0; j < i % 7; j++) {
        structWriter.start();
        structWriter.integer("int").writeInt(j);
        structWriter.bigInt("bigInt").writeBigInt(j);
        structWriter.end();
      }
      fieldWriter.endList();
    }
  }

  private void checkListTypeVectorWithStructType(FieldReader reader) {
    for (int i = 0; i < COUNT; i++) {
      reader.setPosition(i);
      for (int j = 0; j < i % 7; j++) {
        reader.next();
        assertEquals(j, reader.reader().reader("int").readInteger().intValue(), "record: " + i);
        assertEquals(j, reader.reader().reader("bigInt").readLong().longValue());
      }
    }
  }

  private void checkListOfListTypes(final FieldReader reader) {
    for (int i = 0; i < COUNT; i++) {
      reader.setPosition(i);
      for (int j = 0; j < i % 7; j++) {
        reader.next();
        FieldReader innerListReader = reader.reader();
        for (int k = 0; k < i % 13; k++) {
          innerListReader.next();
          assertEquals(k, innerListReader.reader().readInteger().intValue(), "record: " + i);
        }
      }
    }
  }

  private void checkUnionListType(FieldReader reader) {
    for (int i = 0; i < COUNT; i++) {
      reader.setPosition(i);
      for (int j = 0; j < i % 7; j++) {
        reader.next();
        FieldReader innerListReader = reader.reader();
        for (int k = 0; k < i % 13; k++) {
          innerListReader.next();
          if (k % 2 == 0) {
            assertEquals(k, innerListReader.reader().readInteger().intValue(), "record: " + i);
          } else {
            assertEquals(k, innerListReader.reader().readLong().longValue(), "record: " + i);
          }
        }
      }
    }
  }

  private static void createListTypeVectorWithMapType(FieldWriter writer) {
    MapWriter innerMapWriter = writer.map(true);
    for (int i = 0; i < COUNT; i++) {
      writer.startList();
      for (int j = 0; j < i % 7; j++) {
        innerMapWriter.startMap();
        for (int k = 0; k < i % 13; k++) {
          innerMapWriter.startEntry();
          innerMapWriter.key().integer().writeInt(k);
          if (k % 2 == 0) {
            innerMapWriter.value().bigInt().writeBigInt(k);
          }
          innerMapWriter.endEntry();
        }
        innerMapWriter.endMap();
      }
      writer.endList();
    }
  }

  private void checkListTypeMap(FieldReader reader) {
    for (int i = 0; i < COUNT; i++) {
      reader.setPosition(i);
      for (int j = 0; j < i % 7; j++) {
        reader.next();
        UnionMapReader mapReader = (UnionMapReader) reader.reader();
        for (int k = 0; k < i % 13; k++) {
          mapReader.next();
          assertEquals(k, mapReader.key().readInteger().intValue(), "record key: " + i);
          if (k % 2 == 0) {
            assertEquals(k, mapReader.value().readLong().longValue(), "record value: " + i);
          } else {
            assertNull(mapReader.value().readLong(), "record value: " + i);
          }
        }
      }
    }
  }

  /* Test Cases */

  private void createListTypeVectorWithFixedSizeBinaryType(
      FieldWriter writer, List<ArrowBuf> buffers) {
    for (int i = 0; i < COUNT; i++) {
      writer.startList();
      for (int j = 0; j < i % 7; j++) {
        if (j % 2 == 0) {
          writer.writeNull();
        } else {
          ArrowBuf buf = allocator.buffer(4);
          buf.setInt(0, j);
          FixedSizeBinaryHolder holder = new FixedSizeBinaryHolder();
          holder.byteWidth = 4;
          holder.buffer = buf;
          writer.fixedSizeBinary().write(holder);
          buffers.add(buf);
        }
      }
      writer.endList();
    }
  }

  private void checkListTypeVectorWithFixedSizeBinaryType(FieldReader reader) {
    for (int i = 0; i < COUNT; i++) {
      reader.setPosition(i);
      for (int j = 0; j < i % 7; j++) {
        reader.next();
        if (j % 2 == 0) {
          assertFalse(reader.reader().isSet(), "index is set: " + j);
        } else {
          NullableFixedSizeBinaryHolder actual = new NullableFixedSizeBinaryHolder();
          reader.reader().read(actual);
          assertEquals(j, actual.buffer.getInt(0));
          assertEquals(4, actual.byteWidth);
        }
      }
    }
  }

  @Test
  public void listScalarType() {
    try (ListVector listVector = ListVector.empty("list", allocator)) {
      listVector.allocateNew();
      UnionListWriter listWriter = new UnionListWriter(listVector);
      createListTypeVectorWithScalarType(listWriter);
      listWriter.setValueCount(COUNT);
      UnionListReader listReader = new UnionListReader(listVector);
      // validate
      checkListTypeVectorWithScalarType(listReader);
    }
  }

  @Test
  public void listViewScalarType() {
    try (ListViewVector listViewVector = ListViewVector.empty("listview", allocator)) {
      listViewVector.allocateNew();
      UnionListViewWriter listViewWriter = new UnionListViewWriter(listViewVector);
      createListTypeVectorWithScalarType(listViewWriter);
      listViewWriter.setValueCount(COUNT);
      UnionListViewReader listViewReader = new UnionListViewReader(listViewVector);
      // validate
      checkListTypeVectorWithScalarType(listViewReader);
    }
  }

  @Test
  public void testListScalarNull() {
    /* Write to an integer list vector
     * each list of size 8
     * and having its data values alternating between null and a non-null.
     * Read and verify
     */
    try (ListVector listVector = ListVector.empty("list", allocator)) {
      listVector.allocateNew();
      UnionListWriter listWriter = new UnionListWriter(listVector);
      createListTypeVectorWithScalarNull(listWriter);
      listWriter.setValueCount(COUNT);
      UnionListReader listReader = new UnionListReader(listVector);
      checkListTypeVectorWithScalarNull(listReader);
    }
  }

  @Test
  public void testListViewScalarNull() {
    /* Write to an integer list vector
     * each list of size 8
     * and having its data values alternating between null and a non-null.
     * Read and verify
     */
    try (ListViewVector listViewVector = ListViewVector.empty("listview", allocator)) {
      listViewVector.allocateNew();
      UnionListViewWriter listViewWriter = new UnionListViewWriter(listViewVector);
      createListTypeVectorWithScalarNull(listViewWriter);
      listViewWriter.setValueCount(COUNT);
      UnionListViewReader listViewReader = new UnionListViewReader(listViewVector);
      checkListTypeVectorWithScalarNull(listViewReader);
    }
  }

  @Test
  public void listDecimalType() {
    try (ListVector listVector = ListVector.empty("list", allocator)) {
      listVector.allocateNew();
      UnionListWriter listWriter = new UnionListWriter(listVector);
      DecimalHolder holder = new DecimalHolder();
      createListTypeVectorWithDecimalType(listWriter, holder);
      listWriter.setValueCount(COUNT);
      UnionListReader listReader = new UnionListReader(listVector);
      checkListTypeVectorWithDecimalType(listReader);
      holder.buffer.close();
    }
  }

  @Test
  public void listViewDecimalType() {
    try (ListViewVector listViewVector = ListViewVector.empty("listview", allocator)) {
      listViewVector.allocateNew();
      UnionListViewWriter listViewWriter = new UnionListViewWriter(listViewVector);
      DecimalHolder holder = new DecimalHolder();
      createListTypeVectorWithDecimalType(listViewWriter, holder);
      listViewWriter.setValueCount(COUNT);
      UnionListViewReader listViewReader = new UnionListViewReader(listViewVector);
      checkListTypeVectorWithDecimalType(listViewReader);
      holder.buffer.close();
    }
  }

  @Test
  public void listTimeStampMilliTZType() {
    try (ListVector listVector = ListVector.empty("list", allocator)) {
      listVector.allocateNew();
      UnionListWriter listWriter = new UnionListWriter(listVector);
      createListTypeVectorWithTimeStampMilliTZType(listWriter);
      listWriter.setValueCount(COUNT);
      UnionListReader listReader = new UnionListReader(listVector);
      checkListTypeVectorWithTimeStampMilliTZType(listReader);
    }
  }

  @Test
  public void listViewTimeStampMilliTZType() {
    try (ListViewVector listViewVector = ListViewVector.empty("listview", allocator)) {
      listViewVector.allocateNew();
      UnionListViewWriter listViewWriter = new UnionListViewWriter(listViewVector);
      createListTypeVectorWithTimeStampMilliTZType(listViewWriter);
      listViewWriter.setValueCount(COUNT);
      UnionListViewReader listViewReader = new UnionListViewReader(listViewVector);
      checkListTypeVectorWithTimeStampMilliTZType(listViewReader);
    }
  }

  @Test
  public void listDurationType() {
    try (ListVector listVector = ListVector.empty("list", allocator)) {
      listVector.allocateNew();
      UnionListWriter listWriter = new UnionListWriter(listVector);
      createListTypeVectorWithDurationType(listWriter);
      listWriter.setValueCount(COUNT);
      UnionListReader listReader = new UnionListReader(listVector);
      checkListTypeVectorWithDurationType(listReader);
    }
  }

  @Test
  public void listViewDurationType() {
    try (ListViewVector listViewVector = ListViewVector.empty("listview", allocator)) {
      listViewVector.allocateNew();
      UnionListViewWriter listViewWriter = new UnionListViewWriter(listViewVector);
      createListTypeVectorWithDurationType(listViewWriter);
      listViewWriter.setValueCount(COUNT);
      UnionListViewReader listReader = new UnionListViewReader(listViewVector);
      checkListTypeVectorWithDurationType(listReader);
    }
  }

  @Test
  public void listFixedSizeBinaryType() throws Exception {
    List<ArrowBuf> buffers = new ArrayList<>();
    try (ListVector listVector = ListVector.empty("list", allocator)) {
      listVector.allocateNew();
      UnionListWriter listWriter = new UnionListWriter(listVector);
      createListTypeVectorWithFixedSizeBinaryType(listWriter, buffers);
      listWriter.setValueCount(COUNT);
      UnionListReader listReader = new UnionListReader(listVector);
      checkListTypeVectorWithFixedSizeBinaryType(listReader);
    }
    AutoCloseables.close(buffers);
  }

  @Test
  public void listViewFixedSizeBinaryType() throws Exception {
    List<ArrowBuf> buffers = new ArrayList<>();
    try (ListViewVector listViewVector = ListViewVector.empty("listview", allocator)) {
      listViewVector.allocateNew();
      UnionListViewWriter listViewWriter = new UnionListViewWriter(listViewVector);
      createListTypeVectorWithFixedSizeBinaryType(listViewWriter, buffers);
      listViewWriter.setValueCount(COUNT);
      UnionListViewReader listReader = new UnionListViewReader(listViewVector);
      checkListTypeVectorWithFixedSizeBinaryType(listReader);
    }
    AutoCloseables.close(buffers);
  }

  @Test
  public void listScalarTypeNullable() {
    try (ListVector listVector = ListVector.empty("list", allocator)) {
      listVector.allocateNew();
      UnionListWriter listWriter = new UnionListWriter(listVector);
      createScalarTypeVectorWithNullableType(listWriter);
      listWriter.setValueCount(COUNT);
      UnionListReader listReader = new UnionListReader(listVector);
      checkScalarTypeVectorWithNullableType(listReader);
    }
  }

  @Test
  public void listViewScalarTypeNullable() {
    try (ListViewVector listViewVector = ListViewVector.empty("listview", allocator)) {
      listViewVector.allocateNew();
      UnionListViewWriter listViewWriter = new UnionListViewWriter(listViewVector);
      createScalarTypeVectorWithNullableType(listViewWriter);
      listViewWriter.setValueCount(COUNT);
      UnionListViewReader listReader = new UnionListViewReader(listViewVector);
      checkScalarTypeVectorWithNullableType(listReader);
    }
  }

  @Test
  public void listStructType() {
    try (ListVector listVector = ListVector.empty("list", allocator)) {
      listVector.allocateNew();
      UnionListWriter listViewWriter = new UnionListWriter(listVector);
      StructWriter structWriter = listViewWriter.struct();
      createListTypeVectorWithStructType(listViewWriter, structWriter);
      listViewWriter.setValueCount(COUNT);
      UnionListReader listReader = new UnionListReader(listVector);
      checkListTypeVectorWithStructType(listReader);
    }
  }

  @Test
  public void listViewStructType() {
    try (ListViewVector listViewVector = ListViewVector.empty("listview", allocator)) {
      listViewVector.allocateNew();
      UnionListViewWriter listViewWriter = new UnionListViewWriter(listViewVector);
      StructWriter structWriter = listViewWriter.struct();
      createListTypeVectorWithStructType(listViewWriter, structWriter);
      listViewWriter.setValueCount(COUNT);
      UnionListViewReader listReader = new UnionListViewReader(listViewVector);
      checkListTypeVectorWithStructType(listReader);
    }
  }

  @Test
  public void listListType() {
    try (ListVector listVector = ListVector.empty("list", allocator)) {
      listVector.allocateNew();
      UnionListWriter listWriter = new UnionListWriter(listVector);
      for (int i = 0; i < COUNT; i++) {
        listWriter.startList();
        for (int j = 0; j < i % 7; j++) {
          ListWriter innerListWriter = listWriter.list();
          innerListWriter.startList();
          for (int k = 0; k < i % 13; k++) {
            innerListWriter.integer().writeInt(k);
          }
          innerListWriter.endList();
        }
        listWriter.endList();
      }
      listWriter.setValueCount(COUNT);
      UnionListReader listReader = new UnionListReader(listVector);
      checkListOfListTypes(listReader);
    }
  }

  @Test
  public void listViewListType() {
    try (ListViewVector listViewVector = ListViewVector.empty("listview", allocator)) {
      listViewVector.allocateNew();
      UnionListViewWriter listViewWriter = new UnionListViewWriter(listViewVector);
      for (int i = 0; i < COUNT; i++) {
        listViewWriter.startListView();
        for (int j = 0; j < i % 7; j++) {
          ListWriter innerListWriter = listViewWriter.listView();
          innerListWriter.startListView();
          for (int k = 0; k < i % 13; k++) {
            innerListWriter.integer().writeInt(k);
          }
          innerListWriter.endListView();
        }
        listViewWriter.endListView();
      }
      listViewWriter.setValueCount(COUNT);
      UnionListViewReader listReader = new UnionListViewReader(listViewVector);
      checkListOfListTypes(listReader);
    }
  }

  /**
   * This test is similar to {@link #listListType()} but we get the inner list writer once at the
   * beginning.
   */
  @Test
  public void listListType2() {
    try (ListVector listVector = ListVector.empty("list", allocator)) {
      listVector.allocateNew();
      UnionListWriter listWriter = new UnionListWriter(listVector);
      ListWriter innerListWriter = listWriter.list();
      for (int i = 0; i < COUNT; i++) {
        listWriter.startList();
        for (int j = 0; j < i % 7; j++) {
          innerListWriter.startList();
          for (int k = 0; k < i % 13; k++) {
            innerListWriter.integer().writeInt(k);
          }
          innerListWriter.endList();
        }
        listWriter.endList();
      }
      listWriter.setValueCount(COUNT);
      UnionListReader listReader = new UnionListReader(listVector);
      checkListOfListTypes(listReader);
    }
  }

  @Test
  public void listViewListType2() {
    try (ListViewVector listViewVector = ListViewVector.empty("listview", allocator)) {
      listViewVector.allocateNew();
      UnionListViewWriter listViewWriter = new UnionListViewWriter(listViewVector);
      ListWriter innerListWriter = listViewWriter.list();
      for (int i = 0; i < COUNT; i++) {
        listViewWriter.startListView();
        for (int j = 0; j < i % 7; j++) {
          innerListWriter.startListView();
          for (int k = 0; k < i % 13; k++) {
            innerListWriter.integer().writeInt(k);
          }
          innerListWriter.endListView();
        }
        listViewWriter.endListView();
      }
      listViewWriter.setValueCount(COUNT);
      UnionListViewReader listReader = new UnionListViewReader(listViewVector);
      checkListOfListTypes(listReader);
    }
  }

  @Test
  public void unionListListType() {
    try (ListVector listVector = ListVector.empty("list", allocator)) {
      listVector.allocateNew();
      UnionListWriter listWriter = new UnionListWriter(listVector);
      for (int i = 0; i < COUNT; i++) {
        listWriter.startList();
        for (int j = 0; j < i % 7; j++) {
          ListWriter innerListWriter = listWriter.list();
          innerListWriter.startList();
          for (int k = 0; k < i % 13; k++) {
            if (k % 2 == 0) {
              innerListWriter.integer().writeInt(k);
            } else {
              innerListWriter.bigInt().writeBigInt(k);
            }
          }
          innerListWriter.endList();
        }
        listWriter.endList();
      }
      listWriter.setValueCount(COUNT);
      UnionListReader listReader = new UnionListReader(listVector);
      checkUnionListType(listReader);
    }
  }

  @Test
  public void unionListViewListType() {
    try (ListViewVector listViewVector = ListViewVector.empty("listview", allocator)) {
      listViewVector.allocateNew();
      UnionListViewWriter listViewWriter = new UnionListViewWriter(listViewVector);
      for (int i = 0; i < COUNT; i++) {
        listViewWriter.startList();
        for (int j = 0; j < i % 7; j++) {
          ListWriter innerListWriter = listViewWriter.listView();
          innerListWriter.startListView();
          for (int k = 0; k < i % 13; k++) {
            if (k % 2 == 0) {
              innerListWriter.integer().writeInt(k);
            } else {
              innerListWriter.bigInt().writeBigInt(k);
            }
          }
          innerListWriter.endListView();
        }
        listViewWriter.endListView();
      }
      listViewWriter.setValueCount(COUNT);
      UnionListViewReader listViewReader = new UnionListViewReader(listViewVector);
      checkUnionListType(listViewReader);
    }
  }

  /**
   * This test is similar to {@link #unionListListType()} but we get the inner list writer once at
   * the beginning.
   */
  @Test
  public void unionListListType2() {
    try (ListVector listVector = ListVector.empty("list", allocator)) {
      listVector.allocateNew();
      UnionListWriter listWriter = new UnionListWriter(listVector);
      ListWriter innerListWriter = listWriter.listView();
      for (int i = 0; i < COUNT; i++) {
        listWriter.startList();
        for (int j = 0; j < i % 7; j++) {
          innerListWriter.startList();
          for (int k = 0; k < i % 13; k++) {
            if (k % 2 == 0) {
              innerListWriter.integer().writeInt(k);
            } else {
              innerListWriter.bigInt().writeBigInt(k);
            }
          }
          innerListWriter.endList();
        }
        listWriter.endList();
      }
      listWriter.setValueCount(COUNT);
      UnionListReader listReader = new UnionListReader(listVector);
      checkUnionListType(listReader);
    }
  }

  /**
   * This test is similar to {@link #unionListViewListType()} but we get the inner list writer once
   * at the beginning.
   */
  @Test
  public void unionListViewListType2() {
    try (ListViewVector listViewVector = ListViewVector.empty("listview", allocator)) {
      listViewVector.allocateNew();
      UnionListViewWriter listViewWriter = new UnionListViewWriter(listViewVector);
      ListWriter innerListWriter = listViewWriter.listView();
      for (int i = 0; i < COUNT; i++) {
        listViewWriter.startListView();
        for (int j = 0; j < i % 7; j++) {
          innerListWriter.startListView();
          for (int k = 0; k < i % 13; k++) {
            if (k % 2 == 0) {
              innerListWriter.integer().writeInt(k);
            } else {
              innerListWriter.bigInt().writeBigInt(k);
            }
          }
          innerListWriter.endListView();
        }
        listViewWriter.endListView();
      }
      listViewWriter.setValueCount(COUNT);
      UnionListViewReader listViewReader = new UnionListViewReader(listViewVector);
      checkUnionListType(listViewReader);
    }
  }

  @Test
  public void testListMapType() {
    try (ListVector listVector = ListVector.empty("list", allocator)) {
      listVector.allocateNew();
      UnionListWriter listWriter = new UnionListWriter(listVector);

      createListTypeVectorWithMapType(listWriter);
      listWriter.setValueCount(COUNT);
      UnionListReader listReader = new UnionListReader(listVector);
      checkListTypeMap(listReader);
      // Verify that the map vector has keysSorted = true
      MapVector mapVector = (MapVector) listVector.getDataVector();
      ArrowType arrowType = mapVector.getField().getFieldType().getType();
      assertTrue(((ArrowType.Map) arrowType).getKeysSorted());
    }
  }

  @Test
  public void testListViewMapType() {
    try (ListViewVector listViewVector = ListViewVector.empty("listview", allocator)) {
      listViewVector.allocateNew();
      UnionListViewWriter listViewWriter = new UnionListViewWriter(listViewVector);

      createListTypeVectorWithMapType(listViewWriter);
      listViewWriter.setValueCount(COUNT);
      UnionListViewReader listViewReader = new UnionListViewReader(listViewVector);
      checkListTypeMap(listViewReader);
      // Verify that the map vector has keysSorted = true
      MapVector mapVector = (MapVector) listViewVector.getDataVector();
      ArrowType arrowType = mapVector.getField().getFieldType().getType();
      assertTrue(((ArrowType.Map) arrowType).getKeysSorted());
    }
  }

  @Test
  public void simpleUnion() throws Exception {
    List<ArrowBuf> bufs = new ArrayList<ArrowBuf>();
    UnionVector vector =
        new UnionVector("union", allocator, /* field type */ null, /* call-back */ null);
    UnionWriter unionWriter = new UnionWriter(vector);
    unionWriter.allocate();
    for (int i = 0; i < COUNT; i++) {
      unionWriter.setPosition(i);
      if (i % 5 == 0) {
        unionWriter.writeInt(i);
      } else if (i % 5 == 1) {
        TimeStampMilliTZHolder holder = new TimeStampMilliTZHolder();
        holder.value = (long) i;
        holder.timezone = "AsdfTimeZone";
        unionWriter.write(holder);
      } else if (i % 5 == 2) {
        DurationHolder holder = new DurationHolder();
        holder.value = (long) i;
        holder.unit = TimeUnit.NANOSECOND;
        unionWriter.write(holder);
      } else if (i % 5 == 3) {
        FixedSizeBinaryHolder holder = new FixedSizeBinaryHolder();
        ArrowBuf buf = allocator.buffer(4);
        buf.setInt(0, i);
        holder.byteWidth = 4;
        holder.buffer = buf;
        unionWriter.write(holder);
        bufs.add(buf);
      } else {
        unionWriter.writeFloat4((float) i);
      }
    }
    vector.setValueCount(COUNT);
    UnionReader unionReader = new UnionReader(vector);
    for (int i = 0; i < COUNT; i++) {
      unionReader.setPosition(i);
      if (i % 5 == 0) {
        assertEquals(i, unionReader.readInteger().intValue());
      } else if (i % 5 == 1) {
        NullableTimeStampMilliTZHolder holder = new NullableTimeStampMilliTZHolder();
        unionReader.read(holder);
        assertEquals(i, holder.value);
        assertEquals("AsdfTimeZone", holder.timezone);
      } else if (i % 5 == 2) {
        NullableDurationHolder holder = new NullableDurationHolder();
        unionReader.read(holder);
        assertEquals(i, holder.value);
        assertEquals(TimeUnit.NANOSECOND, holder.unit);
      } else if (i % 5 == 3) {
        NullableFixedSizeBinaryHolder holder = new NullableFixedSizeBinaryHolder();
        unionReader.read(holder);
        assertEquals(i, holder.buffer.getInt(0));
        assertEquals(4, holder.byteWidth);
      } else {
        assertEquals((float) i, unionReader.readFloat(), 1e-12);
      }
    }
    vector.close();
    AutoCloseables.close(bufs);
  }

  @Test
  public void promotableWriter() {
    try (NonNullableStructVector parent = NonNullableStructVector.empty("parent", allocator)) {

      ComplexWriter writer = new ComplexWriterImpl("root", parent);
      StructWriter rootWriter = writer.rootAsStruct();
      for (int i = 0; i < 100; i++) {
        BigIntWriter bigIntWriter = rootWriter.bigInt("a");
        bigIntWriter.setPosition(i);
        bigIntWriter.writeBigInt(i);
      }
      Field field = parent.getField().getChildren().get(0).getChildren().get(0);
      assertEquals("a", field.getName());
      assertEquals(Int.TYPE_TYPE, field.getType().getTypeID());
      Int intType = (Int) field.getType();

      assertEquals(64, intType.getBitWidth());
      assertTrue(intType.getIsSigned());
      for (int i = 100; i < 200; i++) {
        VarCharWriter varCharWriter = rootWriter.varChar("a");
        varCharWriter.setPosition(i);
        byte[] bytes = Integer.toString(i).getBytes(StandardCharsets.UTF_8);
        ArrowBuf tempBuf = allocator.buffer(bytes.length);
        tempBuf.setBytes(0, bytes);
        varCharWriter.writeVarChar(0, bytes.length, tempBuf);
        tempBuf.close();
      }
      field = parent.getField().getChildren().get(0).getChildren().get(0);
      assertEquals("a", field.getName());
      assertEquals(Union.TYPE_TYPE, field.getType().getTypeID());
      assertEquals(Int.TYPE_TYPE, field.getChildren().get(0).getType().getTypeID());
      assertEquals(Utf8.TYPE_TYPE, field.getChildren().get(1).getType().getTypeID());
      StructReader rootReader = new SingleStructReaderImpl(parent).reader("root");
      for (int i = 0; i < 100; i++) {
        rootReader.setPosition(i);
        FieldReader reader = rootReader.reader("a");
        Long value = reader.readLong();
        assertNotNull(value, "index: " + i);
        assertEquals(i, value.intValue());
      }
      for (int i = 100; i < 200; i++) {
        rootReader.setPosition(i);
        FieldReader reader = rootReader.reader("a");
        Text value = reader.readText();
        assertEquals(Integer.toString(i), value.toString());
      }
    }
  }

  /** Even without writing to the writer, the union schema is created correctly. */
  @Test
  public void promotableWriterSchema() {
    try (NonNullableStructVector parent = NonNullableStructVector.empty("parent", allocator)) {
      ComplexWriter writer = new ComplexWriterImpl("root", parent);
      StructWriter rootWriter = writer.rootAsStruct();
      rootWriter.bigInt("a");
      rootWriter.varChar("a");

      Field field = parent.getField().getChildren().get(0).getChildren().get(0);
      assertEquals("a", field.getName());
      assertEquals(ArrowTypeID.Union, field.getType().getTypeID());

      assertEquals(ArrowTypeID.Int, field.getChildren().get(0).getType().getTypeID());
      Int intType = (Int) field.getChildren().get(0).getType();
      assertEquals(64, intType.getBitWidth());
      assertTrue(intType.getIsSigned());
      assertEquals(ArrowTypeID.Utf8, field.getChildren().get(1).getType().getTypeID());
    }
  }

  private Set<String> getFieldNames(List<Field> fields) {
    Set<String> fieldNames = new HashSet<>();
    for (Field field : fields) {
      fieldNames.add(field.getName());
      if (!field.getChildren().isEmpty()) {
        for (String name : getFieldNames(field.getChildren())) {
          fieldNames.add(field.getName() + "::" + name);
        }
      }
    }
    return fieldNames;
  }

  @Test
  public void structWriterMixedCaseFieldNames() {
    try (NonNullableStructVector parent = NonNullableStructVector.empty("parent", allocator)) {
      // test case-sensitive StructWriter
      ComplexWriter writer = new ComplexWriterImpl("rootCaseSensitive", parent, false, true);
      StructWriter rootWriterCaseSensitive = writer.rootAsStruct();
      rootWriterCaseSensitive.bigInt("int_field");
      rootWriterCaseSensitive.bigInt("Int_Field");
      rootWriterCaseSensitive.float4("float_field");
      rootWriterCaseSensitive.float4("Float_Field");
      StructWriter structFieldWriterCaseSensitive = rootWriterCaseSensitive.struct("struct_field");
      structFieldWriterCaseSensitive.varChar("char_field");
      structFieldWriterCaseSensitive.varChar("Char_Field");
      ListWriter listFieldWriterCaseSensitive = rootWriterCaseSensitive.list("list_field");
      StructWriter listStructFieldWriterCaseSensitive = listFieldWriterCaseSensitive.struct();
      listStructFieldWriterCaseSensitive.bit("bit_field");
      listStructFieldWriterCaseSensitive.bit("Bit_Field");

      List<Field> fieldsCaseSensitive = parent.getField().getChildren().get(0).getChildren();
      Set<String> fieldNamesCaseSensitive = getFieldNames(fieldsCaseSensitive);
      assertEquals(11, fieldNamesCaseSensitive.size());
      assertTrue(fieldNamesCaseSensitive.contains("int_field"));
      assertTrue(fieldNamesCaseSensitive.contains("Int_Field"));
      assertTrue(fieldNamesCaseSensitive.contains("float_field"));
      assertTrue(fieldNamesCaseSensitive.contains("Float_Field"));
      assertTrue(fieldNamesCaseSensitive.contains("struct_field"));
      assertTrue(fieldNamesCaseSensitive.contains("struct_field::char_field"));
      assertTrue(fieldNamesCaseSensitive.contains("struct_field::Char_Field"));
      assertTrue(fieldNamesCaseSensitive.contains("list_field"));
      assertTrue(fieldNamesCaseSensitive.contains("list_field::$data$"));
      assertTrue(fieldNamesCaseSensitive.contains("list_field::$data$::bit_field"));
      assertTrue(fieldNamesCaseSensitive.contains("list_field::$data$::Bit_Field"));

      // test case-insensitive StructWriter
      ComplexWriter writerCaseInsensitive =
          new ComplexWriterImpl("rootCaseInsensitive", parent, false, false);
      StructWriter rootWriterCaseInsensitive = writerCaseInsensitive.rootAsStruct();

      rootWriterCaseInsensitive.bigInt("int_field");
      rootWriterCaseInsensitive.bigInt("Int_Field");
      rootWriterCaseInsensitive.float4("float_field");
      rootWriterCaseInsensitive.float4("Float_Field");
      StructWriter structFieldWriterCaseInsensitive =
          rootWriterCaseInsensitive.struct("struct_field");
      structFieldWriterCaseInsensitive.varChar("char_field");
      structFieldWriterCaseInsensitive.varChar("Char_Field");
      ListWriter listFieldWriterCaseInsensitive = rootWriterCaseInsensitive.list("list_field");
      StructWriter listStructFieldWriterCaseInsensitive = listFieldWriterCaseInsensitive.struct();
      listStructFieldWriterCaseInsensitive.bit("bit_field");
      listStructFieldWriterCaseInsensitive.bit("Bit_Field");

      List<Field> fieldsCaseInsensitive = parent.getField().getChildren().get(1).getChildren();
      Set<String> fieldNamesCaseInsensitive = getFieldNames(fieldsCaseInsensitive);
      assertEquals(7, fieldNamesCaseInsensitive.size());
      assertTrue(fieldNamesCaseInsensitive.contains("int_field"));
      assertTrue(fieldNamesCaseInsensitive.contains("float_field"));
      assertTrue(fieldNamesCaseInsensitive.contains("struct_field"));
      assertTrue(fieldNamesCaseInsensitive.contains("struct_field::char_field"));
      assertTrue(fieldNamesCaseSensitive.contains("list_field"));
      assertTrue(fieldNamesCaseSensitive.contains("list_field::$data$"));
      assertTrue(fieldNamesCaseSensitive.contains("list_field::$data$::bit_field"));
    }
  }

  @Test
  public void timeStampSecWriter() throws Exception {
    // test values
    final long expectedSecs = 981173106L;
    final LocalDateTime expectedSecDateTime = LocalDateTime.of(2001, 2, 3, 4, 5, 6, 0);

    try (NonNullableStructVector parent = NonNullableStructVector.empty("parent", allocator)) {
      // write

      ComplexWriter writer = new ComplexWriterImpl("root", parent);
      StructWriter rootWriter = writer.rootAsStruct();

      {
        TimeStampSecWriter timeStampSecWriter = rootWriter.timeStampSec("sec");
        timeStampSecWriter.setPosition(0);
        timeStampSecWriter.writeTimeStampSec(expectedSecs);
      }
      {
        TimeStampSecTZWriter timeStampSecTZWriter = rootWriter.timeStampSecTZ("secTZ", "UTC");
        timeStampSecTZWriter.setPosition(1);
        timeStampSecTZWriter.writeTimeStampSecTZ(expectedSecs);
      }
      // schema
      List<Field> children = parent.getField().getChildren().get(0).getChildren();
      checkTimestampField(children.get(0), "sec");
      checkTimestampTZField(children.get(1), "secTZ", "UTC");

      // read
      StructReader rootReader = new SingleStructReaderImpl(parent).reader("root");
      {
        FieldReader secReader = rootReader.reader("sec");
        secReader.setPosition(0);
        LocalDateTime secDateTime = secReader.readLocalDateTime();
        assertEquals(expectedSecDateTime, secDateTime);
        long secLong = secReader.readLong();
        assertEquals(expectedSecs, secLong);
      }
      {
        FieldReader secTZReader = rootReader.reader("secTZ");
        secTZReader.setPosition(1);
        long secTZLong = secTZReader.readLong();
        assertEquals(expectedSecs, secTZLong);
      }
    }
  }

  @Test
  public void timeStampMilliWriters() throws Exception {
    // test values
    final long expectedMillis = 981173106123L;
    final LocalDateTime expectedMilliDateTime =
        LocalDateTime.of(2001, 2, 3, 4, 5, 6, 123 * 1_000_000);

    try (NonNullableStructVector parent = NonNullableStructVector.empty("parent", allocator); ) {
      // write
      ComplexWriter writer = new ComplexWriterImpl("root", parent);
      StructWriter rootWriter = writer.rootAsStruct();
      {
        TimeStampMilliWriter timeStampWriter = rootWriter.timeStampMilli("milli");
        timeStampWriter.setPosition(0);
        timeStampWriter.writeTimeStampMilli(expectedMillis);
      }
      String tz = "UTC";
      {
        TimeStampMilliTZWriter timeStampTZWriter = rootWriter.timeStampMilliTZ("milliTZ", tz);
        timeStampTZWriter.setPosition(0);
        timeStampTZWriter.writeTimeStampMilliTZ(expectedMillis);
      }
      // schema
      List<Field> children = parent.getField().getChildren().get(0).getChildren();
      checkTimestampField(children.get(0), "milli");
      checkTimestampTZField(children.get(1), "milliTZ", tz);

      // read
      StructReader rootReader = new SingleStructReaderImpl(parent).reader("root");

      {
        FieldReader milliReader = rootReader.reader("milli");
        milliReader.setPosition(0);
        LocalDateTime milliDateTime = milliReader.readLocalDateTime();
        assertEquals(expectedMilliDateTime, milliDateTime);
        long milliLong = milliReader.readLong();
        assertEquals(expectedMillis, milliLong);
      }
      {
        FieldReader milliTZReader = rootReader.reader("milliTZ");
        milliTZReader.setPosition(0);
        long milliTZLong = milliTZReader.readLong();
        assertEquals(expectedMillis, milliTZLong);
      }
    }
  }

  private void checkTimestampField(Field field, String name) {
    assertEquals(name, field.getName());
    assertEquals(ArrowType.Timestamp.TYPE_TYPE, field.getType().getTypeID());
  }

  private void checkTimestampTZField(Field field, String name, String tz) {
    checkTimestampField(field, name);
    assertEquals(tz, ((Timestamp) field.getType()).getTimezone());
  }

  @Test
  public void timeStampMicroWriters() throws Exception {
    // test values
    final long expectedMicros = 981173106123456L;
    final LocalDateTime expectedMicroDateTime =
        LocalDateTime.of(2001, 2, 3, 4, 5, 6, 123456 * 1000);

    try (NonNullableStructVector parent = NonNullableStructVector.empty("parent", allocator)) {
      // write
      ComplexWriter writer = new ComplexWriterImpl("root", parent);
      StructWriter rootWriter = writer.rootAsStruct();

      {
        TimeStampMicroWriter timeStampMicroWriter = rootWriter.timeStampMicro("micro");
        timeStampMicroWriter.setPosition(0);
        timeStampMicroWriter.writeTimeStampMicro(expectedMicros);
      }
      String tz = "UTC";
      {
        TimeStampMicroTZWriter timeStampMicroWriter = rootWriter.timeStampMicroTZ("microTZ", tz);
        timeStampMicroWriter.setPosition(1);
        timeStampMicroWriter.writeTimeStampMicroTZ(expectedMicros);
      }

      // schema
      List<Field> children = parent.getField().getChildren().get(0).getChildren();
      checkTimestampField(children.get(0), "micro");
      checkTimestampTZField(children.get(1), "microTZ", tz);

      // read
      StructReader rootReader = new SingleStructReaderImpl(parent).reader("root");
      {
        FieldReader microReader = rootReader.reader("micro");
        microReader.setPosition(0);
        LocalDateTime microDateTime = microReader.readLocalDateTime();
        assertEquals(expectedMicroDateTime, microDateTime);
        long microLong = microReader.readLong();
        assertEquals(expectedMicros, microLong);
      }
      {
        FieldReader microReader = rootReader.reader("microTZ");
        microReader.setPosition(1);
        long microLong = microReader.readLong();
        assertEquals(expectedMicros, microLong);
      }
    }
  }

  @Test
  public void timeStampNanoWriters() throws Exception {
    // test values
    final long expectedNanos = 981173106123456789L;
    final LocalDateTime expectedNanoDateTime = LocalDateTime.of(2001, 2, 3, 4, 5, 6, 123456789);

    try (NonNullableStructVector parent = NonNullableStructVector.empty("parent", allocator)) {
      // write
      ComplexWriter writer = new ComplexWriterImpl("root", parent);
      StructWriter rootWriter = writer.rootAsStruct();

      {
        TimeStampNanoWriter timeStampNanoWriter = rootWriter.timeStampNano("nano");
        timeStampNanoWriter.setPosition(0);
        timeStampNanoWriter.writeTimeStampNano(expectedNanos);
      }
      String tz = "UTC";
      {
        TimeStampNanoTZWriter timeStampNanoWriter = rootWriter.timeStampNanoTZ("nanoTZ", tz);
        timeStampNanoWriter.setPosition(0);
        timeStampNanoWriter.writeTimeStampNanoTZ(expectedNanos);
      }
      // schema
      List<Field> children = parent.getField().getChildren().get(0).getChildren();
      checkTimestampField(children.get(0), "nano");
      checkTimestampTZField(children.get(1), "nanoTZ", tz);
      // read
      StructReader rootReader = new SingleStructReaderImpl(parent).reader("root");

      {
        FieldReader nanoReader = rootReader.reader("nano");
        nanoReader.setPosition(0);
        LocalDateTime nanoDateTime = nanoReader.readLocalDateTime();
        assertEquals(expectedNanoDateTime, nanoDateTime);
        long nanoLong = nanoReader.readLong();
        assertEquals(expectedNanos, nanoLong);
      }
      {
        FieldReader nanoReader = rootReader.reader("nanoTZ");
        nanoReader.setPosition(0);
        long nanoLong = nanoReader.readLong();
        assertEquals(expectedNanos, nanoLong);
        NullableTimeStampNanoTZHolder h = new NullableTimeStampNanoTZHolder();
        nanoReader.read(h);
        assertEquals(expectedNanos, h.value);
      }
    }
  }

  @Test
  public void fixedSizeBinaryWriters() throws Exception {
    // test values
    int numValues = 10;
    int byteWidth = 9;
    byte[][] values = new byte[numValues][byteWidth];
    for (int i = 0; i < numValues; i++) {
      for (int j = 0; j < byteWidth; j++) {
        values[i][j] = ((byte) i);
      }
    }
    ArrowBuf[] bufs = new ArrowBuf[numValues];
    for (int i = 0; i < numValues; i++) {
      bufs[i] = allocator.buffer(byteWidth);
      bufs[i].setBytes(0, values[i]);
    }

    try (NonNullableStructVector parent = NonNullableStructVector.empty("parent", allocator)) {
      // write
      ComplexWriter writer = new ComplexWriterImpl("root", parent);
      StructWriter rootWriter = writer.rootAsStruct();

      String fieldName = "fixedSizeBinary";
      FixedSizeBinaryWriter fixedSizeBinaryWriter =
          rootWriter.fixedSizeBinary(fieldName, byteWidth);
      for (int i = 0; i < numValues; i++) {
        fixedSizeBinaryWriter.setPosition(i);
        fixedSizeBinaryWriter.writeFixedSizeBinary(bufs[i]);
      }

      // schema
      List<Field> children = parent.getField().getChildren().get(0).getChildren();
      assertEquals(fieldName, children.get(0).getName());
      assertEquals(ArrowType.FixedSizeBinary.TYPE_TYPE, children.get(0).getType().getTypeID());

      // read
      StructReader rootReader = new SingleStructReaderImpl(parent).reader("root");

      FieldReader fixedSizeBinaryReader = rootReader.reader(fieldName);
      for (int i = 0; i < numValues; i++) {
        fixedSizeBinaryReader.setPosition(i);
        byte[] readValues = fixedSizeBinaryReader.readByteArray();
        assertArrayEquals(values[i], readValues);
      }
    }

    AutoCloseables.close(bufs);
  }

  @Test
  public void complexCopierWithList() {
    try (NonNullableStructVector parent = NonNullableStructVector.empty("parent", allocator)) {
      ComplexWriter writer = new ComplexWriterImpl("root", parent);
      StructWriter rootWriter = writer.rootAsStruct();
      ListWriter listWriter = rootWriter.list("list");

      StructWriter innerStructWriter = listWriter.struct();
      IntWriter outerIntWriter = listWriter.integer();
      rootWriter.start();
      listWriter.startList();
      outerIntWriter.writeInt(1);
      outerIntWriter.writeInt(2);
      innerStructWriter.start();
      IntWriter intWriter = innerStructWriter.integer("a");
      intWriter.writeInt(1);
      innerStructWriter.end();
      innerStructWriter.start();
      intWriter = innerStructWriter.integer("a");
      intWriter.writeInt(2);
      innerStructWriter.end();
      listWriter.endList();
      rootWriter.end();
      writer.setValueCount(1);

      StructVector structVector = (StructVector) parent.getChild("root");
      TransferPair tp = structVector.getTransferPair(allocator);
      tp.splitAndTransfer(0, 1);
      NonNullableStructVector toStructVector = (NonNullableStructVector) tp.getTo();
      JsonStringHashMap<?, ?> toMapValue = (JsonStringHashMap<?, ?>) toStructVector.getObject(0);
      JsonStringArrayList<?> object = (JsonStringArrayList<?>) toMapValue.get("list");
      assertEquals(1, object.get(0));
      assertEquals(2, object.get(1));
      JsonStringHashMap<?, ?> innerStruct = (JsonStringHashMap<?, ?>) object.get(2);
      assertEquals(1, innerStruct.get("a"));
      innerStruct = (JsonStringHashMap<?, ?>) object.get(3);
      assertEquals(2, innerStruct.get("a"));
      toStructVector.close();
    }
  }

  @Test
  public void complexCopierWithListView() {
    try (NonNullableStructVector parent = NonNullableStructVector.empty("parent", allocator)) {
      ComplexWriter writer = new ComplexWriterImpl("root", parent);
      StructWriter rootWriter = writer.rootAsStruct();
      ListWriter listViewWriter = rootWriter.listView("listView");

      StructWriter innerStructWriter = listViewWriter.struct();
      IntWriter outerIntWriter = listViewWriter.integer();
      rootWriter.start();
      listViewWriter.startListView();
      outerIntWriter.writeInt(1);
      outerIntWriter.writeInt(2);
      innerStructWriter.start();
      IntWriter intWriter = innerStructWriter.integer("a");
      intWriter.writeInt(1);
      innerStructWriter.end();
      innerStructWriter.start();
      intWriter = innerStructWriter.integer("a");
      intWriter.writeInt(2);
      innerStructWriter.end();
      listViewWriter.endListView();
      rootWriter.end();
      writer.setValueCount(1);

      StructVector structVector = (StructVector) parent.getChild("root");
      TransferPair tp = structVector.getTransferPair(allocator);
      tp.splitAndTransfer(0, 1);
      NonNullableStructVector toStructVector = (NonNullableStructVector) tp.getTo();
      JsonStringHashMap<?, ?> toMapValue = (JsonStringHashMap<?, ?>) toStructVector.getObject(0);
      JsonStringArrayList<?> object = (JsonStringArrayList<?>) toMapValue.get("listView");
      assertEquals(1, object.get(0));
      assertEquals(2, object.get(1));
      JsonStringHashMap<?, ?> innerStruct = (JsonStringHashMap<?, ?>) object.get(2);
      assertEquals(1, innerStruct.get("a"));
      innerStruct = (JsonStringHashMap<?, ?>) object.get(3);
      assertEquals(2, innerStruct.get("a"));
      toStructVector.close();
    }
  }

  @Test
  public void testSingleStructWriter1() {
    /* initialize a SingleStructWriter with empty StructVector and then lazily
     * create all vectors with expected initialCapacity.
     */
    try (NonNullableStructVector parent = NonNullableStructVector.empty("parent", allocator)) {
      SingleStructWriter singleStructWriter = new SingleStructWriter(parent);

      int initialCapacity = 1024;
      singleStructWriter.setInitialCapacity(initialCapacity);

      IntWriter intWriter = singleStructWriter.integer("intField");
      BigIntWriter bigIntWriter = singleStructWriter.bigInt("bigIntField");
      Float4Writer float4Writer = singleStructWriter.float4("float4Field");
      Float8Writer float8Writer = singleStructWriter.float8("float8Field");
      ListWriter listWriter = singleStructWriter.list("listField");
      ListWriter listViewWriter = singleStructWriter.listView("listViewField");
      MapWriter mapWriter = singleStructWriter.map("mapField", false);

      int intValue = 100;
      long bigIntValue = 10000;
      float float4Value = 100.5f;
      double float8Value = 100.375;

      for (int i = 0; i < initialCapacity; i++) {
        singleStructWriter.start();

        intWriter.writeInt(intValue + i);
        bigIntWriter.writeBigInt(bigIntValue + (long) i);
        float4Writer.writeFloat4(float4Value + (float) i);
        float8Writer.writeFloat8(float8Value + (double) i);

        listWriter.setPosition(i);
        listWriter.startList();
        listWriter.integer().writeInt(intValue + i);
        listWriter.integer().writeInt(intValue + i + 1);
        listWriter.integer().writeInt(intValue + i + 2);
        listWriter.integer().writeInt(intValue + i + 3);
        listWriter.endList();

        listViewWriter.setPosition(i);
        listViewWriter.startListView();
        listViewWriter.integer().writeInt(intValue + i);
        listViewWriter.integer().writeInt(intValue + i + 1);
        listViewWriter.integer().writeInt(intValue + i + 2);
        listViewWriter.integer().writeInt(intValue + i + 3);
        listViewWriter.endListView();

        mapWriter.setPosition(i);
        mapWriter.startMap();
        mapWriter.startEntry();
        mapWriter.key().integer().writeInt(intValue + i);
        mapWriter.value().integer().writeInt(intValue + i + 1);
        mapWriter.endEntry();
        mapWriter.startEntry();
        mapWriter.key().integer().writeInt(intValue + i + 2);
        mapWriter.value().integer().writeInt(intValue + i + 3);
        mapWriter.endEntry();
        mapWriter.endMap();

        singleStructWriter.end();
      }

      IntVector intVector = (IntVector) parent.getChild("intField");
      BigIntVector bigIntVector = (BigIntVector) parent.getChild("bigIntField");
      Float4Vector float4Vector = (Float4Vector) parent.getChild("float4Field");
      Float8Vector float8Vector = (Float8Vector) parent.getChild("float8Field");

      int capacity = singleStructWriter.getValueCapacity();
      assertTrue(capacity >= initialCapacity && capacity < initialCapacity * 2);
      capacity = intVector.getValueCapacity();
      assertTrue(capacity >= initialCapacity && capacity < initialCapacity * 2);
      capacity = bigIntVector.getValueCapacity();
      assertTrue(capacity >= initialCapacity && capacity < initialCapacity * 2);
      capacity = float4Vector.getValueCapacity();
      assertTrue(capacity >= initialCapacity && capacity < initialCapacity * 2);
      capacity = float8Vector.getValueCapacity();
      assertTrue(capacity >= initialCapacity && capacity < initialCapacity * 2);

      StructReader singleStructReader = new SingleStructReaderImpl(parent);

      IntReader intReader = singleStructReader.reader("intField");
      BigIntReader bigIntReader = singleStructReader.reader("bigIntField");
      Float4Reader float4Reader = singleStructReader.reader("float4Field");
      Float8Reader float8Reader = singleStructReader.reader("float8Field");
      UnionListReader listReader = (UnionListReader) singleStructReader.reader("listField");
      UnionListViewReader listViewReader =
          (UnionListViewReader) singleStructReader.reader("listViewField");
      UnionMapReader mapReader = (UnionMapReader) singleStructReader.reader("mapField");

      for (int i = 0; i < initialCapacity; i++) {
        intReader.setPosition(i);
        bigIntReader.setPosition(i);
        float4Reader.setPosition(i);
        float8Reader.setPosition(i);
        listReader.setPosition(i);
        listViewReader.setPosition(i);
        mapReader.setPosition(i);

        assertEquals(intValue + i, intReader.readInteger().intValue());
        assertEquals(bigIntValue + (long) i, bigIntReader.readLong().longValue());
        assertEquals(float4Value + (float) i, float4Reader.readFloat().floatValue(), 0);
        assertEquals(float8Value + (double) i, float8Reader.readDouble().doubleValue(), 0);

        for (int j = 0; j < 4; j++) {
          listReader.next();
          assertEquals(intValue + i + j, listReader.reader().readInteger().intValue());
        }

        for (int j = 0; j < 4; j++) {
          listViewReader.next();
          assertEquals(intValue + i + j, listViewReader.reader().readInteger().intValue());
        }

        for (int k = 0; k < 4; k += 2) {
          mapReader.next();
          assertEquals(intValue + k + i, mapReader.key().readInteger().intValue());
          assertEquals(intValue + k + i + 1, mapReader.value().readInteger().intValue());
        }
      }
    }
  }

  @Test
  public void testListWriterWithNulls() {
    try (ListVector listVector = ListVector.empty("list", allocator)) {
      listVector.setInitialCapacity(COUNT);
      listVector.allocateNew();
      listVector.getValidityBuffer().setOne(0, (int) listVector.getValidityBuffer().capacity());

      UnionListWriter listWriter = listVector.getWriter();

      // expected listVector :  [[null], null, [2, 4], null, [null], null, [6, 12], ...]
      createNullsWithListWriters(listWriter);
      listVector.setValueCount(COUNT);

      UnionListReader listReader = new UnionListReader(listVector);
      checkNullsWithListWriters(listReader);
    }
  }

  @Test
  public void testListViewWriterWithNulls() {
    try (ListViewVector listViewVector = ListViewVector.empty("listView", allocator)) {
      listViewVector.setInitialCapacity(COUNT);
      listViewVector.allocateNew();
      listViewVector
          .getValidityBuffer()
          .setOne(0, (int) listViewVector.getValidityBuffer().capacity());

      UnionListViewWriter listWriter = listViewVector.getWriter();

      // expected listVector :  [[null], null, [2, 4], null, [null], null, [6, 12], ...]
      createNullsWithListWriters(listWriter);
      listViewVector.setValueCount(COUNT);

      UnionListViewReader listReader = new UnionListViewReader(listViewVector);
      checkNullsWithListWriters(listReader);
    }
  }

  @Test
  public void testListOfListWriterWithNulls() {
    try (ListVector listVector = ListVector.empty("listoflist", allocator)) {
      listVector.setInitialCapacity(COUNT);
      listVector.allocateNew();
      listVector.getValidityBuffer().setOne(0, (int) listVector.getValidityBuffer().capacity());

      UnionListWriter listWriter = listVector.getWriter();

      // create list : [ [null], null, [[null, 2, 4]], null, [null], null, [[null, 6, 12]], ... ]
      for (int i = 0; i < COUNT; i++) {
        listWriter.setPosition(i);
        if (i % 2 == 0) {
          listWriter.startList();
          if (i % 4 == 0) {
            listWriter.list().writeNull();
          } else {
            listWriter.list().startList();
            listWriter.list().integer().writeNull();
            listWriter.list().integer().writeInt(i);
            listWriter.list().integer().writeInt(i * 2);
            listWriter.list().endList();
          }
          listWriter.endList();
        } else {
          listWriter.writeNull();
        }
      }
      listVector.setValueCount(COUNT);

      UnionListReader listReader = new UnionListReader(listVector);
      for (int i = 0; i < COUNT; i++) {
        listReader.setPosition(i);
        if (i % 2 == 0) {
          assertTrue(listReader.isSet());
          listReader.next();
          if (i % 4 == 0) {
            assertFalse(listReader.reader().isSet());
          } else {
            listReader.reader().next();
            assertFalse(listReader.reader().reader().isSet());
            listReader.reader().next();
            assertEquals(i, listReader.reader().reader().readInteger().intValue());
            listReader.reader().next();
            assertEquals(i * 2, listReader.reader().reader().readInteger().intValue());
          }
        } else {
          assertFalse(listReader.isSet());
        }
      }
    }
  }

  @Test
  public void testListViewOfListViewWriterWithNulls() {
    try (ListViewVector listViewVector = ListViewVector.empty("listViewoflistView", allocator)) {
      listViewVector.setInitialCapacity(COUNT);
      listViewVector.allocateNew();
      listViewVector
          .getValidityBuffer()
          .setOne(0, (int) listViewVector.getValidityBuffer().capacity());

      UnionListViewWriter listViewWriter = listViewVector.getWriter();

      // create list : [ [null], null, [[null, 2, 4]], null, [null], null, [[null, 6, 12]], ... ]
      for (int i = 0; i < COUNT; i++) {
        listViewWriter.setPosition(i);
        if (i % 2 == 0) {
          listViewWriter.startListView();
          if (i % 4 == 0) {
            listViewWriter.listView().writeNull();
          } else {
            listViewWriter.listView().startListView();
            listViewWriter.listView().integer().writeNull();
            listViewWriter.listView().integer().writeInt(i);
            listViewWriter.listView().integer().writeInt(i * 2);
            listViewWriter.listView().endListView();
          }
          listViewWriter.endListView();
        } else {
          listViewWriter.writeNull();
        }
      }
      listViewVector.setValueCount(COUNT);

      UnionListViewReader listViewReader = new UnionListViewReader(listViewVector);
      for (int i = 0; i < COUNT; i++) {
        listViewReader.setPosition(i);
        if (i % 2 == 0) {
          assertTrue(listViewReader.isSet());
          listViewReader.next();
          if (i % 4 == 0) {
            assertFalse(listViewReader.reader().isSet());
          } else {
            listViewReader.reader().next();
            assertFalse(listViewReader.reader().reader().isSet());
            listViewReader.reader().next();
            assertEquals(i, listViewReader.reader().reader().readInteger().intValue());
            listViewReader.reader().next();
            assertEquals(i * 2, listViewReader.reader().reader().readInteger().intValue());
          }
        } else {
          assertFalse(listViewReader.isSet());
        }
      }
    }
  }

  @Test
  public void testListOfListOfListWriterWithNulls() {
    try (ListVector listVector = ListVector.empty("listoflistoflist", allocator)) {
      listVector.setInitialCapacity(COUNT);
      listVector.allocateNew();
      listVector.getValidityBuffer().setOne(0, (int) listVector.getValidityBuffer().capacity());

      UnionListWriter listWriter = listVector.getWriter();

      // create list : [ null, [null], [[null]], [[[null, 1, 2]]], null, [null], ...
      for (int i = 0; i < COUNT; i++) {
        listWriter.setPosition(i);
        if (i % 4 == 0) {
          listWriter.writeNull();
        } else {
          listWriter.startList();
          if (i % 4 == 1) {
            listWriter.list().writeNull();
          } else if (i % 4 == 2) {
            listWriter.list().startList();
            listWriter.list().list().writeNull();
            listWriter.list().endList();
          } else {
            listWriter.list().startList();
            listWriter.list().list().startList();
            listWriter.list().list().integer().writeNull();
            listWriter.list().list().integer().writeInt(i);
            listWriter.list().list().integer().writeInt(i * 2);
            listWriter.list().list().endList();
            listWriter.list().endList();
          }
          listWriter.endList();
        }
      }
      listVector.setValueCount(COUNT);

      UnionListReader listReader = new UnionListReader(listVector);
      for (int i = 0; i < COUNT; i++) {
        listReader.setPosition(i);
        if (i % 4 == 0) {
          assertFalse(listReader.isSet());
        } else {
          assertTrue(listReader.isSet());
          listReader.next();
          if (i % 4 == 1) {
            assertFalse(listReader.reader().isSet());
          } else if (i % 4 == 2) {
            listReader.reader().next();
            assertFalse(listReader.reader().reader().isSet());
          } else {
            listReader.reader().next();
            listReader.reader().reader().next();
            assertFalse(listReader.reader().reader().reader().isSet());
            listReader.reader().reader().next();
            assertEquals(i, listReader.reader().reader().reader().readInteger().intValue());
            listReader.reader().reader().next();
            assertEquals(i * 2, listReader.reader().reader().reader().readInteger().intValue());
          }
        }
      }
    }
  }

  @Test
  public void testListViewOfListViewOfListViewWriterWithNulls() {
    try (ListViewVector listViewVector =
        ListViewVector.empty("listViewoflistViewoflistView", allocator)) {
      listViewVector.setInitialCapacity(COUNT);
      listViewVector.allocateNew();
      listViewVector
          .getValidityBuffer()
          .setOne(0, (int) listViewVector.getValidityBuffer().capacity());

      UnionListViewWriter listViewWriter = listViewVector.getWriter();

      // create list : [ null, [null], [[null]], [[[null, 1, 2]]], null, [null], ...
      for (int i = 0; i < COUNT; i++) {
        listViewWriter.setPosition(i);
        if (i % 4 == 0) {
          listViewWriter.writeNull();
        } else {
          listViewWriter.startListView();
          if (i % 4 == 1) {
            listViewWriter.listView().writeNull();
          } else if (i % 4 == 2) {
            listViewWriter.listView().startListView();
            listViewWriter.listView().listView().writeNull();
            listViewWriter.listView().endListView();
          } else {
            listViewWriter.listView().startListView();
            listViewWriter.listView().listView().startListView();
            listViewWriter.listView().listView().integer().writeNull();
            listViewWriter.listView().listView().integer().writeInt(i);
            listViewWriter.listView().listView().integer().writeInt(i * 2);
            listViewWriter.listView().listView().endListView();
            listViewWriter.listView().endListView();
          }
          listViewWriter.endListView();
        }
      }
      listViewVector.setValueCount(COUNT);

      UnionListViewReader listViewReader = new UnionListViewReader(listViewVector);
      for (int i = 0; i < COUNT; i++) {
        listViewReader.setPosition(i);
        if (i % 4 == 0) {
          assertFalse(listViewReader.isSet());
        } else {
          assertTrue(listViewReader.isSet());
          listViewReader.next();
          if (i % 4 == 1) {
            assertFalse(listViewReader.reader().isSet());
          } else if (i % 4 == 2) {
            listViewReader.reader().next();
            assertFalse(listViewReader.reader().reader().isSet());
          } else {
            listViewReader.reader().next();
            listViewReader.reader().reader().next();
            assertFalse(listViewReader.reader().reader().reader().isSet());
            listViewReader.reader().reader().next();
            assertEquals(i, listViewReader.reader().reader().reader().readInteger().intValue());
            listViewReader.reader().reader().next();
            assertEquals(i * 2, listViewReader.reader().reader().reader().readInteger().intValue());
          }
        }
      }
    }
  }

  @Test
  public void testStructOfList() {
    try (StructVector structVector = StructVector.empty("struct1", allocator)) {
      structVector.addOrGetList("childList1");
      NullableStructReaderImpl structReader = structVector.getReader();
      FieldReader childListReader = structReader.reader("childList1");
      assertNotNull(childListReader);
    }

    try (StructVector structVector = StructVector.empty("struct2", allocator)) {
      structVector.addOrGetList("childList2");
      NullableStructWriter structWriter = structVector.getWriter();
      structWriter.start();
      ListWriter listWriter = structWriter.list("childList2");
      listWriter.startList();
      listWriter.integer().writeInt(10);
      listWriter.endList();
      structWriter.end();

      NullableStructReaderImpl structReader = structVector.getReader();
      FieldReader childListReader = structReader.reader("childList2");
      int size = childListReader.size();
      assertEquals(1, size);
      int data = childListReader.reader().readInteger();
      assertEquals(10, data);
    }

    try (StructVector structVector = StructVector.empty("struct3", allocator)) {
      structVector.addOrGetList("childList3");
      NullableStructWriter structWriter = structVector.getWriter();
      for (int i = 0; i < 5; ++i) {
        structWriter.setPosition(i);
        structWriter.start();
        ListWriter listWriter = structWriter.list("childList3");
        listWriter.startList();
        listWriter.integer().writeInt(i);
        listWriter.endList();
        structWriter.end();
      }

      NullableStructReaderImpl structReader = structVector.getReader();
      structReader.setPosition(3);
      FieldReader childListReader = structReader.reader("childList3");
      int size = childListReader.size();
      assertEquals(1, size);
      int data = ((List<Integer>) childListReader.readObject()).get(0);
      assertEquals(3, data);
    }

    try (StructVector structVector = StructVector.empty("struct4", allocator)) {
      structVector.addOrGetList("childList4");
      NullableStructWriter structWriter = structVector.getWriter();
      for (int i = 0; i < 5; ++i) {
        structWriter.setPosition(i);
        structWriter.start();
        structWriter.writeNull();
        structWriter.end();
      }

      NullableStructReaderImpl structReader = structVector.getReader();
      structReader.setPosition(3);
      FieldReader childListReader = structReader.reader("childList4");
      int size = childListReader.size();
      assertEquals(0, size);
    }
  }

  @Test
  public void testMap() {
    try (NonNullableStructVector parent = NonNullableStructVector.empty("parent", allocator)) {
      ComplexWriter writer = new ComplexWriterImpl("root", parent);
      MapWriter mapWriter = writer.rootAsMap(false);
      for (int i = 0; i < COUNT; i++) {
        mapWriter.startMap();
        for (int j = 0; j < i % 7; j++) {
          mapWriter.startEntry();
          if (j % 2 == 0) {
            mapWriter.key().integer().writeInt(j);
            mapWriter.value().integer().writeInt(j + 1);
          } else {
            IntHolder keyHolder = new IntHolder();
            keyHolder.value = j;
            IntHolder valueHolder = new IntHolder();
            valueHolder.value = j + 1;
            mapWriter.key().integer().write(keyHolder);
            mapWriter.value().integer().write(valueHolder);
          }
          mapWriter.endEntry();
        }
        mapWriter.endMap();
      }
      writer.setValueCount(COUNT);
      UnionMapReader mapReader = (UnionMapReader) new SingleStructReaderImpl(parent).reader("root");
      for (int i = 0; i < COUNT; i++) {
        mapReader.setPosition(i);
        for (int j = 0; j < i % 7; j++) {
          mapReader.next();
          assertEquals(j, mapReader.key().readInteger().intValue());
          assertEquals(j + 1, mapReader.value().readInteger().intValue());
        }
      }
    }
  }

  @Test
  public void testMapWithNulls() {
    try (NonNullableStructVector parent = NonNullableStructVector.empty("parent", allocator)) {
      ComplexWriter writer = new ComplexWriterImpl("root", parent);
      MapWriter mapWriter = writer.rootAsMap(false);
      mapWriter.startMap();
      mapWriter.startEntry();
      mapWriter.key().integer().writeNull();
      mapWriter.value().integer().writeInt(1);
      mapWriter.endEntry();
      mapWriter.endMap();
      writer.setValueCount(1);
      UnionMapReader mapReader = (UnionMapReader) new SingleStructReaderImpl(parent).reader("root");
      assertNull(mapReader.key().readInteger());
      assertEquals(1, mapReader.value().readInteger().intValue());
    }
  }

  @Test
  public void testMapWithListKey() {
    try (NonNullableStructVector parent = NonNullableStructVector.empty("parent", allocator)) {
      ComplexWriter writer = new ComplexWriterImpl("root", parent);
      MapWriter mapWriter = writer.rootAsMap(false);
      mapWriter.startMap();
      mapWriter.startEntry();
      mapWriter.key().list().startList();
      for (int i = 0; i < 3; i++) {
        mapWriter.key().list().integer().writeInt(i);
      }
      mapWriter.key().list().endList();
      mapWriter.value().integer().writeInt(1);
      mapWriter.endEntry();
      mapWriter.endMap();
      writer.setValueCount(1);
      UnionMapReader mapReader = (UnionMapReader) new SingleStructReaderImpl(parent).reader("root");
      mapReader.key().next();
      assertEquals(0, mapReader.key().reader().readInteger().intValue());
      mapReader.key().next();
      assertEquals(1, mapReader.key().reader().readInteger().intValue());
      mapReader.key().next();
      assertEquals(2, mapReader.key().reader().readInteger().intValue());
      assertEquals(1, mapReader.value().readInteger().intValue());
    }
  }

  @Test
  public void testMapWithStructKey() {
    try (NonNullableStructVector parent = NonNullableStructVector.empty("parent", allocator)) {
      ComplexWriter writer = new ComplexWriterImpl("root", parent);
      MapWriter mapWriter = writer.rootAsMap(false);
      mapWriter.startMap();
      mapWriter.startEntry();
      mapWriter.key().struct().start();
      mapWriter.key().struct().integer("value1").writeInt(1);
      mapWriter.key().struct().integer("value2").writeInt(2);
      mapWriter.key().struct().end();
      mapWriter.value().integer().writeInt(1);
      mapWriter.endEntry();
      mapWriter.endMap();
      writer.setValueCount(1);
      UnionMapReader mapReader = (UnionMapReader) new SingleStructReaderImpl(parent).reader("root");
      assertEquals(1, mapReader.key().reader("value1").readInteger().intValue());
      assertEquals(2, mapReader.key().reader("value2").readInteger().intValue());
      assertEquals(1, mapReader.value().readInteger().intValue());
    }
  }

  @Test
  public void structWriterVarCharHelpers() {
    try (NonNullableStructVector parent = NonNullableStructVector.empty("parent", allocator)) {
      ComplexWriter writer = new ComplexWriterImpl("root", parent, false, true);
      StructWriter rootWriter = writer.rootAsStruct();
      rootWriter.start();
      rootWriter.setPosition(0);
      rootWriter.varChar("c").writeVarChar(new Text("row1"));
      rootWriter.setPosition(1);
      rootWriter.varChar("c").writeVarChar("row2");
      rootWriter.end();

      VarCharVector vector =
          parent.getChild("root", StructVector.class).getChild("c", VarCharVector.class);

      assertEquals("row1", vector.getObject(0).toString());
      assertEquals("row2", vector.getObject(1).toString());
    }
  }

  @Test
  public void structWriterVarCharViewHelpers() {
    try (NonNullableStructVector parent = NonNullableStructVector.empty("parent", allocator)) {
      ComplexWriter writer = new ComplexWriterImpl("root", parent, false, true);
      StructWriter rootWriter = writer.rootAsStruct();
      rootWriter.start();
      rootWriter.setPosition(0);
      rootWriter.viewVarChar("c").writeViewVarChar(new Text("row1"));
      rootWriter.setPosition(1);
      rootWriter.viewVarChar("c").writeViewVarChar("row2");
      rootWriter.end();

      ViewVarCharVector vector =
          parent.getChild("root", StructVector.class).getChild("c", ViewVarCharVector.class);

      assertEquals("row1", vector.getObject(0).toString());
      assertEquals("row2", vector.getObject(1).toString());
    }
  }

  @Test
  public void structWriterLargeVarCharHelpers() {
    try (NonNullableStructVector parent = NonNullableStructVector.empty("parent", allocator)) {
      ComplexWriter writer = new ComplexWriterImpl("root", parent, false, true);
      StructWriter rootWriter = writer.rootAsStruct();
      rootWriter.start();
      rootWriter.setPosition(0);
      rootWriter.largeVarChar("c").writeLargeVarChar(new Text("row1"));
      rootWriter.setPosition(1);
      rootWriter.largeVarChar("c").writeLargeVarChar("row2");
      rootWriter.end();

      LargeVarCharVector vector =
          parent.getChild("root", StructVector.class).getChild("c", LargeVarCharVector.class);

      assertEquals("row1", vector.getObject(0).toString());
      assertEquals("row2", vector.getObject(1).toString());
    }
  }

  @Test
  public void structWriterVarBinaryHelpers() {
    try (NonNullableStructVector parent = NonNullableStructVector.empty("parent", allocator)) {
      ComplexWriter writer = new ComplexWriterImpl("root", parent, false, true);
      StructWriter rootWriter = writer.rootAsStruct();
      rootWriter.start();
      rootWriter.setPosition(0);
      rootWriter.varBinary("c").writeVarBinary("row1".getBytes(StandardCharsets.UTF_8));
      rootWriter.setPosition(1);
      rootWriter
          .varBinary("c")
          .writeVarBinary(
              "row2".getBytes(StandardCharsets.UTF_8),
              0,
              "row2".getBytes(StandardCharsets.UTF_8).length);
      rootWriter.setPosition(2);
      rootWriter
          .varBinary("c")
          .writeVarBinary(ByteBuffer.wrap("row3".getBytes(StandardCharsets.UTF_8)));
      rootWriter.setPosition(3);
      rootWriter
          .varBinary("c")
          .writeVarBinary(
              ByteBuffer.wrap("row4".getBytes(StandardCharsets.UTF_8)),
              0,
              "row4".getBytes(StandardCharsets.UTF_8).length);
      rootWriter.end();

      VarBinaryVector uv =
          parent.getChild("root", StructVector.class).getChild("c", VarBinaryVector.class);

      assertEquals("row1", new String(uv.get(0), StandardCharsets.UTF_8));
      assertEquals("row2", new String(uv.get(1), StandardCharsets.UTF_8));
      assertEquals("row3", new String(uv.get(2), StandardCharsets.UTF_8));
      assertEquals("row4", new String(uv.get(3), StandardCharsets.UTF_8));
    }
  }

  @Test
  public void structWriterLargeVarBinaryHelpers() {
    try (NonNullableStructVector parent = NonNullableStructVector.empty("parent", allocator)) {
      ComplexWriter writer = new ComplexWriterImpl("root", parent, false, true);
      StructWriter rootWriter = writer.rootAsStruct();
      rootWriter.start();
      rootWriter.setPosition(0);
      rootWriter.largeVarBinary("c").writeLargeVarBinary("row1".getBytes(StandardCharsets.UTF_8));
      rootWriter.setPosition(1);
      rootWriter
          .largeVarBinary("c")
          .writeLargeVarBinary(
              "row2".getBytes(StandardCharsets.UTF_8),
              0,
              "row2".getBytes(StandardCharsets.UTF_8).length);
      rootWriter.setPosition(2);
      rootWriter
          .largeVarBinary("c")
          .writeLargeVarBinary(ByteBuffer.wrap("row3".getBytes(StandardCharsets.UTF_8)));
      rootWriter.setPosition(3);
      rootWriter
          .largeVarBinary("c")
          .writeLargeVarBinary(
              ByteBuffer.wrap("row4".getBytes(StandardCharsets.UTF_8)),
              0,
              "row4".getBytes(StandardCharsets.UTF_8).length);
      rootWriter.end();

      LargeVarBinaryVector uv =
          parent.getChild("root", StructVector.class).getChild("c", LargeVarBinaryVector.class);

      assertEquals("row1", new String(uv.get(0), StandardCharsets.UTF_8));
      assertEquals("row2", new String(uv.get(1), StandardCharsets.UTF_8));
      assertEquals("row3", new String(uv.get(2), StandardCharsets.UTF_8));
      assertEquals("row4", new String(uv.get(3), StandardCharsets.UTF_8));
    }
  }

  @Test
  public void listVarCharHelpers() {
    try (ListVector listVector = ListVector.empty("list", allocator)) {
      listVector.allocateNew();
      UnionListWriter listWriter = new UnionListWriter(listVector);
      listWriter.startList();
      listWriter.writeVarChar("row1");
      listWriter.writeVarChar(new Text("row2"));
      listWriter.endList();
      listWriter.setValueCount(1);
      assertEquals("row1", listVector.getObject(0).get(0).toString());
      assertEquals("row2", listVector.getObject(0).get(1).toString());
    }
  }

  @Test
  public void listLargeVarCharHelpers() {
    try (ListVector listVector = ListVector.empty("list", allocator)) {
      listVector.allocateNew();
      UnionListWriter listWriter = new UnionListWriter(listVector);
      listWriter.startList();
      listWriter.writeLargeVarChar("row1");
      listWriter.writeLargeVarChar(new Text("row2"));
      listWriter.endList();
      listWriter.setValueCount(1);
      assertEquals("row1", listVector.getObject(0).get(0).toString());
      assertEquals("row2", listVector.getObject(0).get(1).toString());
    }
  }

  @Test
  public void listVarBinaryHelpers() {
    try (ListVector listVector = ListVector.empty("list", allocator)) {
      listVector.allocateNew();
      UnionListWriter listWriter = new UnionListWriter(listVector);
      listWriter.startList();
      listWriter.writeVarBinary("row1".getBytes(StandardCharsets.UTF_8));
      listWriter.writeVarBinary(
          "row2".getBytes(StandardCharsets.UTF_8),
          0,
          "row2".getBytes(StandardCharsets.UTF_8).length);
      listWriter.writeVarBinary(ByteBuffer.wrap("row3".getBytes(StandardCharsets.UTF_8)));
      listWriter.writeVarBinary(
          ByteBuffer.wrap("row4".getBytes(StandardCharsets.UTF_8)),
          0,
          "row4".getBytes(StandardCharsets.UTF_8).length);
      listWriter.endList();
      listWriter.setValueCount(1);
      assertEquals(
          "row1", new String((byte[]) listVector.getObject(0).get(0), StandardCharsets.UTF_8));
      assertEquals(
          "row2", new String((byte[]) listVector.getObject(0).get(1), StandardCharsets.UTF_8));
      assertEquals(
          "row3", new String((byte[]) listVector.getObject(0).get(2), StandardCharsets.UTF_8));
      assertEquals(
          "row4", new String((byte[]) listVector.getObject(0).get(3), StandardCharsets.UTF_8));
    }
  }

  @Test
  public void listLargeVarBinaryHelpers() {
    try (ListVector listVector = ListVector.empty("list", allocator)) {
      listVector.allocateNew();
      UnionListWriter listWriter = new UnionListWriter(listVector);
      listWriter.startList();
      listWriter.writeLargeVarBinary("row1".getBytes(StandardCharsets.UTF_8));
      listWriter.writeLargeVarBinary(
          "row2".getBytes(StandardCharsets.UTF_8),
          0,
          "row2".getBytes(StandardCharsets.UTF_8).length);
      listWriter.writeLargeVarBinary(ByteBuffer.wrap("row3".getBytes(StandardCharsets.UTF_8)));
      listWriter.writeLargeVarBinary(
          ByteBuffer.wrap("row4".getBytes(StandardCharsets.UTF_8)),
          0,
          "row4".getBytes(StandardCharsets.UTF_8).length);
      listWriter.endList();
      listWriter.setValueCount(1);
      assertEquals(
          "row1", new String((byte[]) listVector.getObject(0).get(0), StandardCharsets.UTF_8));
      assertEquals(
          "row2", new String((byte[]) listVector.getObject(0).get(1), StandardCharsets.UTF_8));
      assertEquals(
          "row3", new String((byte[]) listVector.getObject(0).get(2), StandardCharsets.UTF_8));
      assertEquals(
          "row4", new String((byte[]) listVector.getObject(0).get(3), StandardCharsets.UTF_8));
    }
  }

  @Test
  public void unionWithVarCharAndBinaryHelpers() throws Exception {
    try (UnionVector vector =
        new UnionVector("union", allocator, /* field type */ null, /* call-back */ null)) {
      UnionWriter unionWriter = new UnionWriter(vector);
      unionWriter.allocate();
      unionWriter.start();
      unionWriter.setPosition(0);
      unionWriter.writeVarChar("row1");
      unionWriter.setPosition(1);
      unionWriter.writeVarChar(new Text("row2"));
      unionWriter.setPosition(2);
      unionWriter.writeLargeVarChar("row3");
      unionWriter.setPosition(3);
      unionWriter.writeLargeVarChar(new Text("row4"));
      unionWriter.setPosition(4);
      unionWriter.writeVarBinary("row5".getBytes(StandardCharsets.UTF_8));
      unionWriter.setPosition(5);
      unionWriter.writeVarBinary(
          "row6".getBytes(StandardCharsets.UTF_8),
          0,
          "row6".getBytes(StandardCharsets.UTF_8).length);
      unionWriter.setPosition(6);
      unionWriter.writeVarBinary(ByteBuffer.wrap("row7".getBytes(StandardCharsets.UTF_8)));
      unionWriter.setPosition(7);
      unionWriter.writeVarBinary(
          ByteBuffer.wrap("row8".getBytes(StandardCharsets.UTF_8)),
          0,
          "row8".getBytes(StandardCharsets.UTF_8).length);
      unionWriter.setPosition(8);
      unionWriter.writeLargeVarBinary("row9".getBytes(StandardCharsets.UTF_8));
      unionWriter.setPosition(9);
      unionWriter.writeLargeVarBinary(
          "row10".getBytes(StandardCharsets.UTF_8),
          0,
          "row10".getBytes(StandardCharsets.UTF_8).length);
      unionWriter.setPosition(10);
      unionWriter.writeLargeVarBinary(ByteBuffer.wrap("row11".getBytes(StandardCharsets.UTF_8)));
      unionWriter.setPosition(11);
      unionWriter.writeLargeVarBinary(
          ByteBuffer.wrap("row12".getBytes(StandardCharsets.UTF_8)),
          0,
          "row12".getBytes(StandardCharsets.UTF_8).length);
      unionWriter.end();

      assertEquals("row1", new String(vector.getVarCharVector().get(0), StandardCharsets.UTF_8));
      assertEquals("row2", new String(vector.getVarCharVector().get(1), StandardCharsets.UTF_8));
      assertEquals(
          "row3", new String(vector.getLargeVarCharVector().get(2), StandardCharsets.UTF_8));
      assertEquals(
          "row4", new String(vector.getLargeVarCharVector().get(3), StandardCharsets.UTF_8));
      assertEquals("row5", new String(vector.getVarBinaryVector().get(4), StandardCharsets.UTF_8));
      assertEquals("row6", new String(vector.getVarBinaryVector().get(5), StandardCharsets.UTF_8));
      assertEquals("row7", new String(vector.getVarBinaryVector().get(6), StandardCharsets.UTF_8));
      assertEquals("row8", new String(vector.getVarBinaryVector().get(7), StandardCharsets.UTF_8));
      assertEquals(
          "row9", new String(vector.getLargeVarBinaryVector().get(8), StandardCharsets.UTF_8));
      assertEquals(
          "row10", new String(vector.getLargeVarBinaryVector().get(9), StandardCharsets.UTF_8));
      assertEquals(
          "row11", new String(vector.getLargeVarBinaryVector().get(10), StandardCharsets.UTF_8));
      assertEquals(
          "row12", new String(vector.getLargeVarBinaryVector().get(11), StandardCharsets.UTF_8));
    }
  }
}
