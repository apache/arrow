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

import static org.junit.Assert.*;

import java.math.BigDecimal;
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
import org.apache.arrow.vector.SchemaChangeCallBack;
import org.apache.arrow.vector.complex.ListVector;
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
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestComplexWriter {

  private BufferAllocator allocator;

  private static final int COUNT = 100;

  @Before
  public void init() {
    allocator = new RootAllocator(Integer.MAX_VALUE);
  }

  @After
  public void terminate() throws Exception {
    allocator.close();
  }

  @Test
  public void simpleNestedTypes() {
    NonNullableStructVector parent = populateStructVector(null);
    StructReader rootReader = new SingleStructReaderImpl(parent).reader("root");
    for (int i = 0; i < COUNT; i++) {
      rootReader.setPosition(i);
      Assert.assertEquals(i, rootReader.reader("int").readInteger().intValue());
      Assert.assertEquals(i, rootReader.reader("bigInt").readLong().longValue());
    }

    parent.close();
  }

  @Test
  public void transferPairSchemaChange() {
    SchemaChangeCallBack callBack1 = new SchemaChangeCallBack();
    SchemaChangeCallBack callBack2 = new SchemaChangeCallBack();
    try (NonNullableStructVector parent = populateStructVector(callBack1)) {
      TransferPair tp = parent.getTransferPair("newVector", allocator, callBack2);

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
        new NonNullableStructVector("parent", allocator, new FieldType(false, Struct.INSTANCE, null, null), callBack);
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
    try (NonNullableStructVector structVector = NonNullableStructVector.empty("parent", allocator)) {
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
   * This test is similar to {@link #nullableStruct()} ()} but we get the inner struct writer once at the beginning.
   */
  @Test
  public void nullableStruct2() {
    try (NonNullableStructVector structVector = NonNullableStructVector.empty("parent", allocator)) {
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

  private void checkNullableStruct(NonNullableStructVector structVector) {
    StructReader rootReader = new SingleStructReaderImpl(structVector).reader("root");
    for (int i = 0; i < COUNT; i++) {
      rootReader.setPosition(i);
      assertTrue("index is set: " + i, rootReader.isSet());
      FieldReader struct = rootReader.reader("struct");
      if (i % 2 == 0) {
        assertTrue("index is set: " + i, struct.isSet());
        assertNotNull("index is set: " + i, struct.readObject());
        assertEquals(i, struct.reader("nested").readLong().longValue());
      } else {
        assertFalse("index is not set: " + i, struct.isSet());
        assertNull("index is not set: " + i, struct.readObject());
      }
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
      assertTrue("row 0 list is not set", rootReader.reader("list").isSet());
      assertEquals(Long.valueOf(0), rootReader.reader("list").reader().readLong());

      rootReader.setPosition(1);
      assertFalse("row 1 list is set", rootReader.reader("list").isSet());
    }
  }

  @Test
  public void listScalarType() {
    try (ListVector listVector = ListVector.empty("list", allocator)) {
      listVector.allocateNew();
      UnionListWriter listWriter = new UnionListWriter(listVector);
      for (int i = 0; i < COUNT; i++) {
        listWriter.startList();
        for (int j = 0; j < i % 7; j++) {
          if (j % 2 == 0) {
            listWriter.writeInt(j);
          } else {
            IntHolder holder = new IntHolder();
            holder.value = j;
            listWriter.write(holder);
          }
        }
        listWriter.endList();
      }
      listWriter.setValueCount(COUNT);
      UnionListReader listReader = new UnionListReader(listVector);
      for (int i = 0; i < COUNT; i++) {
        listReader.setPosition(i);
        for (int j = 0; j < i % 7; j++) {
          listReader.next();
          assertEquals(j, listReader.reader().readInteger().intValue());
        }
      }
    }
  }

  @Test
  public void testListScalarNull() {
    /* Write to a integer list vector
     * each list of size 8 and having it's data values alternating between null and a non-null.
     * Read and verify
     */
    try (ListVector listVector = ListVector.empty("list", allocator)) {
      listVector.allocateNew();
      UnionListWriter listWriter = new UnionListWriter(listVector);
      for (int i = 0; i < COUNT; i++) {
        listWriter.startList();
        for (int j = 0; j < i % 7; j++) {
          if (j % 2 == 0) {
            listWriter.writeNull();
          } else {
            IntHolder holder = new IntHolder();
            holder.value = j;
            listWriter.write(holder);
          }
        }
        listWriter.endList();
      }
      listWriter.setValueCount(COUNT);
      UnionListReader listReader = new UnionListReader(listVector);
      for (int i = 0; i < COUNT; i++) {
        listReader.setPosition(i);
        for (int j = 0; j < i % 7; j++) {
          listReader.next();
          if (j % 2 == 0) {
            assertFalse("index is set: " + j, listReader.reader().isSet());
          } else {
            assertTrue("index is not set: " + j, listReader.reader().isSet());
            assertEquals(j, listReader.reader().readInteger().intValue());
          }
        }
      }
    }
  }

  @Test
  public void listDecimalType() {
    try (ListVector listVector = ListVector.empty("list", allocator)) {
      listVector.allocateNew();
      UnionListWriter listWriter = new UnionListWriter(listVector);
      DecimalHolder holder = new DecimalHolder();
      holder.buffer = allocator.buffer(DecimalVector.TYPE_WIDTH);
      ArrowType arrowType = new ArrowType.Decimal(10, 0, 128);
      for (int i = 0; i < COUNT; i++) {
        listWriter.startList();
        for (int j = 0; j < i % 7; j++) {
          if (j % 4 == 0) {
            listWriter.writeDecimal(new BigDecimal(j));
          } else if (j % 4 == 1) {
            DecimalUtility.writeBigDecimalToArrowBuf(new BigDecimal(j), holder.buffer, 0, DecimalVector.TYPE_WIDTH);
            holder.start = 0;
            holder.scale = 0;
            holder.precision = 10;
            listWriter.write(holder);
          } else if (j % 4 == 2) {
            DecimalUtility.writeBigDecimalToArrowBuf(new BigDecimal(j), holder.buffer, 0, DecimalVector.TYPE_WIDTH);
            listWriter.writeDecimal(0, holder.buffer, arrowType);
          } else {
            byte[] value = BigDecimal.valueOf(j).unscaledValue().toByteArray();
            listWriter.writeBigEndianBytesToDecimal(value, arrowType);
          }
        }
        listWriter.endList();
      }
      listWriter.setValueCount(COUNT);
      UnionListReader listReader = new UnionListReader(listVector);
      for (int i = 0; i < COUNT; i++) {
        listReader.setPosition(i);
        for (int j = 0; j < i % 7; j++) {
          listReader.next();
          Object expected = new BigDecimal(j);
          Object actual = listReader.reader().readBigDecimal();
          assertEquals(expected, actual);
        }
      }
      holder.buffer.close();
    }
  }

  @Test
  public void listTimeStampMilliTZType() {
    try (ListVector listVector = ListVector.empty("list", allocator)) {
      listVector.allocateNew();
      UnionListWriter listWriter = new UnionListWriter(listVector);
      for (int i = 0; i < COUNT; i++) {
        listWriter.startList();
        for (int j = 0; j < i % 7; j++) {
          if (j % 2 == 0) {
            listWriter.writeNull();
          } else {
            TimeStampMilliTZHolder holder = new TimeStampMilliTZHolder();
            holder.timezone = "FakeTimeZone";
            holder.value = j;
            listWriter.timeStampMilliTZ().write(holder);
          }
        }
        listWriter.endList();
      }
      listWriter.setValueCount(COUNT);
      UnionListReader listReader = new UnionListReader(listVector);
      for (int i = 0; i < COUNT; i++) {
        listReader.setPosition(i);
        for (int j = 0; j < i % 7; j++) {
          listReader.next();
          if (j % 2 == 0) {
            assertFalse("index is set: " + j, listReader.reader().isSet());
          } else {
            NullableTimeStampMilliTZHolder actual = new NullableTimeStampMilliTZHolder();
            listReader.reader().read(actual);
            assertEquals(j, actual.value);
            assertEquals("FakeTimeZone", actual.timezone);
          }
        }
      }
    }
  }

  @Test
  public void listDurationType() {
    try (ListVector listVector = ListVector.empty("list", allocator)) {
      listVector.allocateNew();
      UnionListWriter listWriter = new UnionListWriter(listVector);
      for (int i = 0; i < COUNT; i++) {
        listWriter.startList();
        for (int j = 0; j < i % 7; j++) {
          if (j % 2 == 0) {
            listWriter.writeNull();
          } else {
            DurationHolder holder = new DurationHolder();
            holder.unit = TimeUnit.MICROSECOND;
            holder.value = j;
            listWriter.duration().write(holder);
          }
        }
        listWriter.endList();
      }
      listWriter.setValueCount(COUNT);
      UnionListReader listReader = new UnionListReader(listVector);
      for (int i = 0; i < COUNT; i++) {
        listReader.setPosition(i);
        for (int j = 0; j < i % 7; j++) {
          listReader.next();
          if (j % 2 == 0) {
            assertFalse("index is set: " + j, listReader.reader().isSet());
          } else {
            NullableDurationHolder actual = new NullableDurationHolder();
            listReader.reader().read(actual);
            assertEquals(TimeUnit.MICROSECOND, actual.unit);
            assertEquals(j, actual.value);
          }
        }
      }
    }
  }

  @Test
  public void listFixedSizeBinaryType() throws Exception {
    List<ArrowBuf> bufs = new ArrayList<ArrowBuf>();
    try (ListVector listVector = ListVector.empty("list", allocator)) {
      listVector.allocateNew();
      UnionListWriter listWriter = new UnionListWriter(listVector);
      for (int i = 0; i < COUNT; i++) {
        listWriter.startList();
        for (int j = 0; j < i % 7; j++) {
          if (j % 2 == 0) {
            listWriter.writeNull();
          } else {
            ArrowBuf buf = allocator.buffer(4);
            buf.setInt(0, j);
            FixedSizeBinaryHolder holder = new FixedSizeBinaryHolder();
            holder.byteWidth = 4;
            holder.buffer = buf;
            listWriter.fixedSizeBinary().write(holder);
            bufs.add(buf);
          }
        }
        listWriter.endList();
      }
      listWriter.setValueCount(COUNT);
      UnionListReader listReader = new UnionListReader(listVector);
      for (int i = 0; i < COUNT; i++) {
        listReader.setPosition(i);
        for (int j = 0; j < i % 7; j++) {
          listReader.next();
          if (j % 2 == 0) {
            assertFalse("index is set: " + j, listReader.reader().isSet());
          } else {
            NullableFixedSizeBinaryHolder actual = new NullableFixedSizeBinaryHolder();
            listReader.reader().read(actual);
            assertEquals(j, actual.buffer.getInt(0));
            assertEquals(4, actual.byteWidth);
          }
        }
      }
    }
    AutoCloseables.close(bufs);
  }

  @Test
  public void listScalarTypeNullable() {
    try (ListVector listVector = ListVector.empty("list", allocator)) {
      listVector.allocateNew();
      UnionListWriter listWriter = new UnionListWriter(listVector);
      for (int i = 0; i < COUNT; i++) {
        if (i % 2 == 0) {
          listWriter.setPosition(i);
          listWriter.startList();
          for (int j = 0; j < i % 7; j++) {
            listWriter.writeInt(j);
          }
          listWriter.endList();
        }
      }
      listWriter.setValueCount(COUNT);
      UnionListReader listReader = new UnionListReader(listVector);
      for (int i = 0; i < COUNT; i++) {
        listReader.setPosition(i);
        if (i % 2 == 0) {
          assertTrue("index is set: " + i, listReader.isSet());
          assertEquals("correct length at: " + i, i % 7, ((List<?>) listReader.readObject()).size());
        } else {
          assertFalse("index is not set: " + i, listReader.isSet());
          assertNull("index is not set: " + i, listReader.readObject());
        }
      }
    }
  }

  @Test
  public void listStructType() {
    try (ListVector listVector = ListVector.empty("list", allocator)) {
      listVector.allocateNew();
      UnionListWriter listWriter = new UnionListWriter(listVector);
      StructWriter structWriter = listWriter.struct();
      for (int i = 0; i < COUNT; i++) {
        listWriter.startList();
        for (int j = 0; j < i % 7; j++) {
          structWriter.start();
          structWriter.integer("int").writeInt(j);
          structWriter.bigInt("bigInt").writeBigInt(j);
          structWriter.end();
        }
        listWriter.endList();
      }
      listWriter.setValueCount(COUNT);
      UnionListReader listReader = new UnionListReader(listVector);
      for (int i = 0; i < COUNT; i++) {
        listReader.setPosition(i);
        for (int j = 0; j < i % 7; j++) {
          listReader.next();
          Assert.assertEquals("record: " + i, j, listReader.reader().reader("int").readInteger().intValue());
          Assert.assertEquals(j, listReader.reader().reader("bigInt").readLong().longValue());
        }
      }
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
      checkListOfLists(listVector);
    }
  }

  /**
   * This test is similar to {@link #listListType()} but we get the inner list writer once at the beginning.
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
      checkListOfLists(listVector);
    }
  }

  private void checkListOfLists(final ListVector listVector) {
    UnionListReader listReader = new UnionListReader(listVector);
    for (int i = 0; i < COUNT; i++) {
      listReader.setPosition(i);
      for (int j = 0; j < i % 7; j++) {
        listReader.next();
        FieldReader innerListReader = listReader.reader();
        for (int k = 0; k < i % 13; k++) {
          innerListReader.next();
          Assert.assertEquals("record: " + i, k, innerListReader.reader().readInteger().intValue());
        }
      }
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
      checkUnionList(listVector);
    }
  }

  /**
   * This test is similar to {@link #unionListListType()} but we get the inner list writer once at the beginning.
   */
  @Test
  public void unionListListType2() {
    try (ListVector listVector = ListVector.empty("list", allocator)) {
      listVector.allocateNew();
      UnionListWriter listWriter = new UnionListWriter(listVector);
      ListWriter innerListWriter = listWriter.list();

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
      checkUnionList(listVector);
    }
  }

  private void checkUnionList(ListVector listVector) {
    UnionListReader listReader = new UnionListReader(listVector);
    for (int i = 0; i < COUNT; i++) {
      listReader.setPosition(i);
      for (int j = 0; j < i % 7; j++) {
        listReader.next();
        FieldReader innerListReader = listReader.reader();
        for (int k = 0; k < i % 13; k++) {
          innerListReader.next();
          if (k % 2 == 0) {
            Assert.assertEquals("record: " + i, k, innerListReader.reader().readInteger().intValue());
          } else {
            Assert.assertEquals("record: " + i, k, innerListReader.reader().readLong().longValue());
          }
        }
      }
    }
  }

  @Test
  public void testListMapType() {
    try (ListVector listVector = ListVector.empty("list", allocator)) {
      listVector.allocateNew();
      UnionListWriter listWriter = new UnionListWriter(listVector);
      MapWriter innerMapWriter = listWriter.map(true);

      for (int i = 0; i < COUNT; i++) {
        listWriter.startList();
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
        listWriter.endList();
      }
      listWriter.setValueCount(COUNT);
      checkListMap(listVector);

      // Verify that the map vector has keysSorted = true
      MapVector mapVector = (MapVector) listVector.getDataVector();
      ArrowType arrowType = mapVector.getField().getFieldType().getType();
      assertTrue(((ArrowType.Map) arrowType).getKeysSorted());
    }
  }

  private void checkListMap(ListVector listVector) {
    UnionListReader listReader = new UnionListReader(listVector);
    for (int i = 0; i < COUNT; i++) {
      listReader.setPosition(i);
      for (int j = 0; j < i % 7; j++) {
        listReader.next();
        UnionMapReader mapReader = (UnionMapReader) listReader.reader();
        for (int k = 0; k < i % 13; k++) {
          mapReader.next();
          Assert.assertEquals("record key: " + i, k, mapReader.key().readInteger().intValue());
          if (k % 2 == 0) {
            Assert.assertEquals("record value: " + i, k, mapReader.value().readLong().longValue());
          } else {
            Assert.assertNull("record value: " + i, mapReader.value().readLong());
          }
        }
      }
    }
  }

  @Test
  public void simpleUnion() throws Exception {
    List<ArrowBuf> bufs = new ArrayList<ArrowBuf>();
    UnionVector vector = new UnionVector("union", allocator, /* field type */ null, /* call-back */ null);
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
        Assert.assertEquals(i, i, unionReader.readInteger());
      } else if (i % 5 == 1) {
        NullableTimeStampMilliTZHolder holder = new NullableTimeStampMilliTZHolder();
        unionReader.read(holder);
        Assert.assertEquals(i, holder.value);
        Assert.assertEquals("AsdfTimeZone", holder.timezone);
      } else if (i % 5 == 2) {
        NullableDurationHolder holder = new NullableDurationHolder();
        unionReader.read(holder);
        Assert.assertEquals(i, holder.value);
        Assert.assertEquals(TimeUnit.NANOSECOND, holder.unit);
      } else if (i % 5 == 3) {
        NullableFixedSizeBinaryHolder holder = new NullableFixedSizeBinaryHolder();
        unionReader.read(holder);
        assertEquals(i, holder.buffer.getInt(0));
        assertEquals(4, holder.byteWidth);
      } else {
        Assert.assertEquals((float) i, unionReader.readFloat(), 1e-12);
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
      Assert.assertEquals("a", field.getName());
      Assert.assertEquals(Int.TYPE_TYPE, field.getType().getTypeID());
      Int intType = (Int) field.getType();

      Assert.assertEquals(64, intType.getBitWidth());
      Assert.assertTrue(intType.getIsSigned());
      for (int i = 100; i < 200; i++) {
        VarCharWriter varCharWriter = rootWriter.varChar("a");
        varCharWriter.setPosition(i);
        byte[] bytes = Integer.toString(i).getBytes();
        ArrowBuf tempBuf = allocator.buffer(bytes.length);
        tempBuf.setBytes(0, bytes);
        varCharWriter.writeVarChar(0, bytes.length, tempBuf);
        tempBuf.close();
      }
      field = parent.getField().getChildren().get(0).getChildren().get(0);
      Assert.assertEquals("a", field.getName());
      Assert.assertEquals(Union.TYPE_TYPE, field.getType().getTypeID());
      Assert.assertEquals(Int.TYPE_TYPE, field.getChildren().get(0).getType().getTypeID());
      Assert.assertEquals(Utf8.TYPE_TYPE, field.getChildren().get(1).getType().getTypeID());
      StructReader rootReader = new SingleStructReaderImpl(parent).reader("root");
      for (int i = 0; i < 100; i++) {
        rootReader.setPosition(i);
        FieldReader reader = rootReader.reader("a");
        Long value = reader.readLong();
        Assert.assertNotNull("index: " + i, value);
        Assert.assertEquals(i, value.intValue());
      }
      for (int i = 100; i < 200; i++) {
        rootReader.setPosition(i);
        FieldReader reader = rootReader.reader("a");
        Text value = reader.readText();
        Assert.assertEquals(Integer.toString(i), value.toString());
      }
    }
  }

  /**
   * Even without writing to the writer, the union schema is created correctly.
   */
  @Test
  public void promotableWriterSchema() {
    try (NonNullableStructVector parent = NonNullableStructVector.empty("parent", allocator)) {
      ComplexWriter writer = new ComplexWriterImpl("root", parent);
      StructWriter rootWriter = writer.rootAsStruct();
      rootWriter.bigInt("a");
      rootWriter.varChar("a");

      Field field = parent.getField().getChildren().get(0).getChildren().get(0);
      Assert.assertEquals("a", field.getName());
      Assert.assertEquals(ArrowTypeID.Union, field.getType().getTypeID());

      Assert.assertEquals(ArrowTypeID.Int, field.getChildren().get(0).getType().getTypeID());
      Int intType = (Int) field.getChildren().get(0).getType();
      Assert.assertEquals(64, intType.getBitWidth());
      Assert.assertTrue(intType.getIsSigned());
      Assert.assertEquals(ArrowTypeID.Utf8, field.getChildren().get(1).getType().getTypeID());
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
      Assert.assertEquals(11, fieldNamesCaseSensitive.size());
      Assert.assertTrue(fieldNamesCaseSensitive.contains("int_field"));
      Assert.assertTrue(fieldNamesCaseSensitive.contains("Int_Field"));
      Assert.assertTrue(fieldNamesCaseSensitive.contains("float_field"));
      Assert.assertTrue(fieldNamesCaseSensitive.contains("Float_Field"));
      Assert.assertTrue(fieldNamesCaseSensitive.contains("struct_field"));
      Assert.assertTrue(fieldNamesCaseSensitive.contains("struct_field::char_field"));
      Assert.assertTrue(fieldNamesCaseSensitive.contains("struct_field::Char_Field"));
      Assert.assertTrue(fieldNamesCaseSensitive.contains("list_field"));
      Assert.assertTrue(fieldNamesCaseSensitive.contains("list_field::$data$"));
      Assert.assertTrue(fieldNamesCaseSensitive.contains("list_field::$data$::bit_field"));
      Assert.assertTrue(fieldNamesCaseSensitive.contains("list_field::$data$::Bit_Field"));

      // test case-insensitive StructWriter
      ComplexWriter writerCaseInsensitive = new ComplexWriterImpl("rootCaseInsensitive", parent, false, false);
      StructWriter rootWriterCaseInsensitive = writerCaseInsensitive.rootAsStruct();

      rootWriterCaseInsensitive.bigInt("int_field");
      rootWriterCaseInsensitive.bigInt("Int_Field");
      rootWriterCaseInsensitive.float4("float_field");
      rootWriterCaseInsensitive.float4("Float_Field");
      StructWriter structFieldWriterCaseInsensitive = rootWriterCaseInsensitive.struct("struct_field");
      structFieldWriterCaseInsensitive.varChar("char_field");
      structFieldWriterCaseInsensitive.varChar("Char_Field");
      ListWriter listFieldWriterCaseInsensitive = rootWriterCaseInsensitive.list("list_field");
      StructWriter listStructFieldWriterCaseInsensitive = listFieldWriterCaseInsensitive.struct();
      listStructFieldWriterCaseInsensitive.bit("bit_field");
      listStructFieldWriterCaseInsensitive.bit("Bit_Field");

      List<Field> fieldsCaseInsensitive = parent.getField().getChildren().get(1).getChildren();
      Set<String> fieldNamesCaseInsensitive = getFieldNames(fieldsCaseInsensitive);
      Assert.assertEquals(7, fieldNamesCaseInsensitive.size());
      Assert.assertTrue(fieldNamesCaseInsensitive.contains("int_field"));
      Assert.assertTrue(fieldNamesCaseInsensitive.contains("float_field"));
      Assert.assertTrue(fieldNamesCaseInsensitive.contains("struct_field"));
      Assert.assertTrue(fieldNamesCaseInsensitive.contains("struct_field::char_field"));
      Assert.assertTrue(fieldNamesCaseSensitive.contains("list_field"));
      Assert.assertTrue(fieldNamesCaseSensitive.contains("list_field::$data$"));
      Assert.assertTrue(fieldNamesCaseSensitive.contains("list_field::$data$::bit_field"));
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
        Assert.assertEquals(expectedSecDateTime, secDateTime);
        long secLong = secReader.readLong();
        Assert.assertEquals(expectedSecs, secLong);
      }
      {
        FieldReader secTZReader = rootReader.reader("secTZ");
        secTZReader.setPosition(1);
        long secTZLong = secTZReader.readLong();
        Assert.assertEquals(expectedSecs, secTZLong);
      }
    }
  }

  @Test
  public void timeStampMilliWriters() throws Exception {
    // test values
    final long expectedMillis = 981173106123L;
    final LocalDateTime expectedMilliDateTime = LocalDateTime.of(2001, 2, 3, 4, 5, 6, 123 * 1_000_000);

    try (NonNullableStructVector parent = NonNullableStructVector.empty("parent", allocator);) {
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
        Assert.assertEquals(expectedMilliDateTime, milliDateTime);
        long milliLong = milliReader.readLong();
        Assert.assertEquals(expectedMillis, milliLong);
      }
      {
        FieldReader milliTZReader = rootReader.reader("milliTZ");
        milliTZReader.setPosition(0);
        long milliTZLong = milliTZReader.readLong();
        Assert.assertEquals(expectedMillis, milliTZLong);
      }
    }
  }

  private void checkTimestampField(Field field, String name) {
    Assert.assertEquals(name, field.getName());
    Assert.assertEquals(ArrowType.Timestamp.TYPE_TYPE, field.getType().getTypeID());
  }

  private void checkTimestampTZField(Field field, String name, String tz) {
    checkTimestampField(field, name);
    Assert.assertEquals(tz, ((Timestamp) field.getType()).getTimezone());
  }

  @Test
  public void timeStampMicroWriters() throws Exception {
    // test values
    final long expectedMicros = 981173106123456L;
    final LocalDateTime expectedMicroDateTime = LocalDateTime.of(2001, 2, 3, 4, 5, 6, 123456 * 1000);

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
        Assert.assertEquals(expectedMicroDateTime, microDateTime);
        long microLong = microReader.readLong();
        Assert.assertEquals(expectedMicros, microLong);
      }
      {
        FieldReader microReader = rootReader.reader("microTZ");
        microReader.setPosition(1);
        long microLong = microReader.readLong();
        Assert.assertEquals(expectedMicros, microLong);
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
        Assert.assertEquals(expectedNanoDateTime, nanoDateTime);
        long nanoLong = nanoReader.readLong();
        Assert.assertEquals(expectedNanos, nanoLong);
      }
      {
        FieldReader nanoReader = rootReader.reader("nanoTZ");
        nanoReader.setPosition(0);
        long nanoLong = nanoReader.readLong();
        Assert.assertEquals(expectedNanos, nanoLong);
        NullableTimeStampNanoTZHolder h = new NullableTimeStampNanoTZHolder();
        nanoReader.read(h);
        Assert.assertEquals(expectedNanos, h.value);
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
      FixedSizeBinaryWriter fixedSizeBinaryWriter = rootWriter.fixedSizeBinary(fieldName, byteWidth);
      for (int i = 0; i < numValues; i++) {
        fixedSizeBinaryWriter.setPosition(i);
        fixedSizeBinaryWriter.writeFixedSizeBinary(bufs[i]);
      }

      // schema
      List<Field> children = parent.getField().getChildren().get(0).getChildren();
      Assert.assertEquals(fieldName, children.get(0).getName());
      Assert.assertEquals(ArrowType.FixedSizeBinary.TYPE_TYPE, children.get(0).getType().getTypeID());

      // read
      StructReader rootReader = new SingleStructReaderImpl(parent).reader("root");

      FieldReader fixedSizeBinaryReader = rootReader.reader(fieldName);
      for (int i = 0; i < numValues; i++) {
        fixedSizeBinaryReader.setPosition(i);
        byte[] readValues = fixedSizeBinaryReader.readByteArray();
        Assert.assertArrayEquals(values[i], readValues);
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
      UnionMapReader mapReader = (UnionMapReader) singleStructReader.reader("mapField");

      for (int i = 0; i < initialCapacity; i++) {
        intReader.setPosition(i);
        bigIntReader.setPosition(i);
        float4Reader.setPosition(i);
        float8Reader.setPosition(i);
        listReader.setPosition(i);
        mapReader.setPosition(i);

        assertEquals(intValue + i, intReader.readInteger().intValue());
        assertEquals(bigIntValue + (long) i, bigIntReader.readLong().longValue());
        assertEquals(float4Value + (float) i, float4Reader.readFloat().floatValue(), 0);
        assertEquals(float8Value + (double) i, float8Reader.readDouble().doubleValue(), 0);

        for (int j = 0; j < 4; j++) {
          listReader.next();
          assertEquals(intValue + i + j, listReader.reader().readInteger().intValue());
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
      for (int i = 0; i < COUNT; i++) {
        listWriter.setPosition(i);
        if (i % 2 == 0) {
          listWriter.startList();
          if (i % 4 == 0) {
            listWriter.integer().writeNull();
          } else {
            listWriter.integer().writeInt(i);
            listWriter.integer().writeInt(i * 2);
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
          Assert.assertTrue(listReader.isSet());
          listReader.next();
          if (i % 4 == 0) {
            Assert.assertNull(listReader.reader().readInteger());
          } else {
            Assert.assertEquals(i, listReader.reader().readInteger().intValue());
            listReader.next();
            Assert.assertEquals(i * 2, listReader.reader().readInteger().intValue());
          }
        } else {
          Assert.assertFalse(listReader.isSet());
        }
      }
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
          Assert.assertTrue(listReader.isSet());
          listReader.next();
          if (i % 4 == 0) {
            Assert.assertFalse(listReader.reader().isSet());
          } else {
            listReader.reader().next();
            Assert.assertFalse(listReader.reader().reader().isSet());
            listReader.reader().next();
            Assert.assertEquals(i, listReader.reader().reader().readInteger().intValue());
            listReader.reader().next();
            Assert.assertEquals(i * 2, listReader.reader().reader().readInteger().intValue());
          }
        } else {
          Assert.assertFalse(listReader.isSet());
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
          Assert.assertFalse(listReader.isSet());
        } else {
          Assert.assertTrue(listReader.isSet());
          listReader.next();
          if (i % 4 == 1) {
            Assert.assertFalse(listReader.reader().isSet());
          } else if (i % 4 == 2) {
            listReader.reader().next();
            Assert.assertFalse(listReader.reader().reader().isSet());
          } else {
            listReader.reader().next();
            listReader.reader().reader().next();
            Assert.assertFalse(listReader.reader().reader().reader().isSet());
            listReader.reader().reader().next();
            Assert.assertEquals(i, listReader.reader().reader().reader().readInteger().intValue());
            listReader.reader().reader().next();
            Assert.assertEquals(i * 2, listReader.reader().reader().reader().readInteger().intValue());
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
      Assert.assertNotNull(childListReader);
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
      Assert.assertEquals(1, size);
      int data = childListReader.reader().readInteger();
      Assert.assertEquals(10, data);
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
      Assert.assertEquals(1, size);
      int data = ((List<Integer>) childListReader.readObject()).get(0);
      Assert.assertEquals(3, data);
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
      Assert.assertEquals(0, size);
    }
  }
}
