/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.arrow.vector.file;

import java.nio.charset.StandardCharsets;
import java.util.List;

import com.google.common.collect.ImmutableList;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.NullableDateMilliVector;
import org.apache.arrow.vector.NullableTimeMilliVector;
import org.apache.arrow.vector.NullableVarCharVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.ValueVector.Accessor;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.NullableMapVector;
import org.apache.arrow.vector.complex.impl.ComplexWriterImpl;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.complex.writer.BaseWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter.ComplexWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter.DictionaryWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter.ListWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter.MapWriter;
import org.apache.arrow.vector.complex.writer.BigIntWriter;
import org.apache.arrow.vector.complex.writer.DateMilliWriter;
import org.apache.arrow.vector.complex.writer.IntWriter;
import org.apache.arrow.vector.complex.writer.TimeMilliWriter;
import org.apache.arrow.vector.complex.writer.TimeStampMilliTZWriter;
import org.apache.arrow.vector.complex.writer.TimeStampMilliWriter;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryEncoder;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.holders.NullableTimeStampMilliHolder;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.DateUtility;
import org.apache.arrow.vector.util.Text;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDateTime;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ArrowBuf;
import sun.awt.SunHints;

import static org.apache.arrow.vector.TestUtils.newNullableVarCharVector;

/**
 * Helps testing the file formats
 */
public class BaseFileTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(BaseFileTest.class);
  protected static final int COUNT = 10;
  protected BufferAllocator allocator;

  private DateTimeZone defaultTimezone = DateTimeZone.getDefault();

  @Before
  public void init() {
    DateTimeZone.setDefault(DateTimeZone.forOffsetHours(2));
    allocator = new RootAllocator(Integer.MAX_VALUE);
  }

  @After
  public void tearDown() {
    allocator.close();
    DateTimeZone.setDefault(defaultTimezone);
  }

  protected void validateContent(int count, VectorSchemaRoot root) {
    for (int i = 0; i < count; i++) {
      Assert.assertEquals(i, root.getVector("int").getAccessor().getObject(i));
      Assert.assertEquals(Long.valueOf(i), root.getVector("bigInt").getAccessor().getObject(i));
    }
  }

  protected void writeComplexData(int count, MapVector parent) {
    ArrowBuf varchar = allocator.buffer(3);
    varchar.readerIndex(0);
    varchar.setByte(0, 'a');
    varchar.setByte(1, 'b');
    varchar.setByte(2, 'c');
    varchar.writerIndex(3);
    ComplexWriter writer = new ComplexWriterImpl("root", parent);
    MapWriter rootWriter = writer.rootAsMap();
    IntWriter intWriter = rootWriter.integer("int");
    BigIntWriter bigIntWriter = rootWriter.bigInt("bigInt");
    ListWriter listWriter = rootWriter.list("list");
    MapWriter mapWriter = rootWriter.map("map");
    for (int i = 0; i < count; i++) {
      if (i % 5 != 3) {
        intWriter.setPosition(i);
        intWriter.writeInt(i);
      }
      bigIntWriter.setPosition(i);
      bigIntWriter.writeBigInt(i);
      listWriter.setPosition(i);
      listWriter.startList();
      for (int j = 0; j < i % 3; j++) {
        listWriter.varChar().writeVarChar(0, 3, varchar);
      }
      listWriter.endList();
      mapWriter.setPosition(i);
      mapWriter.start();
      mapWriter.timeStampMilli("timestamp").writeTimeStampMilli(i);
      mapWriter.end();
    }
    writer.setValueCount(count);
    varchar.release();
  }

  public void printVectors(List<FieldVector> vectors) {
    for (FieldVector vector : vectors) {
      LOGGER.debug(vector.getField().getName());
      Accessor accessor = vector.getAccessor();
      int valueCount = accessor.getValueCount();
      for (int i = 0; i < valueCount; i++) {
        LOGGER.debug(String.valueOf(accessor.getObject(i)));
      }
    }
  }

  protected void validateComplexContent(int count, VectorSchemaRoot root) {
    Assert.assertEquals(count, root.getRowCount());
    printVectors(root.getFieldVectors());
    for (int i = 0; i < count; i++) {
      Object intVal = root.getVector("int").getAccessor().getObject(i);
      if (i % 5 != 3) {
        Assert.assertEquals(i, intVal);
      } else {
        Assert.assertNull(intVal);
      }
      Assert.assertEquals(Long.valueOf(i), root.getVector("bigInt").getAccessor().getObject(i));
      Assert.assertEquals(i % 3, ((List<?>)root.getVector("list").getAccessor().getObject(i)).size());
      NullableTimeStampMilliHolder h = new NullableTimeStampMilliHolder();
      FieldReader mapReader = root.getVector("map").getReader();
      mapReader.setPosition(i);
      mapReader.reader("timestamp").read(h);
      Assert.assertEquals(i, h.value);
    }
  }

  private LocalDateTime makeDateTimeFromCount(int i) {
    return new LocalDateTime(2000 + i, 1 + i, 1 + i, i, i, i, i);
  }

  protected void writeDateTimeData(int count, NullableMapVector parent) {
    Assert.assertTrue(count < 100);
    ComplexWriter writer = new ComplexWriterImpl("root", parent);
    MapWriter rootWriter = writer.rootAsMap();
    DateMilliWriter dateWriter = rootWriter.dateMilli("date");
    TimeMilliWriter timeWriter = rootWriter.timeMilli("time");
    TimeStampMilliWriter timeStampMilliWriter = rootWriter.timeStampMilli("timestamp-milli");
    TimeStampMilliTZWriter timeStampMilliTZWriter = rootWriter.timeStampMilliTZ("timestamp-milliTZ", "Europe/Paris");
    for (int i = 0; i < count; i++) {
      LocalDateTime dt = makeDateTimeFromCount(i);
      // Number of days in milliseconds since epoch, stored as 64-bit integer, only date part is used
      dateWriter.setPosition(i);
      long dateLong = DateUtility.toMillis(dt.minusMillis(dt.getMillisOfDay()));
      dateWriter.writeDateMilli(dateLong);
      // Time is a value in milliseconds since midnight, stored as 32-bit integer
      timeWriter.setPosition(i);
      timeWriter.writeTimeMilli(dt.getMillisOfDay());
      // Timestamp is milliseconds since the epoch, stored as 64-bit integer
      timeStampMilliWriter.setPosition(i);
      timeStampMilliWriter.writeTimeStampMilli(DateUtility.toMillis(dt));
      timeStampMilliTZWriter.setPosition(i);
      timeStampMilliTZWriter.writeTimeStampMilliTZ(DateUtility.toMillis(dt));
    }
    writer.setValueCount(count);
  }

  protected void validateDateTimeContent(int count, VectorSchemaRoot root) {
    Assert.assertEquals(count, root.getRowCount());
    printVectors(root.getFieldVectors());
    for (int i = 0; i < count; i++) {
      long dateVal = ((NullableDateMilliVector)root.getVector("date")).getAccessor().get(i);
      LocalDateTime dt = makeDateTimeFromCount(i);
      LocalDateTime dateExpected = dt.minusMillis(dt.getMillisOfDay());
      Assert.assertEquals(DateUtility.toMillis(dateExpected), dateVal);
      long timeVal = ((NullableTimeMilliVector)root.getVector("time")).getAccessor().get(i);
      Assert.assertEquals(dt.getMillisOfDay(), timeVal);
      Object timestampMilliVal = root.getVector("timestamp-milli").getAccessor().getObject(i);
      Assert.assertEquals(dt, timestampMilliVal);
      Object timestampMilliTZVal = root.getVector("timestamp-milliTZ").getAccessor().getObject(i);
      Assert.assertEquals(DateUtility.toMillis(dt), timestampMilliTZVal);
    }
  }

  /*
  protected void writeFlatDictionaryData(NullableMapVector parent, DictionaryProvider.MapDictionaryProvider provider) {
    NullableVarCharVector vector = newNullableVarCharVector("varchar", vectorAllocator);
    NullableVarCharVector dictionaryVector = newNullableVarCharVector("dict", vectorAllocator);
    ComplexWriter writer = new ComplexWriterImpl("root", parent);
    MapWriter rootWriter = writer.rootAsMap();
    DictionaryWriter dictWriter = rootWriter.dict(provider);

    vector.allocateNewSafe();
    NullableVarCharVector.Mutator mutator = vector.getMutator();
    mutator.set(0, "foo".getBytes(StandardCharsets.UTF_8));
    mutator.set(1, "bar".getBytes(StandardCharsets.UTF_8));
    mutator.set(3, "baz".getBytes(StandardCharsets.UTF_8));
    mutator.set(4, "bar".getBytes(StandardCharsets.UTF_8));
    mutator.set(5, "baz".getBytes(StandardCharsets.UTF_8));
    mutator.setValueCount(6);

    dictionaryVector.allocateNewSafe();
    mutator = dictionaryVector.getMutator();
    mutator.set(0, "foo".getBytes(StandardCharsets.UTF_8));
    mutator.set(1, "bar".getBytes(StandardCharsets.UTF_8));
    mutator.set(2, "baz".getBytes(StandardCharsets.UTF_8));
    mutator.setValueCount(3);

    Dictionary dictionary = new Dictionary(dictionaryVector, new DictionaryEncoding(1L, false, null));
    provider.put(dictionary);

    FieldVector encodedVector = (FieldVector) DictionaryEncoder.encode(vector, dictionary);

    List<Field> fields = ImmutableList.of(encodedVector.getField());
    List<FieldVector> vectors = ImmutableList.of(encodedVector);
    VectorSchemaRoot root = new VectorSchemaRoot(fields, vectors, 6);

  }*/

  protected void validateFlatDictionary(FieldVector vector, DictionaryProvider provider) {
    Assert.assertNotNull(vector);

    DictionaryEncoding encoding = vector.getField().getDictionary();
    Assert.assertNotNull(encoding);
    Assert.assertEquals(1L, encoding.getId());

    FieldVector.Accessor accessor = vector.getAccessor();
    Assert.assertEquals(6, accessor.getValueCount());
    Assert.assertEquals(0, accessor.getObject(0));
    Assert.assertEquals(1, accessor.getObject(1));
    Assert.assertEquals(null, accessor.getObject(2));
    Assert.assertEquals(2, accessor.getObject(3));
    Assert.assertEquals(1, accessor.getObject(4));
    Assert.assertEquals(2, accessor.getObject(5));

    Dictionary dictionary = provider.lookup(1L);
    Assert.assertNotNull(dictionary);
    NullableVarCharVector.Accessor dictionaryAccessor = ((NullableVarCharVector) dictionary.getVector()).getAccessor();
    Assert.assertEquals(3, dictionaryAccessor.getValueCount());
    Assert.assertEquals(new Text("foo"), dictionaryAccessor.getObject(0));
    Assert.assertEquals(new Text("bar"), dictionaryAccessor.getObject(1));
    Assert.assertEquals(new Text("baz"), dictionaryAccessor.getObject(2));
  }

  protected void writeData(int count, MapVector parent) {
    ComplexWriter writer = new ComplexWriterImpl("root", parent);
    MapWriter rootWriter = writer.rootAsMap();
    IntWriter intWriter = rootWriter.integer("int");
    BigIntWriter bigIntWriter = rootWriter.bigInt("bigInt");
    for (int i = 0; i < count; i++) {
      intWriter.setPosition(i);
      intWriter.writeInt(i);
      bigIntWriter.setPosition(i);
      bigIntWriter.writeBigInt(i);
    }
    writer.setValueCount(count);
  }

  public void validateUnionData(int count, VectorSchemaRoot root) {
    FieldReader unionReader = root.getVector("union").getReader();
    for (int i = 0; i < count; i++) {
      unionReader.setPosition(i);
      switch (i % 4) {
      case 0:
        Assert.assertEquals(i, unionReader.readInteger().intValue());
        break;
      case 1:
        Assert.assertEquals(i, unionReader.readLong().longValue());
        break;
      case 2:
        Assert.assertEquals(i % 3, unionReader.size());
        break;
      case 3:
        NullableTimeStampMilliHolder h = new NullableTimeStampMilliHolder();
        unionReader.reader("timestamp").read(h);
        Assert.assertEquals(i, h.value);
        break;
      }
    }
  }

  public void writeUnionData(int count, NullableMapVector parent) {
    ArrowBuf varchar = allocator.buffer(3);
    varchar.readerIndex(0);
    varchar.setByte(0, 'a');
    varchar.setByte(1, 'b');
    varchar.setByte(2, 'c');
    varchar.writerIndex(3);
    ComplexWriter writer = new ComplexWriterImpl("root", parent);
    MapWriter rootWriter = writer.rootAsMap();
    IntWriter intWriter = rootWriter.integer("union");
    BigIntWriter bigIntWriter = rootWriter.bigInt("union");
    ListWriter listWriter = rootWriter.list("union");
    MapWriter mapWriter = rootWriter.map("union");
    for (int i = 0; i < count; i++) {
      switch (i % 4) {
      case 0:
        intWriter.setPosition(i);
        intWriter.writeInt(i);
        break;
      case 1:
        bigIntWriter.setPosition(i);
        bigIntWriter.writeBigInt(i);
        break;
      case 2:
        listWriter.setPosition(i);
        listWriter.startList();
        for (int j = 0; j < i % 3; j++) {
          listWriter.varChar().writeVarChar(0, 3, varchar);
        }
        listWriter.endList();
        break;
      case 3:
        mapWriter.setPosition(i);
        mapWriter.start();
        mapWriter.timeStampMilli("timestamp").writeTimeStampMilli(i);
        mapWriter.end();
        break;
      }
    }
    writer.setValueCount(count);
    varchar.release();
  }
}
