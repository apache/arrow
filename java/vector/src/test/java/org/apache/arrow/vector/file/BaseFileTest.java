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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

import com.google.common.collect.ImmutableList;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.NullableDateMilliVector;
import org.apache.arrow.vector.NullableDecimalVector;
import org.apache.arrow.vector.NullableIntVector;
import org.apache.arrow.vector.NullableTimeMilliVector;
import org.apache.arrow.vector.NullableVarBinaryVector;
import org.apache.arrow.vector.NullableVarCharVector;
import org.apache.arrow.vector.ValueVector.Accessor;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.NullableMapVector;
import org.apache.arrow.vector.complex.impl.ComplexWriterImpl;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.complex.writer.BaseWriter.ComplexWriter;
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
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
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
      Assert.assertEquals(i % 3, ((List<?>) root.getVector("list").getAccessor().getObject(i)).size());
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
      long dateVal = ((NullableDateMilliVector) root.getVector("date")).getAccessor().get(i);
      LocalDateTime dt = makeDateTimeFromCount(i);
      LocalDateTime dateExpected = dt.minusMillis(dt.getMillisOfDay());
      Assert.assertEquals(DateUtility.toMillis(dateExpected), dateVal);
      long timeVal = ((NullableTimeMilliVector) root.getVector("time")).getAccessor().get(i);
      Assert.assertEquals(dt.getMillisOfDay(), timeVal);
      Object timestampMilliVal = root.getVector("timestamp-milli").getAccessor().getObject(i);
      Assert.assertEquals(dt, timestampMilliVal);
      Object timestampMilliTZVal = root.getVector("timestamp-milliTZ").getAccessor().getObject(i);
      Assert.assertEquals(DateUtility.toMillis(dt), timestampMilliTZVal);
    }
  }

  protected VectorSchemaRoot writeFlatDictionaryData(BufferAllocator bufferAllocator, DictionaryProvider.MapDictionaryProvider provider) {

    // Define dictionaries and add to provider
    NullableVarCharVector dictionary1Vector = newNullableVarCharVector("D1", bufferAllocator);
    dictionary1Vector.allocateNewSafe();
    NullableVarCharVector.Mutator mutator = dictionary1Vector.getMutator();
    mutator.set(0, "foo".getBytes(StandardCharsets.UTF_8));
    mutator.set(1, "bar".getBytes(StandardCharsets.UTF_8));
    mutator.set(2, "baz".getBytes(StandardCharsets.UTF_8));
    mutator.setValueCount(3);

    Dictionary dictionary1 = new Dictionary(dictionary1Vector, new DictionaryEncoding(1L, false, null));
    provider.put(dictionary1);

    NullableVarCharVector dictionary2Vector = newNullableVarCharVector("D2", bufferAllocator);
    dictionary2Vector.allocateNewSafe();
    mutator = dictionary2Vector.getMutator();
    mutator.set(0, "micro".getBytes(StandardCharsets.UTF_8));
    mutator.set(1, "small".getBytes(StandardCharsets.UTF_8));
    mutator.set(2, "large".getBytes(StandardCharsets.UTF_8));
    mutator.setValueCount(3);

    Dictionary dictionary2 = new Dictionary(dictionary2Vector, new DictionaryEncoding(2L, false, null));
    provider.put(dictionary2);

    // Populate the vectors
    NullableVarCharVector vector1A = newNullableVarCharVector("varcharA", bufferAllocator);
    vector1A.allocateNewSafe();
    mutator = vector1A.getMutator();
    mutator.set(0, "foo".getBytes(StandardCharsets.UTF_8));
    mutator.set(1, "bar".getBytes(StandardCharsets.UTF_8));
    mutator.set(3, "baz".getBytes(StandardCharsets.UTF_8));
    mutator.set(4, "bar".getBytes(StandardCharsets.UTF_8));
    mutator.set(5, "baz".getBytes(StandardCharsets.UTF_8));
    mutator.setValueCount(6);

    FieldVector encodedVector1A = (FieldVector) DictionaryEncoder.encode(vector1A, dictionary1);
    vector1A.close();  // Done with this vector after encoding

    // Write this vector using indices instead of encoding
    NullableIntVector encodedVector1B = new NullableIntVector("varcharB", bufferAllocator);
    encodedVector1B.allocateNewSafe();
    NullableIntVector.Mutator mutator1B = encodedVector1B.getMutator();
    mutator1B.set(0, 2);  // "baz"
    mutator1B.set(1, 1);  // "bar"
    mutator1B.set(2, 2);  // "baz"
    mutator1B.set(4, 1);  // "bar"
    mutator1B.set(5, 0);  // "foo"
    mutator1B.setValueCount(6);

    NullableVarCharVector vector2 = newNullableVarCharVector("sizes", bufferAllocator);
    vector2.allocateNewSafe();
    mutator = vector2.getMutator();
    mutator.set(1, "large".getBytes(StandardCharsets.UTF_8));
    mutator.set(2, "small".getBytes(StandardCharsets.UTF_8));
    mutator.set(3, "small".getBytes(StandardCharsets.UTF_8));
    mutator.set(4, "large".getBytes(StandardCharsets.UTF_8));
    mutator.setValueCount(6);

    FieldVector encodedVector2 = (FieldVector) DictionaryEncoder.encode(vector2, dictionary2);
    vector2.close();  // Done with this vector after encoding

    List<Field> fields = ImmutableList.of(encodedVector1A.getField(), encodedVector1B.getField(), encodedVector2.getField());
    List<FieldVector> vectors = ImmutableList.of(encodedVector1A, encodedVector1B, encodedVector2);

    return new VectorSchemaRoot(fields, vectors, encodedVector1A.getAccessor().getValueCount());
  }

  protected void validateFlatDictionary(VectorSchemaRoot root, DictionaryProvider provider) {
    FieldVector vector1A = root.getVector("varcharA");
    Assert.assertNotNull(vector1A);

    DictionaryEncoding encoding1A = vector1A.getField().getDictionary();
    Assert.assertNotNull(encoding1A);
    Assert.assertEquals(1L, encoding1A.getId());

    FieldVector.Accessor accessor = vector1A.getAccessor();
    Assert.assertEquals(6, accessor.getValueCount());
    Assert.assertEquals(0, accessor.getObject(0));
    Assert.assertEquals(1, accessor.getObject(1));
    Assert.assertEquals(null, accessor.getObject(2));
    Assert.assertEquals(2, accessor.getObject(3));
    Assert.assertEquals(1, accessor.getObject(4));
    Assert.assertEquals(2, accessor.getObject(5));

    FieldVector vector1B = root.getVector("varcharB");
    Assert.assertNotNull(vector1B);

    DictionaryEncoding encoding1B = vector1A.getField().getDictionary();
    Assert.assertNotNull(encoding1B);
    Assert.assertTrue(encoding1A.equals(encoding1B));
    Assert.assertEquals(1L, encoding1B.getId());

    accessor = vector1B.getAccessor();
    Assert.assertEquals(6, accessor.getValueCount());
    Assert.assertEquals(2, accessor.getObject(0));
    Assert.assertEquals(1, accessor.getObject(1));
    Assert.assertEquals(2, accessor.getObject(2));
    Assert.assertEquals(null, accessor.getObject(3));
    Assert.assertEquals(1, accessor.getObject(4));
    Assert.assertEquals(0, accessor.getObject(5));

    FieldVector vector2 = root.getVector("sizes");
    Assert.assertNotNull(vector2);

    DictionaryEncoding encoding2 = vector2.getField().getDictionary();
    Assert.assertNotNull(encoding2);
    Assert.assertEquals(2L, encoding2.getId());

    accessor = vector2.getAccessor();
    Assert.assertEquals(6, accessor.getValueCount());
    Assert.assertEquals(null, accessor.getObject(0));
    Assert.assertEquals(2, accessor.getObject(1));
    Assert.assertEquals(1, accessor.getObject(2));
    Assert.assertEquals(1, accessor.getObject(3));
    Assert.assertEquals(2, accessor.getObject(4));
    Assert.assertEquals(null, accessor.getObject(5));

    Dictionary dictionary1 = provider.lookup(1L);
    Assert.assertNotNull(dictionary1);
    NullableVarCharVector.Accessor dictionaryAccessor = ((NullableVarCharVector) dictionary1.getVector()).getAccessor();
    Assert.assertEquals(3, dictionaryAccessor.getValueCount());
    Assert.assertEquals(new Text("foo"), dictionaryAccessor.getObject(0));
    Assert.assertEquals(new Text("bar"), dictionaryAccessor.getObject(1));
    Assert.assertEquals(new Text("baz"), dictionaryAccessor.getObject(2));

    Dictionary dictionary2 = provider.lookup(2L);
    Assert.assertNotNull(dictionary2);
    dictionaryAccessor = ((NullableVarCharVector) dictionary2.getVector()).getAccessor();
    Assert.assertEquals(3, dictionaryAccessor.getValueCount());
    Assert.assertEquals(new Text("micro"), dictionaryAccessor.getObject(0));
    Assert.assertEquals(new Text("small"), dictionaryAccessor.getObject(1));
    Assert.assertEquals(new Text("large"), dictionaryAccessor.getObject(2));
  }

  protected VectorSchemaRoot writeNestedDictionaryData(BufferAllocator bufferAllocator, DictionaryProvider.MapDictionaryProvider provider) {

    // Define the dictionary and add to the provider
    NullableVarCharVector dictionaryVector = newNullableVarCharVector("D2", bufferAllocator);
    dictionaryVector.allocateNewSafe();
    dictionaryVector.getMutator().set(0, "foo".getBytes(StandardCharsets.UTF_8));
    dictionaryVector.getMutator().set(1, "bar".getBytes(StandardCharsets.UTF_8));
    dictionaryVector.getMutator().setValueCount(2);

    Dictionary dictionary = new Dictionary(dictionaryVector, new DictionaryEncoding(2L, false, null));
    provider.put(dictionary);

    // Write the vector data using dictionary indices
    ListVector listVector = ListVector.empty("list", bufferAllocator);
    DictionaryEncoding encoding = dictionary.getEncoding();
    listVector.addOrGetVector(new FieldType(true, encoding.getIndexType(), encoding));
    listVector.allocateNew();
    UnionListWriter listWriter = new UnionListWriter(listVector);
    listWriter.startList();
    listWriter.writeInt(0);
    listWriter.writeInt(1);
    listWriter.endList();
    listWriter.startList();
    listWriter.writeInt(0);
    listWriter.endList();
    listWriter.startList();
    listWriter.writeInt(1);
    listWriter.endList();
    listWriter.setValueCount(3);

    List<Field> fields = ImmutableList.of(listVector.getField());
    List<FieldVector> vectors = ImmutableList.<FieldVector>of(listVector);
    return new VectorSchemaRoot(fields, vectors, 3);
  }

  protected void validateNestedDictionary(VectorSchemaRoot root, DictionaryProvider provider) {
    FieldVector vector = root.getFieldVectors().get(0);
    Assert.assertNotNull(vector);
    Assert.assertNull(vector.getField().getDictionary());
    Field nestedField = vector.getField().getChildren().get(0);

    DictionaryEncoding encoding = nestedField.getDictionary();
    Assert.assertNotNull(encoding);
    Assert.assertEquals(2L, encoding.getId());
    Assert.assertEquals(new ArrowType.Int(32, true), encoding.getIndexType());

    FieldVector.Accessor accessor = vector.getAccessor();
    Assert.assertEquals(3, accessor.getValueCount());
    Assert.assertEquals(Arrays.asList(0, 1), accessor.getObject(0));
    Assert.assertEquals(Arrays.asList(0), accessor.getObject(1));
    Assert.assertEquals(Arrays.asList(1), accessor.getObject(2));

    Dictionary dictionary = provider.lookup(2L);
    Assert.assertNotNull(dictionary);
    NullableVarCharVector.Accessor dictionaryAccessor = ((NullableVarCharVector) dictionary.getVector()).getAccessor();
    Assert.assertEquals(2, dictionaryAccessor.getValueCount());
    Assert.assertEquals(new Text("foo"), dictionaryAccessor.getObject(0));
    Assert.assertEquals(new Text("bar"), dictionaryAccessor.getObject(1));
  }

  protected VectorSchemaRoot writeDecimalData(BufferAllocator bufferAllocator) {
    NullableDecimalVector decimalVector1 = new NullableDecimalVector("decimal1", bufferAllocator, 10, 3);
    NullableDecimalVector decimalVector2 = new NullableDecimalVector("decimal2", bufferAllocator, 4, 2);
    NullableDecimalVector decimalVector3 = new NullableDecimalVector("decimal3", bufferAllocator, 16, 8);

    int count = 10;
    decimalVector1.allocateNew(count);
    decimalVector2.allocateNew(count);
    decimalVector3.allocateNew(count);

    for (int i = 0; i < count; i++) {
      decimalVector1.getMutator().setSafe(i, new BigDecimal(BigInteger.valueOf(i), 3));
      decimalVector2.getMutator().setSafe(i, new BigDecimal(BigInteger.valueOf(i * (1 << 10)), 2));
      decimalVector3.getMutator().setSafe(i, new BigDecimal(BigInteger.valueOf(i * 1111111111111111L), 8));
    }

    decimalVector1.getMutator().setValueCount(count);
    decimalVector2.getMutator().setValueCount(count);
    decimalVector3.getMutator().setValueCount(count);

    List<Field> fields = ImmutableList.of(decimalVector1.getField(), decimalVector2.getField(), decimalVector3.getField());
    List<FieldVector> vectors = ImmutableList.<FieldVector>of(decimalVector1, decimalVector2, decimalVector3);
    return new VectorSchemaRoot(fields, vectors, count);
  }

  protected void validateDecimalData(VectorSchemaRoot root) {
    NullableDecimalVector decimalVector1 = (NullableDecimalVector) root.getVector("decimal1");
    NullableDecimalVector decimalVector2 = (NullableDecimalVector) root.getVector("decimal2");
    NullableDecimalVector decimalVector3 = (NullableDecimalVector) root.getVector("decimal3");
    int count = 10;
    Assert.assertEquals(count, root.getRowCount());

    for (int i = 0; i < count; i++) {
      // Verify decimal 1 vector
      BigDecimal readValue = decimalVector1.getAccessor().getObject(i);
      ArrowType.Decimal type = (ArrowType.Decimal) decimalVector1.getField().getType();
      BigDecimal genValue = new BigDecimal(BigInteger.valueOf(i), type.getScale());
      Assert.assertEquals(genValue, readValue);

      // Verify decimal 2 vector
      readValue = decimalVector2.getAccessor().getObject(i);
      type = (ArrowType.Decimal) decimalVector2.getField().getType();
      genValue = new BigDecimal(BigInteger.valueOf(i * (1 << 10)), type.getScale());
      Assert.assertEquals(genValue, readValue);

      // Verify decimal 3 vector
      readValue = decimalVector3.getAccessor().getObject(i);
      type = (ArrowType.Decimal) decimalVector3.getField().getType();
      genValue = new BigDecimal(BigInteger.valueOf(i * 1111111111111111L), type.getScale());
      Assert.assertEquals(genValue, readValue);
    }
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

  protected void writeVarBinaryData(int count, NullableMapVector parent) {
    Assert.assertTrue(count < 100);
    ComplexWriter writer = new ComplexWriterImpl("root", parent);
    MapWriter rootWriter = writer.rootAsMap();
    ListWriter listWriter = rootWriter.list("list");
    ArrowBuf varbin = allocator.buffer(count);
    for (int i = 0; i < count; i++) {
      varbin.setByte(i, i);
      listWriter.setPosition(i);
      listWriter.startList();
      for (int j = 0; j < i % 3; j++) {
        listWriter.varBinary().writeVarBinary(0, i + 1, varbin);
      }
      listWriter.endList();
    }
    writer.setValueCount(count);
    varbin.release();
  }

  protected void validateVarBinary(int count, VectorSchemaRoot root) {
    Assert.assertEquals(count, root.getRowCount());
    ListVector listVector = (ListVector) root.getVector("list");
    byte[] expectedArray = new byte[count];
    int numVarBinaryValues = 0;
    for (int i = 0; i < count; i++) {
      expectedArray[i] = (byte) i;
      Object obj = listVector.getAccessor().getObject(i);
      List<?> objList = (List) obj;
      if (i % 3 == 0) {
        Assert.assertTrue(objList.isEmpty());
      } else {
        byte[] expected = Arrays.copyOfRange(expectedArray, 0, i + 1);
        for (int j = 0; j < i % 3; j++) {
          byte[] result = (byte[]) objList.get(j);
          Assert.assertArrayEquals(result, expected);
          numVarBinaryValues++;
        }
      }
    }

    // ListVector lastSet should be the index of last value + 1
    Assert.assertEquals(listVector.getMutator().getLastSet(), count);

    // NullableVarBinaryVector lastSet should be the index of last value
    NullableVarBinaryVector binaryVector = (NullableVarBinaryVector) listVector.getChildrenFromFields().get(0);
    Assert.assertEquals(binaryVector.getMutator().getLastSet(), numVarBinaryValues - 1);
  }
}
