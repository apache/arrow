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

package org.apache.arrow.vector.ipc;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

import com.google.common.collect.ImmutableList;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.impl.ComplexWriterImpl;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.complex.writer.BaseWriter.ComplexWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter.ListWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter.StructWriter;
import org.apache.arrow.vector.complex.writer.BigIntWriter;
import org.apache.arrow.vector.complex.writer.DateMilliWriter;
import org.apache.arrow.vector.complex.writer.Float4Writer;
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

import static org.apache.arrow.vector.TestUtils.newVarCharVector;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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

  protected void writeData(int count, StructVector parent) {
    ComplexWriter writer = new ComplexWriterImpl("root", parent);
    StructWriter rootWriter = writer.rootAsStruct();
    IntWriter intWriter = rootWriter.integer("int");
    BigIntWriter bigIntWriter = rootWriter.bigInt("bigInt");
    Float4Writer float4Writer = rootWriter.float4("float");
    for (int i = 0; i < count; i++) {
      intWriter.setPosition(i);
      intWriter.writeInt(i);
      bigIntWriter.setPosition(i);
      bigIntWriter.writeBigInt(i);
      float4Writer.setPosition(i);
      float4Writer.writeFloat4(i == 0 ? Float.NaN : i);
    }
    writer.setValueCount(count);
  }

  protected void validateContent(int count, VectorSchemaRoot root) {
    for (int i = 0; i < count; i++) {
      Assert.assertEquals(i, root.getVector("int").getObject(i));
      Assert.assertEquals(Long.valueOf(i), root.getVector("bigInt").getObject(i));
      Assert.assertEquals(i == 0 ? Float.NaN : i, root.getVector("float").getObject(i));
    }
  }

  protected void writeComplexData(int count, StructVector parent) {
    ArrowBuf varchar = allocator.buffer(3);
    varchar.readerIndex(0);
    varchar.setByte(0, 'a');
    varchar.setByte(1, 'b');
    varchar.setByte(2, 'c');
    varchar.writerIndex(3);
    ComplexWriter writer = new ComplexWriterImpl("root", parent);
    StructWriter rootWriter = writer.rootAsStruct();
    IntWriter intWriter = rootWriter.integer("int");
    BigIntWriter bigIntWriter = rootWriter.bigInt("bigInt");
    ListWriter listWriter = rootWriter.list("list");
    StructWriter structWriter = rootWriter.struct("struct");
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
      structWriter.setPosition(i);
      structWriter.start();
      structWriter.timeStampMilli("timestamp").writeTimeStampMilli(i);
      structWriter.end();
    }
    writer.setValueCount(count);
    varchar.release();
  }

  public void printVectors(List<FieldVector> vectors) {
    for (FieldVector vector : vectors) {
      LOGGER.debug(vector.getField().getName());
      int valueCount = vector.getValueCount();
      for (int i = 0; i < valueCount; i++) {
        LOGGER.debug(String.valueOf(vector.getObject(i)));
      }
    }
  }

  protected void validateComplexContent(int count, VectorSchemaRoot root) {
    Assert.assertEquals(count, root.getRowCount());
    printVectors(root.getFieldVectors());
    for (int i = 0; i < count; i++) {

      Object intVal = root.getVector("int").getObject(i);
      if (i % 5 != 3) {
        Assert.assertEquals(i, intVal);
      } else {
        Assert.assertNull(intVal);
      }
      Assert.assertEquals(Long.valueOf(i), root.getVector("bigInt").getObject(i));
      Assert.assertEquals(i % 3, ((List<?>) root.getVector("list").getObject(i)).size());
      NullableTimeStampMilliHolder h = new NullableTimeStampMilliHolder();
      FieldReader structReader = root.getVector("struct").getReader();
      structReader.setPosition(i);
      structReader.reader("timestamp").read(h);
      Assert.assertEquals(i, h.value);
    }
  }

  private LocalDateTime makeDateTimeFromCount(int i) {
    return new LocalDateTime(2000 + i, 1 + i, 1 + i, i, i, i, i);
  }

  protected void writeDateTimeData(int count, StructVector parent) {
    Assert.assertTrue(count < 100);
    ComplexWriter writer = new ComplexWriterImpl("root", parent);
    StructWriter rootWriter = writer.rootAsStruct();
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
      long dateVal = ((DateMilliVector) root.getVector("date")).get(i);
      LocalDateTime dt = makeDateTimeFromCount(i);
      LocalDateTime dateExpected = dt.minusMillis(dt.getMillisOfDay());
      Assert.assertEquals(DateUtility.toMillis(dateExpected), dateVal);
      long timeVal = ((TimeMilliVector) root.getVector("time")).get(i);
      Assert.assertEquals(dt.getMillisOfDay(), timeVal);
      Object timestampMilliVal = root.getVector("timestamp-milli").getObject(i);
      Assert.assertEquals(dt, timestampMilliVal);
      Object timestampMilliTZVal = root.getVector("timestamp-milliTZ").getObject(i);
      Assert.assertEquals(DateUtility.toMillis(dt), timestampMilliTZVal);
    }
  }

  protected VectorSchemaRoot writeFlatDictionaryData(BufferAllocator bufferAllocator, DictionaryProvider.MapDictionaryProvider provider) {

    // Define dictionaries and add to provider
    VarCharVector dictionary1Vector = newVarCharVector("D1", bufferAllocator);
    dictionary1Vector.allocateNewSafe();
    dictionary1Vector.set(0, "foo".getBytes(StandardCharsets.UTF_8));
    dictionary1Vector.set(1, "bar".getBytes(StandardCharsets.UTF_8));
    dictionary1Vector.set(2, "baz".getBytes(StandardCharsets.UTF_8));
    dictionary1Vector.setValueCount(3);

    Dictionary dictionary1 = new Dictionary(dictionary1Vector, new DictionaryEncoding(1L, false, null));
    provider.put(dictionary1);

    VarCharVector dictionary2Vector = newVarCharVector("D2", bufferAllocator);
    dictionary2Vector.allocateNewSafe();
    dictionary2Vector.set(0, "micro".getBytes(StandardCharsets.UTF_8));
    dictionary2Vector.set(1, "small".getBytes(StandardCharsets.UTF_8));
    dictionary2Vector.set(2, "large".getBytes(StandardCharsets.UTF_8));
    dictionary2Vector.setValueCount(3);

    Dictionary dictionary2 = new Dictionary(dictionary2Vector, new DictionaryEncoding(2L, false, null));
    provider.put(dictionary2);

    // Populate the vectors
    VarCharVector vector1A = newVarCharVector("varcharA", bufferAllocator);
    vector1A.allocateNewSafe();
    vector1A.set(0, "foo".getBytes(StandardCharsets.UTF_8));
    vector1A.set(1, "bar".getBytes(StandardCharsets.UTF_8));
    vector1A.set(3, "baz".getBytes(StandardCharsets.UTF_8));
    vector1A.set(4, "bar".getBytes(StandardCharsets.UTF_8));
    vector1A.set(5, "baz".getBytes(StandardCharsets.UTF_8));
    vector1A.setValueCount(6);

    FieldVector encodedVector1A = (FieldVector) DictionaryEncoder.encode(vector1A, dictionary1);
    vector1A.close();  // Done with this vector after encoding

    // Write this vector using indices instead of encoding
    IntVector encodedVector1B = new IntVector("varcharB", bufferAllocator);
    encodedVector1B.allocateNewSafe();
    encodedVector1B.set(0, 2);  // "baz"
    encodedVector1B.set(1, 1);  // "bar"
    encodedVector1B.set(2, 2);  // "baz"
    encodedVector1B.set(4, 1);  // "bar"
    encodedVector1B.set(5, 0);  // "foo"
    encodedVector1B.setValueCount(6);

    VarCharVector vector2 = newVarCharVector("sizes", bufferAllocator);
    vector2.allocateNewSafe();
    vector2.set(1, "large".getBytes(StandardCharsets.UTF_8));
    vector2.set(2, "small".getBytes(StandardCharsets.UTF_8));
    vector2.set(3, "small".getBytes(StandardCharsets.UTF_8));
    vector2.set(4, "large".getBytes(StandardCharsets.UTF_8));
    vector2.setValueCount(6);

    FieldVector encodedVector2 = (FieldVector) DictionaryEncoder.encode(vector2, dictionary2);
    vector2.close();  // Done with this vector after encoding

    List<Field> fields = ImmutableList.of(encodedVector1A.getField(), encodedVector1B.getField(), encodedVector2.getField());
    List<FieldVector> vectors = ImmutableList.of(encodedVector1A, encodedVector1B, encodedVector2);

    return new VectorSchemaRoot(fields, vectors, encodedVector1A.getValueCount());
  }

  protected void validateFlatDictionary(VectorSchemaRoot root, DictionaryProvider provider) {
    FieldVector vector1A = root.getVector("varcharA");
    Assert.assertNotNull(vector1A);

    DictionaryEncoding encoding1A = vector1A.getField().getDictionary();
    Assert.assertNotNull(encoding1A);
    Assert.assertEquals(1L, encoding1A.getId());

    Assert.assertEquals(6, vector1A.getValueCount());
    Assert.assertEquals(0, vector1A.getObject(0));
    Assert.assertEquals(1, vector1A.getObject(1));
    Assert.assertEquals(null, vector1A.getObject(2));
    Assert.assertEquals(2, vector1A.getObject(3));
    Assert.assertEquals(1, vector1A.getObject(4));
    Assert.assertEquals(2, vector1A.getObject(5));

    FieldVector vector1B = root.getVector("varcharB");
    Assert.assertNotNull(vector1B);

    DictionaryEncoding encoding1B = vector1A.getField().getDictionary();
    Assert.assertNotNull(encoding1B);
    Assert.assertTrue(encoding1A.equals(encoding1B));
    Assert.assertEquals(1L, encoding1B.getId());

    Assert.assertEquals(6, vector1B.getValueCount());
    Assert.assertEquals(2, vector1B.getObject(0));
    Assert.assertEquals(1, vector1B.getObject(1));
    Assert.assertEquals(2, vector1B.getObject(2));
    Assert.assertEquals(null, vector1B.getObject(3));
    Assert.assertEquals(1, vector1B.getObject(4));
    Assert.assertEquals(0, vector1B.getObject(5));

    FieldVector vector2 = root.getVector("sizes");
    Assert.assertNotNull(vector2);

    DictionaryEncoding encoding2 = vector2.getField().getDictionary();
    Assert.assertNotNull(encoding2);
    Assert.assertEquals(2L, encoding2.getId());

    Assert.assertEquals(6, vector2.getValueCount());
    Assert.assertEquals(null, vector2.getObject(0));
    Assert.assertEquals(2, vector2.getObject(1));
    Assert.assertEquals(1, vector2.getObject(2));
    Assert.assertEquals(1, vector2.getObject(3));
    Assert.assertEquals(2, vector2.getObject(4));
    Assert.assertEquals(null, vector2.getObject(5));

    Dictionary dictionary1 = provider.lookup(1L);
    Assert.assertNotNull(dictionary1);
    VarCharVector dictionaryVector = ((VarCharVector) dictionary1.getVector());
    Assert.assertEquals(3, dictionaryVector.getValueCount());
    Assert.assertEquals(new Text("foo"), dictionaryVector.getObject(0));
    Assert.assertEquals(new Text("bar"), dictionaryVector.getObject(1));
    Assert.assertEquals(new Text("baz"), dictionaryVector.getObject(2));

    Dictionary dictionary2 = provider.lookup(2L);
    Assert.assertNotNull(dictionary2);
    dictionaryVector = ((VarCharVector) dictionary2.getVector());
    Assert.assertEquals(3, dictionaryVector.getValueCount());
    Assert.assertEquals(new Text("micro"), dictionaryVector.getObject(0));
    Assert.assertEquals(new Text("small"), dictionaryVector.getObject(1));
    Assert.assertEquals(new Text("large"), dictionaryVector.getObject(2));
  }

  protected VectorSchemaRoot writeNestedDictionaryData(BufferAllocator bufferAllocator, DictionaryProvider.MapDictionaryProvider provider) {

    // Define the dictionary and add to the provider
    VarCharVector dictionaryVector = newVarCharVector("D2", bufferAllocator);
    dictionaryVector.allocateNewSafe();
    dictionaryVector.set(0, "foo".getBytes(StandardCharsets.UTF_8));
    dictionaryVector.set(1, "bar".getBytes(StandardCharsets.UTF_8));
    dictionaryVector.setValueCount(2);

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

    Assert.assertEquals(3, vector.getValueCount());
    Assert.assertEquals(Arrays.asList(0, 1), vector.getObject(0));
    Assert.assertEquals(Arrays.asList(0), vector.getObject(1));
    Assert.assertEquals(Arrays.asList(1), vector.getObject(2));

    Dictionary dictionary = provider.lookup(2L);
    Assert.assertNotNull(dictionary);
    VarCharVector dictionaryVector = ((VarCharVector) dictionary.getVector());
    Assert.assertEquals(2, dictionaryVector.getValueCount());
    Assert.assertEquals(new Text("foo"), dictionaryVector.getObject(0));
    Assert.assertEquals(new Text("bar"), dictionaryVector.getObject(1));
  }

  protected VectorSchemaRoot writeDecimalData(BufferAllocator bufferAllocator) {
    DecimalVector decimalVector1 = new DecimalVector("decimal1", bufferAllocator, 10, 3);
    DecimalVector decimalVector2 = new DecimalVector("decimal2", bufferAllocator, 4, 2);
    DecimalVector decimalVector3 = new DecimalVector("decimal3", bufferAllocator, 16, 8);

    int count = 10;
    decimalVector1.allocateNew(count);
    decimalVector2.allocateNew(count);
    decimalVector3.allocateNew(count);

    for (int i = 0; i < count; i++) {
      decimalVector1.setSafe(i, new BigDecimal(BigInteger.valueOf(i), 3));
      decimalVector2.setSafe(i, new BigDecimal(BigInteger.valueOf(i * (1 << 10)), 2));
      decimalVector3.setSafe(i, new BigDecimal(BigInteger.valueOf(i * 1111111111111111L), 8));
    }

    decimalVector1.setValueCount(count);
    decimalVector2.setValueCount(count);
    decimalVector3.setValueCount(count);

    List<Field> fields = ImmutableList.of(decimalVector1.getField(), decimalVector2.getField(), decimalVector3.getField());
    List<FieldVector> vectors = ImmutableList.<FieldVector>of(decimalVector1, decimalVector2, decimalVector3);
    return new VectorSchemaRoot(fields, vectors, count);
  }

  protected void validateDecimalData(VectorSchemaRoot root) {
    DecimalVector decimalVector1 = (DecimalVector) root.getVector("decimal1");
    DecimalVector decimalVector2 = (DecimalVector) root.getVector("decimal2");
    DecimalVector decimalVector3 = (DecimalVector) root.getVector("decimal3");
    int count = 10;
    Assert.assertEquals(count, root.getRowCount());

    for (int i = 0; i < count; i++) {
      // Verify decimal 1 vector
      BigDecimal readValue = decimalVector1.getObject(i);
      ArrowType.Decimal type = (ArrowType.Decimal) decimalVector1.getField().getType();
      BigDecimal genValue = new BigDecimal(BigInteger.valueOf(i), type.getScale());
      Assert.assertEquals(genValue, readValue);

      // Verify decimal 2 vector
      readValue = decimalVector2.getObject(i);
      type = (ArrowType.Decimal) decimalVector2.getField().getType();
      genValue = new BigDecimal(BigInteger.valueOf(i * (1 << 10)), type.getScale());
      Assert.assertEquals(genValue, readValue);

      // Verify decimal 3 vector
      readValue = decimalVector3.getObject(i);
      type = (ArrowType.Decimal) decimalVector3.getField().getType();
      genValue = new BigDecimal(BigInteger.valueOf(i * 1111111111111111L), type.getScale());
      Assert.assertEquals(genValue, readValue);
    }
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

  public void writeUnionData(int count, StructVector parent) {
    ArrowBuf varchar = allocator.buffer(3);
    varchar.readerIndex(0);
    varchar.setByte(0, 'a');
    varchar.setByte(1, 'b');
    varchar.setByte(2, 'c');
    varchar.writerIndex(3);
    ComplexWriter writer = new ComplexWriterImpl("root", parent);
    StructWriter rootWriter = writer.rootAsStruct();
    IntWriter intWriter = rootWriter.integer("union");
    BigIntWriter bigIntWriter = rootWriter.bigInt("union");
    ListWriter listWriter = rootWriter.list("union");
    StructWriter structWriter = rootWriter.struct("union");
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
          structWriter.setPosition(i);
          structWriter.start();
          structWriter.timeStampMilli("timestamp").writeTimeStampMilli(i);
          structWriter.end();
          break;
      }
    }
    writer.setValueCount(count);
    varchar.release();
  }

  protected void writeVarBinaryData(int count, StructVector parent) {
    Assert.assertTrue(count < 100);
    ComplexWriter writer = new ComplexWriterImpl("root", parent);
    StructWriter rootWriter = writer.rootAsStruct();
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
      Object obj = listVector.getObject(i);
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
    Assert.assertEquals(listVector.getLastSet(), count);

    // VarBinaryVector lastSet should be the index of last value
    VarBinaryVector binaryVector = (VarBinaryVector) listVector.getChildrenFromFields().get(0);
    Assert.assertEquals(binaryVector.getLastSet(), numVarBinaryValues - 1);
  }

  protected void writeBatchData(ArrowWriter writer, IntVector vector, VectorSchemaRoot root) throws IOException {
    writer.start();

    vector.setNull(0);
    vector.setSafe(1, 1);
    vector.setSafe(2, 2);
    vector.setNull(3);
    vector.setSafe(4, 1);
    vector.setValueCount(5);
    root.setRowCount(5);
    writer.writeBatch();

    vector.setNull(0);
    vector.setSafe(1, 1);
    vector.setSafe(2, 2);
    vector.setValueCount(3);
    root.setRowCount(3);
    writer.writeBatch();

    writer.end();
  }

  protected void validateBatchData(ArrowReader reader, IntVector vector) throws IOException {
    reader.loadNextBatch();

    assertEquals(vector.getValueCount(), 5);
    assertTrue(vector.isNull(0));
    assertEquals(vector.get(1), 1);
    assertEquals(vector.get(2), 2);
    assertTrue(vector.isNull(3));
    assertEquals(vector.get(4), 1);

    reader.loadNextBatch();

    assertEquals(vector.getValueCount(), 3);
    assertTrue(vector.isNull(0));
    assertEquals(vector.get(1), 1);
    assertEquals(vector.get(2), 2);
  }
}
