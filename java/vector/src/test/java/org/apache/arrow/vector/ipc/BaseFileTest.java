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

package org.apache.arrow.vector.ipc;

import static org.apache.arrow.vector.TestUtils.newVarCharVector;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.Collections2;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.UInt1Vector;
import org.apache.arrow.vector.UInt2Vector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.impl.ComplexWriterImpl;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.complex.impl.UnionMapReader;
import org.apache.arrow.vector.complex.impl.UnionMapWriter;
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
import org.apache.arrow.vector.complex.writer.TimeStampNanoWriter;
import org.apache.arrow.vector.complex.writer.UInt1Writer;
import org.apache.arrow.vector.complex.writer.UInt2Writer;
import org.apache.arrow.vector.complex.writer.UInt4Writer;
import org.apache.arrow.vector.complex.writer.UInt8Writer;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryEncoder;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.holders.NullableTimeStampMilliHolder;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.JsonStringArrayList;
import org.apache.arrow.vector.util.Text;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ArrowBuf;

/**
 * Helps testing the file formats.
 */
public class BaseFileTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(BaseFileTest.class);
  protected static final int COUNT = 10;
  protected BufferAllocator allocator;

  @Before
  public void init() {
    allocator = new RootAllocator(Integer.MAX_VALUE);
  }

  @After
  public void tearDown() {
    allocator.close();
  }


  private static short [] uint1Values = new short[]{0, 255, 1, 128, 2};
  private static char [] uint2Values = new char[]{0, Character.MAX_VALUE, 1, Short.MAX_VALUE * 2, 2};
  private static long [] uint4Values = new long[]{0, Integer.MAX_VALUE + 1L, 1, Integer.MAX_VALUE * 2L, 2};
  private static BigInteger[] uint8Values = new BigInteger[]{BigInteger.valueOf(0),
      BigInteger.valueOf(Long.MAX_VALUE).multiply(BigInteger.valueOf(2)), BigInteger.valueOf(2),
      BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.valueOf(1)), BigInteger.valueOf(2)};

  protected void writeData(int count, StructVector parent) {
    ComplexWriter writer = new ComplexWriterImpl("root", parent);
    StructWriter rootWriter = writer.rootAsStruct();
    IntWriter intWriter = rootWriter.integer("int");
    UInt1Writer uint1Writer = rootWriter.uInt1("uint1");
    UInt2Writer uint2Writer = rootWriter.uInt2("uint2");
    UInt4Writer uint4Writer = rootWriter.uInt4("uint4");
    UInt8Writer uint8Writer = rootWriter.uInt8("uint8");
    BigIntWriter bigIntWriter = rootWriter.bigInt("bigInt");
    Float4Writer float4Writer = rootWriter.float4("float");
    for (int i = 0; i < count; i++) {
      intWriter.setPosition(i);
      intWriter.writeInt(i);
      uint1Writer.setPosition(i);
      // TODO: Fix add safe write methods on uint methods.
      uint1Writer.setPosition(i);
      uint1Writer.writeUInt1((byte)uint1Values[i % uint1Values.length] );
      uint2Writer.setPosition(i);
      uint2Writer.writeUInt2((char)uint2Values[i % uint2Values.length] );
      uint4Writer.setPosition(i);
      uint4Writer.writeUInt4((int)uint4Values[i % uint4Values.length] );
      uint8Writer.setPosition(i);
      uint8Writer.writeUInt8(uint8Values[i % uint8Values.length].longValue());
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
      Assert.assertEquals((Short)uint1Values[i % uint1Values.length],
          ((UInt1Vector)root.getVector("uint1")).getObjectNoOverflow(i));
      Assert.assertEquals("Failed for index: " + i, (Character)uint2Values[i % uint2Values.length],
          (Character)((UInt2Vector)root.getVector("uint2")).get(i));
      Assert.assertEquals("Failed for index: " + i, (Long)uint4Values[i % uint4Values.length],
          ((UInt4Vector)root.getVector("uint4")).getObjectNoOverflow(i));
      Assert.assertEquals("Failed for index: " + i, uint8Values[i % uint8Values.length],
          ((UInt8Vector)root.getVector("uint8")).getObjectNoOverflow(i));
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
    varchar.getReferenceManager().release();
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
    return LocalDateTime.of(2000 + i, 1 + i, 1 + i, i, i, i, i * 100_000_000 + i);
  }

  protected void writeDateTimeData(int count, StructVector parent) {
    Assert.assertTrue(count < 100);
    ComplexWriter writer = new ComplexWriterImpl("root", parent);
    StructWriter rootWriter = writer.rootAsStruct();
    DateMilliWriter dateWriter = rootWriter.dateMilli("date");
    TimeMilliWriter timeWriter = rootWriter.timeMilli("time");
    TimeStampMilliWriter timeStampMilliWriter = rootWriter.timeStampMilli("timestamp-milli");
    TimeStampMilliTZWriter timeStampMilliTZWriter = rootWriter.timeStampMilliTZ("timestamp-milliTZ", "Europe/Paris");
    TimeStampNanoWriter timeStampNanoWriter = rootWriter.timeStampNano("timestamp-nano");
    for (int i = 0; i < count; i++) {
      LocalDateTime dt = makeDateTimeFromCount(i);
      // Number of days in milliseconds since epoch, stored as 64-bit integer, only date part is used
      dateWriter.setPosition(i);
      long dateLong = dt.toLocalDate().atStartOfDay().toInstant(ZoneOffset.UTC).toEpochMilli();
      dateWriter.writeDateMilli(dateLong);
      // Time is a value in milliseconds since midnight, stored as 32-bit integer
      timeWriter.setPosition(i);
      int milliOfDay = (int) java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(dt.toLocalTime().toNanoOfDay());
      timeWriter.writeTimeMilli(milliOfDay);
      // Timestamp as milliseconds since the epoch, stored as 64-bit integer
      timeStampMilliWriter.setPosition(i);
      timeStampMilliWriter.writeTimeStampMilli(dt.toInstant(ZoneOffset.UTC).toEpochMilli());
      // Timestamp as milliseconds since epoch with timezone
      timeStampMilliTZWriter.setPosition(i);
      timeStampMilliTZWriter.writeTimeStampMilliTZ(dt.atZone(ZoneId.of("Europe/Paris")).toInstant().toEpochMilli());
      // Timestamp as nanoseconds since epoch
      timeStampNanoWriter.setPosition(i);
      long tsNanos = dt.toInstant(ZoneOffset.UTC).toEpochMilli() * 1_000_000 + i;  // need to add back in nano val
      timeStampNanoWriter.writeTimeStampNano(tsNanos);
    }
    writer.setValueCount(count);
  }

  protected void validateDateTimeContent(int count, VectorSchemaRoot root) {
    Assert.assertEquals(count, root.getRowCount());
    printVectors(root.getFieldVectors());
    for (int i = 0; i < count; i++) {
      LocalDateTime dt = makeDateTimeFromCount(i);
      LocalDateTime dtMilli = dt.minusNanos(i);
      LocalDateTime dateVal = ((DateMilliVector) root.getVector("date")).getObject(i);
      LocalDateTime dateExpected = dt.toLocalDate().atStartOfDay();
      Assert.assertEquals(dateExpected, dateVal);
      LocalTime timeVal = ((TimeMilliVector) root.getVector("time")).getObject(i).toLocalTime();
      Assert.assertEquals(dtMilli.toLocalTime(), timeVal);
      Object timestampMilliVal = root.getVector("timestamp-milli").getObject(i);
      Assert.assertEquals(dtMilli, timestampMilliVal);
      Object timestampMilliTZVal = root.getVector("timestamp-milliTZ").getObject(i);
      Assert.assertEquals(dt.atZone(ZoneId.of("Europe/Paris")).toInstant().toEpochMilli(), timestampMilliTZVal);
      Object timestampNanoVal = root.getVector("timestamp-nano").getObject(i);
      Assert.assertEquals(dt, timestampNanoVal);
    }
  }

  protected VectorSchemaRoot writeFlatDictionaryData(
      BufferAllocator bufferAllocator,
      DictionaryProvider.MapDictionaryProvider provider) {

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

    List<Field> fields = Arrays.asList(encodedVector1A.getField(), encodedVector1B.getField(),
        encodedVector2.getField());
    List<FieldVector> vectors = Collections2.asImmutableList(encodedVector1A, encodedVector1B, encodedVector2);

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

  protected VectorSchemaRoot writeNestedDictionaryData(
      BufferAllocator bufferAllocator,
      DictionaryProvider.MapDictionaryProvider provider) {

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

    List<Field> fields = Collections2.asImmutableList(listVector.getField());
    List<FieldVector> vectors = Collections2.asImmutableList(listVector);
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

    List<Field> fields = Collections2.asImmutableList(decimalVector1.getField(), decimalVector2.getField(),
        decimalVector3.getField());
    List<FieldVector> vectors = Collections2.asImmutableList(decimalVector1, decimalVector2, decimalVector3);
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
        default:
          assert false : "Unexpected value in switch statement: " + i;
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
        default:
          assert false : "Unexpected value in switch statement: " + i;
      }
    }
    writer.setValueCount(count);
    varchar.getReferenceManager().release();
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
    varbin.getReferenceManager().release();
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
    Assert.assertEquals(listVector.getLastSet(), count - 1);

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

  protected VectorSchemaRoot writeMapData(BufferAllocator bufferAllocator) {
    MapVector mapVector = MapVector.empty("map", bufferAllocator, false);
    MapVector sortedMapVector = MapVector.empty("mapSorted", bufferAllocator, true);
    mapVector.allocateNew();
    sortedMapVector.allocateNew();
    UnionMapWriter mapWriter = mapVector.getWriter();
    UnionMapWriter sortedMapWriter = sortedMapVector.getWriter();

    final int count = 10;
    for (int i = 0; i < count; i++) {
      // Write mapVector with NULL values
      // i == 1 is a NULL
      if (i != 1) {
        mapWriter.setPosition(i);
        mapWriter.startMap();
        // i == 3 is an empty map
        if (i != 3) {
          for (int j = 0; j < i + 1; j++) {
            mapWriter.startEntry();
            mapWriter.key().bigInt().writeBigInt(j);
            // i == 5 maps to a NULL value
            if (i != 5) {
              mapWriter.value().integer().writeInt(j);
            }
            mapWriter.endEntry();
          }
        }
        mapWriter.endMap();
      }
      // Write sortedMapVector
      sortedMapWriter.setPosition(i);
      sortedMapWriter.startMap();
      for (int j = 0; j < i + 1; j++) {
        sortedMapWriter.startEntry();
        sortedMapWriter.key().bigInt().writeBigInt(j);
        sortedMapWriter.value().integer().writeInt(j);
        sortedMapWriter.endEntry();
      }
      sortedMapWriter.endMap();
    }
    mapWriter.setValueCount(COUNT);
    sortedMapWriter.setValueCount(COUNT);

    List<Field> fields = Collections2.asImmutableList(mapVector.getField(), sortedMapVector.getField());
    List<FieldVector> vectors = Collections2.asImmutableList(mapVector, sortedMapVector);
    return new VectorSchemaRoot(fields, vectors, count);
  }

  protected void validateMapData(VectorSchemaRoot root) {
    MapVector mapVector = (MapVector) root.getVector("map");
    MapVector sortedMapVector = (MapVector) root.getVector("mapSorted");

    final int count = 10;
    Assert.assertEquals(count, root.getRowCount());

    UnionMapReader mapReader = new UnionMapReader(mapVector);
    UnionMapReader sortedMapReader = new UnionMapReader(sortedMapVector);
    for (int i = 0; i < count; i++) {
      // Read mapVector with NULL values
      mapReader.setPosition(i);
      if (i == 1) {
        assertFalse(mapReader.isSet());
      } else {
        if (i == 3) {
          JsonStringArrayList<?> result = (JsonStringArrayList<?>) mapReader.readObject();
          assertTrue(result.isEmpty());
        } else {
          for (int j = 0; j < i + 1; j++) {
            mapReader.next();
            assertEquals(j, mapReader.key().readLong().longValue());
            if (i == 5) {
              assertFalse(mapReader.value().isSet());
            } else {
              assertEquals(j, mapReader.value().readInteger().intValue());
            }
          }
        }
      }
      // Read sortedMapVector
      sortedMapReader.setPosition(i);
      for (int j = 0; j < i + 1; j++) {
        sortedMapReader.next();
        assertEquals(j, sortedMapReader.key().readLong().longValue());
        assertEquals(j, sortedMapReader.value().readInteger().intValue());
      }
    }
  }

  protected VectorSchemaRoot writeListAsMapData(BufferAllocator bufferAllocator) {
    ListVector mapEntryList = ListVector.empty("entryList", bufferAllocator);
    FieldType mapEntryType = new FieldType(false, ArrowType.Struct.INSTANCE, null, null);
    StructVector mapEntryData = new StructVector("entryData", bufferAllocator, mapEntryType, null);
    mapEntryData.addOrGet("myKey", new FieldType(false, new ArrowType.Int(64, true), null), BigIntVector.class);
    mapEntryData.addOrGet("myValue", FieldType.nullable(new ArrowType.Int(32, true)), IntVector.class);
    mapEntryList.initializeChildrenFromFields(Collections2.asImmutableList(mapEntryData.getField()));
    UnionListWriter entryWriter = mapEntryList.getWriter();
    entryWriter.allocate();

    final int count = 10;
    for (int i = 0; i < count; i++) {
      entryWriter.setPosition(i);
      entryWriter.startList();
      for (int j = 0; j < i + 1; j++) {
        entryWriter.struct().start();
        entryWriter.struct().bigInt("myKey").writeBigInt(j);
        entryWriter.struct().integer("myValue").writeInt(j);
        entryWriter.struct().end();
      }
      entryWriter.endList();
    }
    entryWriter.setValueCount(COUNT);

    MapVector mapVector = MapVector.empty("map", bufferAllocator, false);
    mapEntryList.makeTransferPair(mapVector).transfer();

    List<Field> fields = Collections2.asImmutableList(mapVector.getField());
    List<FieldVector> vectors = Collections2.asImmutableList(mapVector);
    return new VectorSchemaRoot(fields, vectors, count);
  }

  protected void validateListAsMapData(VectorSchemaRoot root) {
    MapVector sortedMapVector = (MapVector) root.getVector("map");

    final int count = 10;
    Assert.assertEquals(count, root.getRowCount());

    UnionMapReader sortedMapReader = new UnionMapReader(sortedMapVector);
    sortedMapReader.setKeyValueNames("myKey", "myValue");
    for (int i = 0; i < count; i++) {
      sortedMapReader.setPosition(i);
      for (int j = 0; j < i + 1; j++) {
        sortedMapReader.next();
        assertEquals(j, sortedMapReader.key().readLong().longValue());
        assertEquals(j, sortedMapReader.value().readInteger().intValue());
      }
    }
  }
}
