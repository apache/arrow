/*******************************************************************************
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
 ******************************************************************************/

package org.apache.arrow.vector.file.json;

import static com.fasterxml.jackson.core.JsonToken.END_ARRAY;
import static com.fasterxml.jackson.core.JsonToken.END_OBJECT;
import static com.fasterxml.jackson.core.JsonToken.START_ARRAY;
import static com.fasterxml.jackson.core.JsonToken.START_OBJECT;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.arrow.vector.schema.ArrowVectorType.*;

import java.io.File;
import java.io.IOException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.*;

import com.google.common.collect.ImmutableList;
import io.netty.buffer.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.schema.ArrowFieldNode;
import org.apache.arrow.vector.schema.ArrowVectorType;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.DictionaryUtility;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.MappingJsonFactory;
import com.google.common.base.Objects;

public class JsonFileReader implements AutoCloseable, DictionaryProvider {
  private final JsonParser parser;
  private final BufferAllocator allocator;
  private Schema schema;
  private Map<Long, Dictionary> dictionaries;
  private Boolean started = false;

  public JsonFileReader(File inputFile, BufferAllocator allocator) throws JsonParseException, IOException {
    super();
    this.allocator = allocator;
    MappingJsonFactory jsonFactory = new MappingJsonFactory();
    this.parser = jsonFactory.createParser(inputFile);
  }

  @Override
  public Dictionary lookup(long id) {
    if (!started) {
      throw new IllegalStateException("Unable to lookup until after read() has started");
    }

    return dictionaries.get(id);
  }

  public Schema start() throws JsonParseException, IOException {
    readToken(START_OBJECT);
    {
      Schema originalSchema = readNextField("schema", Schema.class);
      List<Field> fields = new ArrayList<>();
      dictionaries = new HashMap<>();

      // Convert fields with dictionaries to have the index type
      for (Field field : originalSchema.getFields()) {
        fields.add(DictionaryUtility.toMemoryFormat(field, allocator, dictionaries));
      }
      this.schema = new Schema(fields, originalSchema.getCustomMetadata());

      if (!dictionaries.isEmpty()) {
        nextFieldIs("dictionaries");
        readDictionaryBatches();
      }

      nextFieldIs("batches");
      readToken(START_ARRAY);
      started = true;
      return this.schema;
    }
  }

  private void readDictionaryBatches() throws JsonParseException, IOException {
    readToken(START_ARRAY);
    JsonToken token = parser.nextToken();
    boolean haveDictionaryBatch = token == START_OBJECT;
    while (haveDictionaryBatch) {

      // Lookup what dictionary for the batch about to be read
      long id = readNextField("id", Long.class);
      Dictionary dict = dictionaries.get(id);
      if (dict == null) {
        throw new IllegalArgumentException("Dictionary with id: " + id + " missing encoding from schema Field");
      }

      // Read the dictionary record batch
      nextFieldIs("data");
      FieldVector vector = dict.getVector();
      List<Field> fields = ImmutableList.of(vector.getField());
      List<FieldVector> vectors = ImmutableList.of(vector);
      VectorSchemaRoot root = new VectorSchemaRoot(fields, vectors, vector.getValueCount());
      read(root);

      readToken(END_OBJECT);
      token = parser.nextToken();
      haveDictionaryBatch = token == START_OBJECT;
    }

    if (token != END_ARRAY) {
      throw new IllegalArgumentException("Invalid token: " + token + " expected end of array at " + parser.getTokenLocation());
    }
  }

  public boolean read(VectorSchemaRoot root) throws IOException {
    JsonToken t = parser.nextToken();
    if (t == START_OBJECT) {
      {
        int count = readNextField("count", Integer.class);
        root.setRowCount(count);
        nextFieldIs("columns");
        readToken(START_ARRAY);
        {
          for (Field field : root.getSchema().getFields()) {
            FieldVector vector = root.getVector(field.getName());
            readFromJsonIntoVector(field, vector);
          }
        }
        readToken(END_ARRAY);
      }
      readToken(END_OBJECT);
      return true;
    } else if (t == END_ARRAY) {
      root.setRowCount(0);
      return false;
    } else {
      throw new IllegalArgumentException("Invalid token: " + t);
    }
  }

  public VectorSchemaRoot read() throws IOException {
    JsonToken t = parser.nextToken();
    if (t == START_OBJECT) {
      VectorSchemaRoot recordBatch = VectorSchemaRoot.create(schema, allocator);
      {
        int count = readNextField("count", Integer.class);
        recordBatch.setRowCount(count);
        nextFieldIs("columns");
        readToken(START_ARRAY);
        {
          for (Field field : schema.getFields()) {
            FieldVector vector = recordBatch.getVector(field.getName());
            readFromJsonIntoVector(field, vector);
          }
        }
        readToken(END_ARRAY);
      }
      readToken(END_OBJECT);
      return recordBatch;
    } else if (t == END_ARRAY) {
      return null;
    } else {
      throw new IllegalArgumentException("Invalid token: " + t);
    }
  }

  private void readFromJsonIntoVector(Field field, FieldVector vector) throws JsonParseException, IOException {
    List<ArrowVectorType> vectorTypes = field.getTypeLayout().getVectorTypes();
    ArrowBuf[] vectorBuffers = new ArrowBuf[vectorTypes.size()];
    /*
     * The order of inner buffers is :
     * Fixed width vector:
     *    -- validity buffer
     *    -- data buffer
     * Variable width vector:
     *    -- validity buffer
     *    -- offset buffer
     *    -- data buffer
     *
     * This is similar to what getFieldInnerVectors() used to give but now that we don't have
     * inner vectors anymore, we will work directly at the buffer level -- populate buffers
     * locally as we read from Json parser and do loadFieldBuffers on the vector followed by
     * releasing the local buffers.
     */
    readToken(START_OBJECT);
    {
      // If currently reading dictionaries, field name is not important so don't check
      String name = readNextField("name", String.class);
      if (started && !Objects.equal(field.getName(), name)) {
        throw new IllegalArgumentException("Expected field " + field.getName() + " but got " + name);
      }

      /* Initialize the vector with required capacity but don't allocate since we would
       * be doing loadFieldBuffers.
       */
      int valueCount = readNextField("count", Integer.class);
      vector.setInitialCapacity(valueCount);

      for (int v = 0; v < vectorTypes.size(); v++) {
        ArrowVectorType vectorType = vectorTypes.get(v);
        nextFieldIs(vectorType.getName());
        readToken(START_ARRAY);
        int innerBufferValueCount = valueCount;
        if (vectorType.equals(OFFSET)) {
          /* offset buffer has 1 additional value capacity */
          innerBufferValueCount = valueCount + 1;
        }
        for (int i = 0; i < innerBufferValueCount; i++) {
          /* write data to the buffer */
          parser.nextToken();
          /* for variable width vectors, value count doesn't help pre-determining the capacity of
           * the underlying data buffer. So we need to pass down the offset buffer (which was already
           * populated in the previous iteration of this loop).
           */
          if (vectorType.equals(DATA) && (vector.getMinorType() == Types.MinorType.VARCHAR
                  || vector.getMinorType() == Types.MinorType.VARBINARY)) {
            vectorBuffers[v] = setValueFromParser(vectorType, vector, vectorBuffers[v],
                    vectorBuffers[v-1], i, innerBufferValueCount);
          } else {
            vectorBuffers[v] = setValueFromParser(vectorType, vector, vectorBuffers[v],
                    null, i, innerBufferValueCount);
          }
        }
        readToken(END_ARRAY);
      }

      vector.loadFieldBuffers(new ArrowFieldNode(valueCount, 0), Arrays.asList(vectorBuffers));

      // read child vectors, if any
      List<Field> fields = field.getChildren();
      if (!fields.isEmpty()) {
        List<FieldVector> vectorChildren = vector.getChildrenFromFields();
        if (fields.size() != vectorChildren.size()) {
          throw new IllegalArgumentException("fields and children are not the same size: " + fields.size() + " != " + vectorChildren.size());
        }
        nextFieldIs("children");
        readToken(START_ARRAY);
        for (int i = 0; i < fields.size(); i++) {
          Field childField = fields.get(i);
          FieldVector childVector = vectorChildren.get(i);
          readFromJsonIntoVector(childField, childVector);
        }
        readToken(END_ARRAY);
      }
    }
    readToken(END_OBJECT);

    for (ArrowBuf buffer: vectorBuffers) {
      buffer.release();
    }
  }

  private byte[] decodeHexSafe(String hexString) throws IOException {
    try {
      return Hex.decodeHex(hexString.toCharArray());
    } catch (DecoderException e) {
      throw new IOException("Unable to decode hex string: " + hexString, e);
    }
  }

  private ArrowBuf setValueFromParser(ArrowVectorType bufferType, FieldVector vector,
                                      ArrowBuf buffer, ArrowBuf offsetBuffer, int index,
                                      int valueCount) throws IOException {
    if (bufferType.equals(TYPE)) {
      buffer = NullableTinyIntVector.set(buffer, allocator,
              valueCount, index, parser.readValueAs(Byte.class));
    } else if (bufferType.equals(OFFSET)) {
      buffer = BaseNullableVariableWidthVector.set(buffer, allocator,
              valueCount, index, parser.readValueAs(Integer.class));
    } else if (bufferType.equals(VALIDITY)) {
      buffer = BitVectorHelper.setValidityBit(buffer, allocator,
              valueCount, index, parser.readValueAs(Boolean.class) ? 1 : 0);
    } else if (bufferType.equals(DATA)) {
      switch (vector.getMinorType()) {
        case BIT:
          buffer = BitVectorHelper.setValidityBit(buffer, allocator,
                  valueCount, index, parser.readValueAs(Boolean.class) ? 1 : 0);
          break;
        case TINYINT:
          buffer = NullableTinyIntVector.set(buffer, allocator,
                  valueCount, index, parser.readValueAs(Byte.class));
          break;
        case SMALLINT:
          buffer = NullableSmallIntVector.set(buffer, allocator,
                  valueCount, index, parser.readValueAs(Short.class));
          break;
        case INT:
          buffer = NullableIntVector.set(buffer, allocator,
                  valueCount, index, parser.readValueAs(Integer.class));
          break;
        case BIGINT:
          buffer = NullableBigIntVector.set(buffer, allocator,
                  valueCount, index, parser.readValueAs(Long.class));
          break;
        case FLOAT4:
          buffer = NullableFloat4Vector.set(buffer, allocator,
                  valueCount, index, parser.readValueAs(Float.class));
          break;
        case FLOAT8:
          buffer = NullableFloat8Vector.set(buffer, allocator,
                  valueCount, index, parser.readValueAs(Double.class));
          break;
        case DECIMAL:
          buffer = NullableDecimalVector.set(buffer, allocator,
                  valueCount, index, decodeHexSafe(parser.readValueAs(String.class)));
          break;
        case VARBINARY:
          assert (offsetBuffer != null);
          buffer = BaseNullableVariableWidthVector.set(buffer, offsetBuffer, allocator, index,
                  decodeHexSafe(parser.readValueAs(String.class)), valueCount);
          break;
        case VARCHAR:
          assert (offsetBuffer != null);
          buffer = BaseNullableVariableWidthVector.set(buffer, offsetBuffer, allocator, index,
                  parser.readValueAs(String.class).getBytes(UTF_8), valueCount);
          break;
        case DATEDAY:
          buffer = NullableDateDayVector.set(buffer, allocator,
                  valueCount, index, parser.readValueAs(Integer.class));
          break;
        case DATEMILLI:
          buffer = NullableDateMilliVector.set(buffer, allocator,
                  valueCount, index, parser.readValueAs(Long.class));
          break;
        case TIMESEC:
          buffer = NullableTimeSecVector.set(buffer, allocator,
                  valueCount, index, parser.readValueAs(Integer.class));
          break;
        case TIMEMILLI:
          buffer = NullableTimeMilliVector.set(buffer, allocator,
                  valueCount, index, parser.readValueAs(Integer.class));
          break;
        case TIMEMICRO:
          buffer = NullableTimeMicroVector.set(buffer, allocator,
                  valueCount, index, parser.readValueAs(Long.class));
          break;
        case TIMENANO:
          buffer = NullableTimeNanoVector.set(buffer, allocator,
                  valueCount, index, parser.readValueAs(Long.class));
          break;
        case TIMESTAMPSEC:
          buffer = NullableTimeStampSecVector.set(buffer, allocator,
                  valueCount, index, parser.readValueAs(Long.class));
          break;
        case TIMESTAMPMILLI:
          buffer = NullableTimeStampMilliVector.set(buffer, allocator,
                  valueCount, index, parser.readValueAs(Long.class));
          break;
        case TIMESTAMPMICRO:
          buffer = NullableTimeStampMicroVector.set(buffer, allocator,
                  valueCount, index, parser.readValueAs(Long.class));
          break;
        case TIMESTAMPNANO:
          buffer = NullableTimeStampNanoVector.set(buffer, allocator,
                  valueCount, index, parser.readValueAs(Long.class));
          break;
        case TIMESTAMPSECTZ:
          buffer = NullableTimeStampSecTZVector.set(buffer, allocator,
                  valueCount, index, parser.readValueAs(Long.class));
          break;
        case TIMESTAMPMILLITZ:
          buffer = NullableTimeStampMilliTZVector.set(buffer, allocator,
                  valueCount, index, parser.readValueAs(Long.class));
          break;
        case TIMESTAMPMICROTZ:
          buffer = NullableTimeStampMicroTZVector.set(buffer, allocator,
                  valueCount, index, parser.readValueAs(Long.class));
          break;
        case TIMESTAMPNANOTZ:
          buffer = NullableTimeStampNanoTZVector.set(buffer, allocator,
                  valueCount, index, parser.readValueAs(Long.class));
          break;
        default:
          throw new UnsupportedOperationException("minor type: " + vector.getMinorType());
      }
    }

    return buffer;
  }

  @Override
  public void close() throws IOException {
    parser.close();
    for (Dictionary dictionary : dictionaries.values()) {
      dictionary.getVector().close();
    }
  }

  private <T> T readNextField(String expectedFieldName, Class<T> c) throws IOException, JsonParseException {
    nextFieldIs(expectedFieldName);
    parser.nextToken();
    return parser.readValueAs(c);
  }

  private void nextFieldIs(String expectedFieldName) throws IOException, JsonParseException {
    String name = parser.nextFieldName();
    if (name == null || !name.equals(expectedFieldName)) {
      throw new IllegalStateException("Expected " + expectedFieldName + " but got " + name);
    }
  }

  private void readToken(JsonToken expected) throws JsonParseException, IOException {
    JsonToken t = parser.nextToken();
    if (t != expected) {
      throw new IllegalStateException("Expected " + expected + " but got " + t);
    }
  }

}
