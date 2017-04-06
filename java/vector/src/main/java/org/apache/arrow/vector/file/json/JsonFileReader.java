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
import static org.apache.arrow.vector.schema.ArrowVectorType.OFFSET;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.BufferBacked;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.TimeSecVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.TimeStampNanoVector;
import org.apache.arrow.vector.TimeStampSecVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.UInt1Vector;
import org.apache.arrow.vector.UInt2Vector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.ValueVector.Mutator;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.NullableMapVector;
import org.apache.arrow.vector.schema.ArrowVectorType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.MappingJsonFactory;
import com.google.common.base.Objects;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;

public class JsonFileReader implements AutoCloseable {
  private final File inputFile;
  private final JsonParser parser;
  private final BufferAllocator allocator;
  private Schema schema;

  public JsonFileReader(File inputFile, BufferAllocator allocator) throws JsonParseException, IOException {
    super();
    this.inputFile = inputFile;
    this.allocator = allocator;
    MappingJsonFactory jsonFactory = new MappingJsonFactory();
    this.parser = jsonFactory.createParser(inputFile);
  }

  public Schema start() throws JsonParseException, IOException {
    readToken(START_OBJECT);
    {
      this.schema = readNextField("schema", Schema.class);
      nextFieldIs("batches");
      readToken(START_ARRAY);
      return schema;
    }
  }

  public void read(VectorSchemaRoot root) throws IOException {
    JsonToken t = parser.nextToken();
    if (t == START_OBJECT) {
      {
        int count = readNextField("count", Integer.class);
        root.setRowCount(count);
        nextFieldIs("columns");
        readToken(START_ARRAY);
        {
          for (Field field : schema.getFields()) {
            FieldVector vector = root.getVector(field.getName());
            readVector(field, vector);
          }
        }
        readToken(END_ARRAY);
      }
      readToken(END_OBJECT);
    } else if (t == END_ARRAY) {
      root.setRowCount(0);
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
            readVector(field, vector);
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

  private void readVector(Field field, FieldVector vector) throws JsonParseException, IOException {
    List<ArrowVectorType> vectorTypes = field.getTypeLayout().getVectorTypes();
    List<BufferBacked> fieldInnerVectors = vector.getFieldInnerVectors();
    if (vectorTypes.size() != fieldInnerVectors.size()) {
      throw new IllegalArgumentException("vector types and inner vectors are not the same size: " + vectorTypes.size() + " != " + fieldInnerVectors.size());
    }
    readToken(START_OBJECT);
    {
      String name = readNextField("name", String.class);
      if (!Objects.equal(field.getName(), name)) {
        throw new IllegalArgumentException("Expected field " + field.getName() + " but got " + name);
      }
      int count = readNextField("count", Integer.class);
      for (int v = 0; v < vectorTypes.size(); v++) {
        ArrowVectorType vectorType = vectorTypes.get(v);
        BufferBacked innerVector = fieldInnerVectors.get(v);
        nextFieldIs(vectorType.getName());
        readToken(START_ARRAY);
        ValueVector valueVector = (ValueVector)innerVector;
        valueVector.allocateNew();
        Mutator mutator = valueVector.getMutator();

        int innerVectorCount = vectorType.equals(OFFSET) ? count + 1 : count;
        for (int i = 0; i < innerVectorCount; i++) {
          parser.nextToken();
          setValueFromParser(valueVector, i);
        }
        mutator.setValueCount(innerVectorCount);
        readToken(END_ARRAY);
      }
      // if children
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
          readVector(childField, childVector);
        }
        readToken(END_ARRAY);
      }
      if (vector instanceof NullableMapVector) {
        ((NullableMapVector)vector).valueCount = count;
      }
    }
    readToken(END_OBJECT);
  }

  private byte[] decodeHexSafe(String hexString) throws IOException {
    try {
      return Hex.decodeHex(hexString.toCharArray());
    } catch (DecoderException e) {
      throw new IOException("Unable to decode hex string: " + hexString, e);
    }
  }

  private void setValueFromParser(ValueVector valueVector, int i) throws IOException {
    switch (valueVector.getMinorType()) {
    case BIT:
      ((BitVector)valueVector).getMutator().set(i, parser.readValueAs(Boolean.class) ? 1 : 0);
      break;
    case TINYINT:
      ((TinyIntVector)valueVector).getMutator().set(i, parser.readValueAs(Integer.class));
      break;
    case SMALLINT:
      ((SmallIntVector)valueVector).getMutator().set(i, parser.readValueAs(Integer.class));
      break;
    case INT:
      ((IntVector)valueVector).getMutator().set(i, parser.readValueAs(Integer.class));
      break;
    case BIGINT:
      ((BigIntVector)valueVector).getMutator().set(i, parser.readValueAs(Long.class));
      break;
    case UINT1:
      ((UInt1Vector)valueVector).getMutator().set(i, parser.readValueAs(Integer.class));
      break;
    case UINT2:
      ((UInt2Vector)valueVector).getMutator().set(i, parser.readValueAs(Integer.class));
      break;
    case UINT4:
      ((UInt4Vector)valueVector).getMutator().set(i, parser.readValueAs(Integer.class));
      break;
    case UINT8:
      ((UInt8Vector)valueVector).getMutator().set(i, parser.readValueAs(Long.class));
      break;
    case FLOAT4:
      ((Float4Vector)valueVector).getMutator().set(i, parser.readValueAs(Float.class));
      break;
    case FLOAT8:
      ((Float8Vector)valueVector).getMutator().set(i, parser.readValueAs(Double.class));
      break;
    case VARBINARY:
      ((VarBinaryVector)valueVector).getMutator().setSafe(i, decodeHexSafe(parser.readValueAs(String.class)));
      break;
    case VARCHAR:
      ((VarCharVector)valueVector).getMutator().setSafe(i, parser.readValueAs(String.class).getBytes(UTF_8));
      break;
    case DATEDAY:
      ((DateDayVector)valueVector).getMutator().set(i, parser.readValueAs(Integer.class));
      break;
    case DATEMILLI:
      ((DateMilliVector)valueVector).getMutator().set(i, parser.readValueAs(Long.class));
      break;
    case TIMESEC:
      ((TimeSecVector)valueVector).getMutator().set(i, parser.readValueAs(Integer.class));
      break;
    case TIMEMILLI:
      ((TimeMilliVector)valueVector).getMutator().set(i, parser.readValueAs(Integer.class));
      break;
    case TIMEMICRO:
      ((TimeMicroVector)valueVector).getMutator().set(i, parser.readValueAs(Long.class));
      break;
    case TIMENANO:
      ((TimeNanoVector)valueVector).getMutator().set(i, parser.readValueAs(Long.class));
      break;
    case TIMESTAMPSEC:
      ((TimeStampSecVector)valueVector).getMutator().set(i, parser.readValueAs(Long.class));
      break;
    case TIMESTAMPMILLI:
      ((TimeStampMilliVector)valueVector).getMutator().set(i, parser.readValueAs(Long.class));
      break;
    case TIMESTAMPMICRO:
      ((TimeStampMicroVector)valueVector).getMutator().set(i, parser.readValueAs(Long.class));
      break;
    case TIMESTAMPNANO:
      ((TimeStampNanoVector)valueVector).getMutator().set(i, parser.readValueAs(Long.class));
      break;
    default:
      throw new UnsupportedOperationException("minor type: " + valueVector.getMinorType());
    }
  }

  @Override
  public void close() throws IOException {
    parser.close();
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
