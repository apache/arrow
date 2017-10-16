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

import static org.apache.arrow.vector.schema.ArrowVectorType.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import io.netty.buffer.ArrowBuf;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.schema.ArrowVectorType;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter.NopIndenter;
import com.fasterxml.jackson.databind.MappingJsonFactory;
import org.apache.arrow.vector.util.DecimalUtility;
import org.apache.arrow.vector.util.DictionaryUtility;
import org.apache.commons.codec.binary.Hex;

public class JsonFileWriter implements AutoCloseable {

  public static final class JSONWriteConfig {
    private final boolean pretty;

    private JSONWriteConfig(boolean pretty) {
      this.pretty = pretty;
    }

    private JSONWriteConfig() {
      this.pretty = false;
    }

    public JSONWriteConfig pretty(boolean pretty) {
      return new JSONWriteConfig(pretty);
    }
  }

  public static JSONWriteConfig config() {
    return new JSONWriteConfig();
  }

  private final JsonGenerator generator;
  private Schema schema;

  public JsonFileWriter(File outputFile) throws IOException {
    this(outputFile, config());
  }

  public JsonFileWriter(File outputFile, JSONWriteConfig config) throws IOException {
    MappingJsonFactory jsonFactory = new MappingJsonFactory();
    this.generator = jsonFactory.createGenerator(outputFile, JsonEncoding.UTF8);
    if (config.pretty) {
      DefaultPrettyPrinter prettyPrinter = new DefaultPrettyPrinter();
      prettyPrinter.indentArraysWith(NopIndenter.instance);
      this.generator.setPrettyPrinter(prettyPrinter);
    }
  }

  public void start(Schema schema, DictionaryProvider provider) throws IOException {
    List<Field> fields = new ArrayList<>(schema.getFields().size());
    Set<Long> dictionaryIdsUsed = new HashSet<>();
    this.schema = schema;  // Store original Schema to ensure batches written match

    // Convert fields with dictionaries to have dictionary type
    for (Field field : schema.getFields()) {
      fields.add(DictionaryUtility.toMessageFormat(field, provider, dictionaryIdsUsed));
    }
    Schema updatedSchema = new Schema(fields, schema.getCustomMetadata());

    generator.writeStartObject();
    generator.writeObjectField("schema", updatedSchema);

    // Write all dictionaries that were used
    if (!dictionaryIdsUsed.isEmpty()) {
      writeDictionaryBatches(generator, dictionaryIdsUsed, provider);
    }

    // Start writing of record batches
    generator.writeArrayFieldStart("batches");
  }

  private void writeDictionaryBatches(JsonGenerator generator, Set<Long> dictionaryIdsUsed, DictionaryProvider provider) throws IOException {
    generator.writeArrayFieldStart("dictionaries");
    for (Long id : dictionaryIdsUsed) {
      generator.writeStartObject();
      generator.writeObjectField("id", id);

      generator.writeFieldName("data");
      Dictionary dictionary = provider.lookup(id);
      FieldVector vector = dictionary.getVector();
      List<Field> fields = ImmutableList.of(vector.getField());
      List<FieldVector> vectors = ImmutableList.of(vector);
      VectorSchemaRoot root = new VectorSchemaRoot(fields, vectors, vector.getValueCount());
      writeBatch(root);

      generator.writeEndObject();
    }
    generator.writeEndArray();
  }

  public void write(VectorSchemaRoot recordBatch) throws IOException {
    if (!recordBatch.getSchema().equals(schema)) {
      throw new IllegalArgumentException("record batches must have the same schema: " + schema);
    }
    writeBatch(recordBatch);
  }

  private void writeBatch(VectorSchemaRoot recordBatch) throws IOException {
    generator.writeStartObject();
    {
      generator.writeObjectField("count", recordBatch.getRowCount());
      generator.writeArrayFieldStart("columns");
      for (Field field : recordBatch.getSchema().getFields()) {
        FieldVector vector = recordBatch.getVector(field.getName());
        writeFromVectorIntoJson(field, vector);
      }
      generator.writeEndArray();
    }
    generator.writeEndObject();
  }

  private void writeFromVectorIntoJson(Field field, FieldVector vector) throws IOException {
    List<ArrowVectorType> vectorTypes = field.getTypeLayout().getVectorTypes();
    List<ArrowBuf> vectorBuffers = vector.getFieldBuffers();
    if (vectorTypes.size() != vectorBuffers.size()) {
      throw new IllegalArgumentException("vector types and inner vector buffers are not the same size: " + vectorTypes.size() + " != " + vectorBuffers.size());
    }
    generator.writeStartObject();
    {
      generator.writeObjectField("name", field.getName());
      int valueCount = vector.getValueCount();
      generator.writeObjectField("count", valueCount);
      for (int v = 0; v < vectorTypes.size(); v++) {
        ArrowVectorType vectorType = vectorTypes.get(v);
        ArrowBuf vectorBuffer = vectorBuffers.get(v);
        generator.writeArrayFieldStart(vectorType.getName());
        final int bufferValueCount = (vectorType.equals(OFFSET)) ? valueCount + 1 : valueCount;
        for (int i = 0; i < bufferValueCount; i++) {
          if (vectorType.equals(DATA) && (vector.getMinorType() == Types.MinorType.VARCHAR ||
                  vector.getMinorType() == Types.MinorType.VARBINARY)) {
            writeValueToGenerator(vectorType, vectorBuffer, vectorBuffers.get(v-1), vector, i);
          } else {
            writeValueToGenerator(vectorType, vectorBuffer, null, vector, i);
          }
        }
        generator.writeEndArray();
      }
      List<Field> fields = field.getChildren();
      List<FieldVector> children = vector.getChildrenFromFields();
      if (fields.size() != children.size()) {
        throw new IllegalArgumentException("fields and children are not the same size: " + fields.size() + " != " + children.size());
      }
      if (fields.size() > 0) {
        generator.writeArrayFieldStart("children");
        for (int i = 0; i < fields.size(); i++) {
          Field childField = fields.get(i);
          FieldVector childVector = children.get(i);
          writeFromVectorIntoJson(childField, childVector);
        }
        generator.writeEndArray();
      }
    }
    generator.writeEndObject();
  }

  private void writeValueToGenerator(ArrowVectorType bufferType, ArrowBuf buffer,
                                     ArrowBuf offsetBuffer, FieldVector vector, int index) throws IOException {
    if (bufferType.equals(TYPE)) {
      generator.writeNumber(buffer.getByte(index * NullableTinyIntVector.TYPE_WIDTH));
    } else if (bufferType.equals(OFFSET)) {
      generator.writeNumber(buffer.getInt(index * BaseNullableVariableWidthVector.OFFSET_WIDTH));
    } else if(bufferType.equals(VALIDITY)) {
      generator.writeNumber(vector.isNull(index) ? 0 : 1);
    } else if (bufferType.equals(DATA)) {
      switch (vector.getMinorType()) {
        case TINYINT:
          generator.writeNumber(NullableTinyIntVector.get(buffer, index));
          break;
        case SMALLINT:
          generator.writeNumber(NullableSmallIntVector.get(buffer, index));
          break;
        case INT:
          generator.writeNumber(NullableIntVector.get(buffer, index));
          break;
        case BIGINT:
          generator.writeNumber(NullableBigIntVector.get(buffer, index));
          break;
        case FLOAT4:
          generator.writeNumber(NullableFloat4Vector.get(buffer, index));
          break;
        case FLOAT8:
          generator.writeNumber(NullableFloat8Vector.get(buffer, index));
          break;
        case DATEDAY:
          generator.writeNumber(NullableDateDayVector.get(buffer, index));
          break;
        case DATEMILLI:
          generator.writeNumber(NullableDateMilliVector.get(buffer, index));
          break;
        case TIMESEC:
          generator.writeNumber(NullableTimeSecVector.get(buffer, index));
          break;
        case TIMEMILLI:
          generator.writeNumber(NullableTimeMilliVector.get(buffer, index));
          break;
        case TIMEMICRO:
          generator.writeNumber(NullableTimeMicroVector.get(buffer, index));
          break;
        case TIMENANO:
          generator.writeNumber(NullableTimeNanoVector.get(buffer, index));
          break;
        case TIMESTAMPSEC:
          generator.writeNumber(NullableTimeStampSecVector.get(buffer, index));
          break;
        case TIMESTAMPMILLI:
          generator.writeNumber(NullableTimeStampMilliVector.get(buffer, index));
          break;
        case TIMESTAMPMICRO:
          generator.writeNumber(NullableTimeStampMicroVector.get(buffer, index));
          break;
        case TIMESTAMPNANO:
          generator.writeNumber(NullableTimeStampNanoVector.get(buffer, index));
          break;
        case TIMESTAMPSECTZ:
          generator.writeNumber(NullableTimeStampSecTZVector.get(buffer, index));
          break;
        case TIMESTAMPMILLITZ:
          generator.writeNumber(NullableTimeStampMilliTZVector.get(buffer, index));
          break;
        case TIMESTAMPMICROTZ:
          generator.writeNumber(NullableTimeStampMicroTZVector.get(buffer, index));
          break;
        case TIMESTAMPNANOTZ:
          generator.writeNumber(NullableTimeStampNanoTZVector.get(buffer, index));
          break;
        case BIT:
          generator.writeNumber(BitVectorHelper.get(buffer, index));
          break;
        case VARBINARY: {
          assert offsetBuffer != null;
          String hexString = Hex.encodeHexString(BaseNullableVariableWidthVector.get(buffer,
                  offsetBuffer, index));
          generator.writeObject(hexString);
          break;
        }
        case VARCHAR: {
          assert offsetBuffer != null;
          byte[] b = (BaseNullableVariableWidthVector.get(buffer, offsetBuffer, index));
          generator.writeString(new String(b, "UTF-8"));
          break;
        }
        case DECIMAL: {
          String hexString = Hex.encodeHexString(DecimalUtility.getByteArrayFromArrowBuf(buffer,
                  index));
          generator.writeString(hexString);
          break;
        }
        default:
          throw new UnsupportedOperationException("minor type: " + vector.getMinorType());
      }
    }
  }

  @Override
  public void close() throws IOException {
    generator.writeEndArray();
    generator.writeEndObject();
    generator.close();
  }

}
