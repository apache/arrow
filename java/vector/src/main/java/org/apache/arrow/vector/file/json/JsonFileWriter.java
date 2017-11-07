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

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import io.netty.buffer.ArrowBuf;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.BufferBacked;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.TimeSecVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.TimeStampNanoVector;
import org.apache.arrow.vector.TimeStampSecVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.ValueVector.Accessor;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.schema.ArrowVectorType;
import org.apache.arrow.vector.types.pojo.ArrowType;
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
      VectorSchemaRoot root = new VectorSchemaRoot(fields, vectors, vector.getAccessor().getValueCount());
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
        writeVector(field, vector);
      }
      generator.writeEndArray();
    }
    generator.writeEndObject();
  }

  private void writeVector(Field field, FieldVector vector) throws IOException {
    List<ArrowVectorType> vectorTypes = field.getTypeLayout().getVectorTypes();
    List<BufferBacked> fieldInnerVectors = vector.getFieldInnerVectors();
    if (vectorTypes.size() != fieldInnerVectors.size()) {
      throw new IllegalArgumentException("vector types and inner vectors are not the same size: " + vectorTypes.size() + " != " + fieldInnerVectors.size());
    }
    generator.writeStartObject();
    {
      generator.writeObjectField("name", field.getName());
      int valueCount = vector.getAccessor().getValueCount();
      generator.writeObjectField("count", valueCount);
      for (int v = 0; v < vectorTypes.size(); v++) {
        ArrowVectorType vectorType = vectorTypes.get(v);
        BufferBacked innerVector = fieldInnerVectors.get(v);
        generator.writeArrayFieldStart(vectorType.getName());
        ValueVector valueVector = (ValueVector) innerVector;
        for (int i = 0; i < valueVector.getAccessor().getValueCount(); i++) {
          writeValueToGenerator(valueVector, i);
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
          writeVector(childField, childVector);
        }
        generator.writeEndArray();
      }
    }
    generator.writeEndObject();
  }

  private void writeValueToGenerator(ValueVector valueVector, int i) throws IOException {
    switch (valueVector.getMinorType()) {
      case DATEDAY:
        generator.writeNumber(((DateDayVector) valueVector).getAccessor().get(i));
        break;
      case DATEMILLI:
        generator.writeNumber(((DateMilliVector) valueVector).getAccessor().get(i));
        break;
      case TIMESEC:
        generator.writeNumber(((TimeSecVector) valueVector).getAccessor().get(i));
        break;
      case TIMEMILLI:
        generator.writeNumber(((TimeMilliVector) valueVector).getAccessor().get(i));
        break;
      case TIMEMICRO:
        generator.writeNumber(((TimeMicroVector) valueVector).getAccessor().get(i));
        break;
      case TIMENANO:
        generator.writeNumber(((TimeNanoVector) valueVector).getAccessor().get(i));
        break;
      case TIMESTAMPSEC:
        generator.writeNumber(((TimeStampSecVector) valueVector).getAccessor().get(i));
        break;
      case TIMESTAMPMILLI:
        generator.writeNumber(((TimeStampMilliVector) valueVector).getAccessor().get(i));
        break;
      case TIMESTAMPMICRO:
        generator.writeNumber(((TimeStampMicroVector) valueVector).getAccessor().get(i));
        break;
      case TIMESTAMPNANO:
        generator.writeNumber(((TimeStampNanoVector) valueVector).getAccessor().get(i));
        break;
      case BIT:
        generator.writeNumber(((BitVector) valueVector).getAccessor().get(i));
        break;
      case VARBINARY: {
          String hexString = Hex.encodeHexString(((VarBinaryVector) valueVector).getAccessor().get(i));
          generator.writeString(hexString);
        }
        break;
      case DECIMAL: {
          BigDecimal decimalValue = ((DecimalVector) valueVector).getAccessor().getObject(i);
          // We write the unscaled value, because the scale is stored in the type metadata.
          generator.writeString(decimalValue.unscaledValue().toString());
        }
        break;
      default:
        // TODO: each type
        Accessor accessor = valueVector.getAccessor();
        Object value = accessor.getObject(i);
        if (value instanceof Number || value instanceof Boolean) {
          generator.writeObject(value);
        } else {
          generator.writeObject(value.toString());
        }
        break;
    }
  }

  @Override
  public void close() throws IOException {
    generator.writeEndArray();
    generator.writeEndObject();
    generator.close();
  }

}
