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

import static org.apache.arrow.vector.BufferLayout.BufferType.*;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter.NopIndenter;
import com.fasterxml.jackson.databind.MappingJsonFactory;
import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.BaseLargeVariableWidthVector;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.BaseVariableWidthViewVector;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVectorHelper;
import org.apache.arrow.vector.BufferLayout.BufferType;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.Decimal256Vector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.DurationVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.IntervalDayVector;
import org.apache.arrow.vector.IntervalMonthDayNanoVector;
import org.apache.arrow.vector.IntervalYearVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.TimeSecVector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.TimeStampMilliTZVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.TimeStampNanoTZVector;
import org.apache.arrow.vector.TimeStampNanoVector;
import org.apache.arrow.vector.TimeStampSecTZVector;
import org.apache.arrow.vector.TimeStampSecVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.TypeLayout;
import org.apache.arrow.vector.UInt1Vector;
import org.apache.arrow.vector.UInt2Vector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.BaseRepeatedValueViewVector;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.DecimalUtility;
import org.apache.arrow.vector.util.DictionaryUtility;
import org.apache.commons.codec.binary.Hex;

/**
 * A writer that converts binary Vectors into an <em>internal, unstable</em> JSON format suitable
 * for integration testing.
 *
 * <p>This writer does NOT implement a JSON dataset format like JSONL.
 */
public class JsonFileWriter implements AutoCloseable {

  /** Configuration POJO for writing JSON files. */
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

  /** Constructs a new writer that will output to <code>outputFile</code>. */
  public JsonFileWriter(File outputFile) throws IOException {
    this(outputFile, config());
  }

  /** Constructs a new writer that will output to <code>outputFile</code> with the given options. */
  public JsonFileWriter(File outputFile, JSONWriteConfig config) throws IOException {
    MappingJsonFactory jsonFactory = new MappingJsonFactory();
    this.generator = jsonFactory.createGenerator(outputFile, JsonEncoding.UTF8);
    if (config.pretty) {
      DefaultPrettyPrinter prettyPrinter = new DefaultPrettyPrinter();
      prettyPrinter.indentArraysWith(NopIndenter.instance);
      this.generator.setPrettyPrinter(prettyPrinter);
    }
    // Allow writing of floating point NaN values not as strings
    this.generator.configure(JsonGenerator.Feature.QUOTE_NON_NUMERIC_NUMBERS, false);
  }

  /** Writes out the "header" of the file including the schema and any dictionaries required. */
  public void start(Schema schema, DictionaryProvider provider) throws IOException {
    List<Field> fields = new ArrayList<>(schema.getFields().size());
    Set<Long> dictionaryIdsUsed = new HashSet<>();
    this.schema = schema; // Store original Schema to ensure batches written match

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

  private void writeDictionaryBatches(
      JsonGenerator generator, Set<Long> dictionaryIdsUsed, DictionaryProvider provider)
      throws IOException {
    generator.writeArrayFieldStart("dictionaries");
    for (Long id : dictionaryIdsUsed) {
      generator.writeStartObject();
      generator.writeObjectField("id", id);

      generator.writeFieldName("data");
      Dictionary dictionary = provider.lookup(id);
      FieldVector vector = dictionary.getVector();
      List<Field> fields = Collections.singletonList(vector.getField());
      List<FieldVector> vectors = Collections.singletonList(vector);
      VectorSchemaRoot root = new VectorSchemaRoot(fields, vectors, vector.getValueCount());
      writeBatch(root);

      generator.writeEndObject();
    }
    generator.writeEndArray();
  }

  /** Writes the record batch to the JSON file. */
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
        FieldVector vector = recordBatch.getVector(field);
        writeFromVectorIntoJson(field, vector);
      }
      generator.writeEndArray();
    }
    generator.writeEndObject();
  }

  private void writeFromVectorIntoJson(Field field, FieldVector vector) throws IOException {
    TypeLayout typeLayout = TypeLayout.getTypeLayout(field.getType());
    List<BufferType> vectorTypes = typeLayout.getBufferTypes();
    List<ArrowBuf> vectorBuffers = vector.getFieldBuffers();

    if (typeLayout.isFixedBufferCount()) {
      if (vectorTypes.size() != vectorBuffers.size()) {
        throw new IllegalArgumentException(
            "vector types and inner vector buffers are not the same size: "
                + vectorTypes.size()
                + " != "
                + vectorBuffers.size());
      }
    } else {
      vectorTypes.add(VARIADIC_DATA_BUFFERS);
    }

    generator.writeStartObject();
    {
      generator.writeObjectField("name", field.getName());
      int valueCount = vector.getValueCount();
      generator.writeObjectField("count", valueCount);

      for (int v = 0; v < vectorTypes.size(); v++) {
        BufferType bufferType = vectorTypes.get(v);
        ArrowBuf vectorBuffer = vectorBuffers.get(v);
        // Note that in JSON format we cannot have VARIADIC_DATA_BUFFERS repeated,
        // thus the values are only written to a single entity.
        generator.writeArrayFieldStart(bufferType.getName());
        final int bufferValueCount =
            (bufferType.equals(OFFSET)
                    && vector.getMinorType() != MinorType.DENSEUNION
                    && vector.getMinorType() != MinorType.LISTVIEW)
                ? valueCount + 1
                : valueCount;
        for (int i = 0; i < bufferValueCount; i++) {
          if (bufferType.equals(DATA)
              && (vector.getMinorType() == MinorType.VARCHAR
                  || vector.getMinorType() == MinorType.VARBINARY)) {
            writeValueToGenerator(bufferType, vectorBuffer, vectorBuffers.get(v - 1), vector, i);
          } else if (bufferType.equals(VIEWS)
              && (vector.getMinorType() == MinorType.VIEWVARCHAR
                  || vector.getMinorType() == MinorType.VIEWVARBINARY)) {
            // writing views
            ArrowBuf viewBuffer = vectorBuffers.get(1);
            List<ArrowBuf> dataBuffers = vectorBuffers.subList(v + 1, vectorBuffers.size());
            writeValueToViewGenerator(bufferType, viewBuffer, dataBuffers, vector, i);
          } else if (bufferType.equals(VARIADIC_DATA_BUFFERS)
              && (vector.getMinorType() == MinorType.VIEWVARCHAR
                  || vector.getMinorType() == MinorType.VIEWVARBINARY)) {
            ArrowBuf viewBuffer = vectorBuffers.get(1); // check if this is v-1
            List<ArrowBuf> dataBuffers = vectorBuffers.subList(v, vectorBuffers.size());
            if (!dataBuffers.isEmpty()) {
              writeValueToDataBufferGenerator(bufferType, viewBuffer, dataBuffers, vector);
              // The variadic buffers are written at once and doesn't require iterating for
              // each index.
              // So, break the loop.
              break;
            }
          } else if (bufferType.equals(OFFSET)
              && vector.getValueCount() == 0
              && (vector.getMinorType() == MinorType.LIST
                  || vector.getMinorType() == MinorType.LISTVIEW
                  || vector.getMinorType() == MinorType.MAP
                  || vector.getMinorType() == MinorType.VARBINARY
                  || vector.getMinorType() == MinorType.VARCHAR)) {
            // Empty vectors may not have allocated an offsets buffer
            try (ArrowBuf vectorBufferTmp = vector.getAllocator().buffer(4)) {
              vectorBufferTmp.setInt(0, 0);
              writeValueToGenerator(bufferType, vectorBufferTmp, null, vector, i);
            }
          } else if (bufferType.equals(OFFSET)
              && vector.getValueCount() == 0
              && (vector.getMinorType() == MinorType.LARGELIST
                  || vector.getMinorType() == MinorType.LARGEVARBINARY
                  || vector.getMinorType() == MinorType.LARGEVARCHAR)) {
            // Empty vectors may not have allocated an offsets buffer
            try (ArrowBuf vectorBufferTmp = vector.getAllocator().buffer(8)) {
              vectorBufferTmp.setLong(0, 0);
              writeValueToGenerator(bufferType, vectorBufferTmp, null, vector, i);
            }
          } else {
            writeValueToGenerator(bufferType, vectorBuffer, null, vector, i);
          }
        }
        generator.writeEndArray();
      }

      List<Field> fields = field.getChildren();
      List<FieldVector> children = vector.getChildrenFromFields();
      if (fields.size() != children.size()) {
        throw new IllegalArgumentException(
            "fields and children are not the same size: "
                + fields.size()
                + " != "
                + children.size());
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

  /**
   * Get data of a view by index.
   *
   * @param viewBuffer view buffer
   * @param dataBuffers data buffers
   * @param index index of the view
   * @return byte array of the view
   */
  private byte[] getView(final ArrowBuf viewBuffer, final List<ArrowBuf> dataBuffers, int index) {
    final int dataLength =
        viewBuffer.getInt((long) index * BaseVariableWidthViewVector.ELEMENT_SIZE);
    byte[] result = new byte[dataLength];

    final int inlineSize = BaseVariableWidthViewVector.INLINE_SIZE;
    final int elementSize = BaseVariableWidthViewVector.ELEMENT_SIZE;
    final int lengthWidth = BaseVariableWidthViewVector.LENGTH_WIDTH;
    final int prefixWidth = BaseVariableWidthViewVector.PREFIX_WIDTH;
    final int bufIndexWidth = BaseVariableWidthViewVector.BUF_INDEX_WIDTH;

    if (dataLength > inlineSize) {
      // data is in the data buffer
      // get buffer index
      final int bufferIndex =
          viewBuffer.getInt(((long) index * elementSize) + lengthWidth + prefixWidth);
      // get data offset
      final int dataOffset =
          viewBuffer.getInt(
              ((long) index * elementSize) + lengthWidth + prefixWidth + bufIndexWidth);
      dataBuffers.get(bufferIndex).getBytes(dataOffset, result, 0, dataLength);
    } else {
      // data is in the view buffer
      viewBuffer.getBytes((long) index * elementSize + lengthWidth, result, 0, dataLength);
    }
    return result;
  }

  private void writeValueToViewGenerator(
      BufferType bufferType,
      ArrowBuf viewBuffer,
      List<ArrowBuf> dataBuffers,
      FieldVector vector,
      final int index)
      throws IOException {
    Preconditions.checkNotNull(viewBuffer);
    byte[] b = getView(viewBuffer, dataBuffers, index);
    final int elementSize = BaseVariableWidthViewVector.ELEMENT_SIZE;
    final int lengthWidth = BaseVariableWidthViewVector.LENGTH_WIDTH;
    final int prefixWidth = BaseVariableWidthViewVector.PREFIX_WIDTH;
    final int bufIndexWidth = BaseVariableWidthViewVector.BUF_INDEX_WIDTH;
    final int length = viewBuffer.getInt((long) index * elementSize);
    generator.writeStartObject();
    generator.writeFieldName("SIZE");
    generator.writeObject(length);
    if (length > 12) {
      byte[] prefix = Arrays.copyOfRange(b, 0, prefixWidth);
      final int bufferIndex =
          viewBuffer.getInt(((long) index * elementSize) + lengthWidth + prefixWidth);
      // get data offset
      final int dataOffset =
          viewBuffer.getInt(
              ((long) index * elementSize) + lengthWidth + prefixWidth + bufIndexWidth);
      generator.writeFieldName("PREFIX_HEX");
      generator.writeString(Hex.encodeHexString(prefix));
      generator.writeFieldName("BUFFER_INDEX");
      generator.writeObject(bufferIndex);
      generator.writeFieldName("OFFSET");
      generator.writeObject(dataOffset);
    } else {
      generator.writeFieldName("INLINED");
      if (vector.getMinorType() == MinorType.VIEWVARCHAR) {
        generator.writeString(new String(b, "UTF-8"));
      } else {
        generator.writeString(Hex.encodeHexString(b));
      }
    }
    generator.writeEndObject();
  }

  private void writeValueToDataBufferGenerator(
      BufferType bufferType, ArrowBuf viewBuffer, List<ArrowBuf> dataBuffers, FieldVector vector)
      throws IOException {
    if (bufferType.equals(VARIADIC_DATA_BUFFERS)) {
      Preconditions.checkNotNull(viewBuffer);
      Preconditions.checkArgument(!dataBuffers.isEmpty());

      for (int i = 0; i < dataBuffers.size(); i++) {
        ArrowBuf dataBuf = dataBuffers.get(i);
        byte[] result = new byte[(int) dataBuf.writerIndex()];
        dataBuf.getBytes(0, result);
        if (result != null) {
          generator.writeString(Hex.encodeHexString(result));
        }
      }
    }
  }

  private void writeValueToGenerator(
      BufferType bufferType,
      ArrowBuf buffer,
      ArrowBuf offsetBuffer,
      FieldVector vector,
      final int index)
      throws IOException {
    if (bufferType.equals(TYPE)) {
      generator.writeNumber(buffer.getByte(index * TinyIntVector.TYPE_WIDTH));
    } else if (bufferType.equals(OFFSET)) {
      switch (vector.getMinorType()) {
        case VARCHAR:
        case VARBINARY:
        case LIST:
        case MAP:
          generator.writeNumber(buffer.getInt((long) index * BaseVariableWidthVector.OFFSET_WIDTH));
          break;
        case LISTVIEW:
          generator.writeNumber(
              buffer.getInt((long) index * BaseRepeatedValueViewVector.OFFSET_WIDTH));
          break;
        case LARGELIST:
        case LARGEVARBINARY:
        case LARGEVARCHAR:
          generator.writeNumber(
              buffer.getLong((long) index * BaseLargeVariableWidthVector.OFFSET_WIDTH));
          break;
        default:
          throw new IllegalArgumentException("Type has no offset buffer: " + vector.getField());
      }
    } else if (bufferType.equals(VALIDITY)) {
      generator.writeNumber(vector.isNull(index) ? 0 : 1);
    } else if (bufferType.equals(DATA)) {
      switch (vector.getMinorType()) {
        case TINYINT:
          generator.writeNumber(TinyIntVector.get(buffer, index));
          break;
        case SMALLINT:
          generator.writeNumber(SmallIntVector.get(buffer, index));
          break;
        case INT:
          generator.writeNumber(IntVector.get(buffer, index));
          break;
        case BIGINT:
          generator.writeString(String.valueOf(BigIntVector.get(buffer, index)));
          break;
        case UINT1:
          generator.writeNumber(UInt1Vector.getNoOverflow(buffer, index));
          break;
        case UINT2:
          generator.writeNumber(UInt2Vector.get(buffer, index));
          break;
        case UINT4:
          generator.writeNumber(UInt4Vector.getNoOverflow(buffer, index));
          break;
        case UINT8:
          generator.writeString(UInt8Vector.getNoOverflow(buffer, index).toString());
          break;
        case FLOAT4:
          generator.writeNumber(Float4Vector.get(buffer, index));
          break;
        case FLOAT8:
          generator.writeNumber(Float8Vector.get(buffer, index));
          break;
        case DATEDAY:
          generator.writeNumber(DateDayVector.get(buffer, index));
          break;
        case DATEMILLI:
          generator.writeNumber(DateMilliVector.get(buffer, index));
          break;
        case TIMESEC:
          generator.writeNumber(TimeSecVector.get(buffer, index));
          break;
        case TIMEMILLI:
          generator.writeNumber(TimeMilliVector.get(buffer, index));
          break;
        case TIMEMICRO:
          generator.writeNumber(TimeMicroVector.get(buffer, index));
          break;
        case TIMENANO:
          generator.writeNumber(TimeNanoVector.get(buffer, index));
          break;
        case TIMESTAMPSEC:
          generator.writeNumber(TimeStampSecVector.get(buffer, index));
          break;
        case TIMESTAMPMILLI:
          generator.writeNumber(TimeStampMilliVector.get(buffer, index));
          break;
        case TIMESTAMPMICRO:
          generator.writeNumber(TimeStampMicroVector.get(buffer, index));
          break;
        case TIMESTAMPNANO:
          generator.writeNumber(TimeStampNanoVector.get(buffer, index));
          break;
        case TIMESTAMPSECTZ:
          generator.writeNumber(TimeStampSecTZVector.get(buffer, index));
          break;
        case TIMESTAMPMILLITZ:
          generator.writeNumber(TimeStampMilliTZVector.get(buffer, index));
          break;
        case TIMESTAMPMICROTZ:
          generator.writeNumber(TimeStampMicroTZVector.get(buffer, index));
          break;
        case TIMESTAMPNANOTZ:
          generator.writeNumber(TimeStampNanoTZVector.get(buffer, index));
          break;
        case DURATION:
          generator.writeNumber(DurationVector.get(buffer, index));
          break;
        case INTERVALYEAR:
          generator.writeNumber(IntervalYearVector.getTotalMonths(buffer, index));
          break;
        case INTERVALDAY:
          generator.writeStartObject();
          generator.writeObjectField("days", IntervalDayVector.getDays(buffer, index));
          generator.writeObjectField(
              "milliseconds", IntervalDayVector.getMilliseconds(buffer, index));
          generator.writeEndObject();
          break;
        case INTERVALMONTHDAYNANO:
          generator.writeStartObject();
          generator.writeObjectField("months", IntervalMonthDayNanoVector.getMonths(buffer, index));
          generator.writeObjectField("days", IntervalMonthDayNanoVector.getDays(buffer, index));
          generator.writeObjectField(
              "nanoseconds", IntervalMonthDayNanoVector.getNanoseconds(buffer, index));
          generator.writeEndObject();
          break;
        case BIT:
          generator.writeNumber(BitVectorHelper.get(buffer, index));
          break;
        case VARBINARY:
          {
            Preconditions.checkNotNull(offsetBuffer);
            String hexString =
                Hex.encodeHexString(BaseVariableWidthVector.get(buffer, offsetBuffer, index));
            generator.writeObject(hexString);
            break;
          }
        case FIXEDSIZEBINARY:
          int byteWidth = ((FixedSizeBinaryVector) vector).getByteWidth();
          String fixedSizeHexString =
              Hex.encodeHexString(FixedSizeBinaryVector.get(buffer, index, byteWidth));
          generator.writeObject(fixedSizeHexString);
          break;
        case VARCHAR:
          {
            Preconditions.checkNotNull(offsetBuffer);
            byte[] b = (BaseVariableWidthVector.get(buffer, offsetBuffer, index));
            generator.writeString(new String(b, "UTF-8"));
            break;
          }
        case DECIMAL:
          {
            int scale = ((DecimalVector) vector).getScale();
            BigDecimal decimalValue =
                DecimalUtility.getBigDecimalFromArrowBuf(
                    buffer, index, scale, DecimalVector.TYPE_WIDTH);
            // We write the unscaled value, because the scale is stored in the type metadata.
            generator.writeString(decimalValue.unscaledValue().toString());
            break;
          }
        case DECIMAL256:
          {
            int scale = ((Decimal256Vector) vector).getScale();
            BigDecimal decimalValue =
                DecimalUtility.getBigDecimalFromArrowBuf(
                    buffer, index, scale, Decimal256Vector.TYPE_WIDTH);
            // We write the unscaled value, because the scale is stored in the type metadata.
            generator.writeString(decimalValue.unscaledValue().toString());
            break;
          }

        default:
          throw new UnsupportedOperationException("minor type: " + vector.getMinorType());
      }
    } else if (bufferType.equals(SIZE)) {
      generator.writeNumber(buffer.getInt((long) index * BaseRepeatedValueViewVector.SIZE_WIDTH));
    }
  }

  @Override
  public void close() throws IOException {
    generator.writeEndArray();
    generator.writeEndObject();
    generator.close();
  }
}
