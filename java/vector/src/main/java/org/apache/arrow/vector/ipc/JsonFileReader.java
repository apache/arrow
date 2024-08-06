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

import static com.fasterxml.jackson.core.JsonToken.END_ARRAY;
import static com.fasterxml.jackson.core.JsonToken.END_OBJECT;
import static com.fasterxml.jackson.core.JsonToken.START_ARRAY;
import static com.fasterxml.jackson.core.JsonToken.START_OBJECT;
import static org.apache.arrow.vector.BufferLayout.BufferType.DATA;
import static org.apache.arrow.vector.BufferLayout.BufferType.OFFSET;
import static org.apache.arrow.vector.BufferLayout.BufferType.SIZE;
import static org.apache.arrow.vector.BufferLayout.BufferType.TYPE;
import static org.apache.arrow.vector.BufferLayout.BufferType.VALIDITY;
import static org.apache.arrow.vector.BufferLayout.BufferType.VARIADIC_DATA_BUFFERS;
import static org.apache.arrow.vector.BufferLayout.BufferType.VIEWS;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonParser.Feature;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.MappingJsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.BaseVariableWidthViewVector;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVectorHelper;
import org.apache.arrow.vector.BufferLayout.BufferType;
import org.apache.arrow.vector.Decimal256Vector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.IntervalDayVector;
import org.apache.arrow.vector.IntervalMonthDayNanoVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.TypeLayout;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.ArrowType.ListView;
import org.apache.arrow.vector.types.pojo.ArrowType.Union;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.DecimalUtility;
import org.apache.arrow.vector.util.DictionaryUtility;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;

/**
 * A reader for JSON files that translates them into vectors. This reader is used for integration
 * tests.
 *
 * <p>This class uses a streaming parser API, method naming tends to reflect this implementation
 * detail.
 */
public class JsonFileReader implements AutoCloseable, DictionaryProvider {
  private final JsonParser parser;
  private final BufferAllocator allocator;
  private Schema schema;
  private Map<Long, Dictionary> dictionaries;
  private Boolean started = false;

  /**
   * Constructs a new instance.
   *
   * @param inputFile The file to read.
   * @param allocator The allocator to use for allocating buffers.
   */
  public JsonFileReader(File inputFile, BufferAllocator allocator)
      throws JsonParseException, IOException {
    super();
    this.allocator = allocator;
    MappingJsonFactory jsonFactory =
        new MappingJsonFactory(
            new ObjectMapper()
                // ignore case for enums
                .configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS, true));
    this.parser = jsonFactory.createParser(inputFile);
    // Allow reading NaN for floating point values
    this.parser.configure(Feature.ALLOW_NON_NUMERIC_NUMBERS, true);
  }

  @Override
  public Dictionary lookup(long id) {
    if (!started) {
      throw new IllegalStateException("Unable to lookup until after read() has started");
    }

    return dictionaries.get(id);
  }

  @Override
  public Set<Long> getDictionaryIds() {
    return dictionaries.keySet();
  }

  /** Reads the beginning (schema section) of the json file and returns it. */
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
        throw new IllegalArgumentException(
            "Dictionary with id: " + id + " missing encoding from schema Field");
      }

      // Read the dictionary record batch
      nextFieldIs("data");
      FieldVector vector = dict.getVector();
      List<Field> fields = Collections.singletonList(vector.getField());
      List<FieldVector> vectors = Collections.singletonList(vector);
      VectorSchemaRoot root = new VectorSchemaRoot(fields, vectors, vector.getValueCount());
      read(root);

      readToken(END_OBJECT);
      token = parser.nextToken();
      haveDictionaryBatch = token == START_OBJECT;
    }

    if (token != END_ARRAY) {
      throw new IllegalArgumentException(
          "Invalid token: " + token + " expected end of array at " + parser.getTokenLocation());
    }
  }

  /** Reads the next record batch from the file into <code>root</code>. */
  public boolean read(VectorSchemaRoot root) throws IOException {
    JsonToken t = parser.nextToken();
    if (t == START_OBJECT) {
      {
        int count = readNextField("count", Integer.class);
        nextFieldIs("columns");
        readToken(START_ARRAY);
        {
          for (Field field : root.getSchema().getFields()) {
            FieldVector vector = root.getVector(field);
            readFromJsonIntoVector(field, vector);
          }
        }
        readToken(END_ARRAY);
        root.setRowCount(count);
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

  /** Returns the next record batch from the file. */
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
            FieldVector vector = recordBatch.getVector(field);
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

  /**
   * Skips a number of record batches in the file.
   *
   * @param numBatches the number of batches to skip
   * @return the actual number of skipped batches.
   */
  // This is currently called using JPype by the integration tests.
  public int skip(int numBatches) throws IOException {
    for (int i = 0; i < numBatches; ++i) {
      JsonToken t = parser.nextToken();
      if (t == START_OBJECT) {
        parser.skipChildren();
        assert parser.getCurrentToken() == END_OBJECT;
      } else if (t == END_ARRAY) {
        return i;
      } else {
        throw new IllegalArgumentException("Invalid token: " + t);
      }
    }
    return numBatches;
  }

  private abstract class BufferReader {
    protected abstract ArrowBuf read(BufferAllocator allocator, int count) throws IOException;

    ArrowBuf readBuffer(BufferAllocator allocator, int count) throws IOException {
      readToken(START_ARRAY);
      ArrowBuf buf = read(allocator, count);
      readToken(END_ARRAY);
      return buf;
    }
  }

  /**
   * Read all the variadic data buffers from the parser.
   *
   * @param allocator BufferAllocator
   * @param variadicBuffersCount Number of variadic buffers
   * @return List of ArrowBuf
   * @throws IOException throws IOException in a failure
   */
  List<ArrowBuf> readVariadicBuffers(BufferAllocator allocator, int variadicBuffersCount)
      throws IOException {
    readToken(START_ARRAY);
    ArrayList<ArrowBuf> dataBuffers = new ArrayList<>(variadicBuffersCount);
    for (int i = 0; i < variadicBuffersCount; i++) {
      parser.nextToken();
      final byte[] value;

      String variadicStr = parser.readValueAs(String.class);
      if (variadicStr == null) {
        value = new byte[0];
      } else {
        value = decodeHexSafe(variadicStr);
      }

      ArrowBuf buf = allocator.buffer(value.length);
      buf.writeBytes(value);
      dataBuffers.add(buf);
    }
    readToken(END_ARRAY);
    return dataBuffers;
  }

  private ArrowBuf readViewBuffers(
      BufferAllocator allocator, int count, List<Integer> variadicBufferIndices, MinorType type)
      throws IOException {
    readToken(START_ARRAY);
    ArrayList<byte[]> values = new ArrayList<>(count);
    long bufferSize = 0L;
    for (int i = 0; i < count; i++) {
      readToken(START_OBJECT);
      final int length = readNextField("SIZE", Integer.class);
      byte[] value;
      if (length > BaseVariableWidthViewVector.INLINE_SIZE) {
        // PREFIX_HEX
        final byte[] prefix = decodeHexSafe(readNextField("PREFIX_HEX", String.class));
        // BUFFER_INDEX
        final int bufferIndex = readNextField("BUFFER_INDEX", Integer.class);
        if (variadicBufferIndices.isEmpty()) {
          variadicBufferIndices.add(bufferIndex);
        } else {
          int lastBufferIndex = variadicBufferIndices.get(variadicBufferIndices.size() - 1);
          if (lastBufferIndex != bufferIndex) {
            variadicBufferIndices.add(bufferIndex);
          }
        }

        // OFFSET
        final int offset = readNextField("OFFSET", Integer.class);
        ByteBuffer buffer =
            ByteBuffer.allocate(BaseVariableWidthViewVector.ELEMENT_SIZE)
                .order(ByteOrder.LITTLE_ENDIAN); // Allocate a ByteBuffer of size 16 bytes
        buffer.putInt(length); // Write 'length' to bytes 0-3
        buffer.put(prefix); // Write 'prefix' to bytes 4-7
        buffer.putInt(bufferIndex); // Write 'bufferIndex' to bytes 8-11
        buffer.putInt(offset); // Write 'offset' to bytes 12-15
        value = buffer.array(); // Convert the ByteBuffer to a byte array
      } else {
        // in-line
        ByteBuffer buffer =
            ByteBuffer.allocate(BaseVariableWidthViewVector.ELEMENT_SIZE)
                .order(ByteOrder.LITTLE_ENDIAN); // Allocate a ByteBuffer of size 16 bytes
        buffer.putInt(length); // Write 'length' to bytes 0-3
        // INLINE
        if (type == MinorType.VIEWVARCHAR) {
          buffer.put(readNextField("INLINED", String.class).getBytes(StandardCharsets.UTF_8));
        } else {
          String inlined = readNextField("INLINED", String.class);
          if (inlined == null) {
            buffer.put(new byte[length]);
          } else {
            buffer.put(decodeHexSafe(inlined));
          }
        }
        value = buffer.array(); // Convert the ByteBuffer to a byte array
      }
      values.add(value);
      bufferSize += value.length;
      readToken(END_OBJECT);
    }

    ArrowBuf buf = allocator.buffer(bufferSize);

    for (byte[] value : values) {
      buf.writeBytes(value);
    }
    readToken(END_ARRAY);
    return buf;
  }

  private class BufferHelper {

    BufferReader BIT =
        new BufferReader() {
          @Override
          protected ArrowBuf read(BufferAllocator allocator, int count) throws IOException {
            final int bufferSize = BitVectorHelper.getValidityBufferSize(count);
            ArrowBuf buf = allocator.buffer(bufferSize);

            // C++ integration test fails without this.
            buf.setZero(0, bufferSize);

            for (int i = 0; i < count; i++) {
              parser.nextToken();
              BitVectorHelper.setValidityBit(buf, i, parser.readValueAs(Boolean.class) ? 1 : 0);
            }

            buf.writerIndex(bufferSize);
            return buf;
          }
        };

    BufferReader DAY_MILLIS =
        new BufferReader() {
          @Override
          protected ArrowBuf read(BufferAllocator allocator, int count) throws IOException {
            final long size = (long) count * IntervalDayVector.TYPE_WIDTH;
            ArrowBuf buf = allocator.buffer(size);

            for (int i = 0; i < count; i++) {
              readToken(START_OBJECT);
              buf.writeInt(readNextField("days", Integer.class));
              buf.writeInt(readNextField("milliseconds", Integer.class));
              readToken(END_OBJECT);
            }

            return buf;
          }
        };

    BufferReader MONTH_DAY_NANOS =
        new BufferReader() {
          @Override
          protected ArrowBuf read(BufferAllocator allocator, int count) throws IOException {
            final long size = (long) count * IntervalMonthDayNanoVector.TYPE_WIDTH;
            ArrowBuf buf = allocator.buffer(size);

            for (int i = 0; i < count; i++) {
              readToken(START_OBJECT);
              buf.writeInt(readNextField("months", Integer.class));
              buf.writeInt(readNextField("days", Integer.class));
              buf.writeLong(readNextField("nanoseconds", Long.class));
              readToken(END_OBJECT);
            }

            return buf;
          }
        };

    BufferReader INT1 =
        new BufferReader() {
          @Override
          protected ArrowBuf read(BufferAllocator allocator, int count) throws IOException {
            final long size = (long) count * TinyIntVector.TYPE_WIDTH;
            ArrowBuf buf = allocator.buffer(size);

            for (int i = 0; i < count; i++) {
              parser.nextToken();
              buf.writeByte(parser.getByteValue());
            }

            return buf;
          }
        };

    BufferReader INT2 =
        new BufferReader() {
          @Override
          protected ArrowBuf read(BufferAllocator allocator, int count) throws IOException {
            final long size = (long) count * SmallIntVector.TYPE_WIDTH;
            ArrowBuf buf = allocator.buffer(size);

            for (int i = 0; i < count; i++) {
              parser.nextToken();
              buf.writeShort(parser.getShortValue());
            }

            return buf;
          }
        };

    BufferReader INT4 =
        new BufferReader() {
          @Override
          protected ArrowBuf read(BufferAllocator allocator, int count) throws IOException {
            final long size = (long) count * IntVector.TYPE_WIDTH;
            ArrowBuf buf = allocator.buffer(size);

            for (int i = 0; i < count; i++) {
              parser.nextToken();
              buf.writeInt(parser.getIntValue());
            }

            return buf;
          }
        };

    BufferReader INT8 =
        new BufferReader() {
          @Override
          protected ArrowBuf read(BufferAllocator allocator, int count) throws IOException {
            final long size = (long) count * BigIntVector.TYPE_WIDTH;
            ArrowBuf buf = allocator.buffer(size);

            for (int i = 0; i < count; i++) {
              parser.nextToken();
              String value = parser.getValueAsString();
              buf.writeLong(Long.valueOf(value));
            }

            return buf;
          }
        };

    BufferReader UINT1 =
        new BufferReader() {
          @Override
          protected ArrowBuf read(BufferAllocator allocator, int count) throws IOException {
            final long size = (long) count * TinyIntVector.TYPE_WIDTH;
            ArrowBuf buf = allocator.buffer(size);

            for (int i = 0; i < count; i++) {
              parser.nextToken();
              buf.writeByte(parser.getShortValue() & 0xFF);
            }

            return buf;
          }
        };

    BufferReader UINT2 =
        new BufferReader() {
          @Override
          protected ArrowBuf read(BufferAllocator allocator, int count) throws IOException {
            final long size = (long) count * SmallIntVector.TYPE_WIDTH;
            ArrowBuf buf = allocator.buffer(size);

            for (int i = 0; i < count; i++) {
              parser.nextToken();
              buf.writeShort(parser.getIntValue() & 0xFFFF);
            }

            return buf;
          }
        };

    BufferReader UINT4 =
        new BufferReader() {
          @Override
          protected ArrowBuf read(BufferAllocator allocator, int count) throws IOException {
            final long size = (long) count * IntVector.TYPE_WIDTH;
            ArrowBuf buf = allocator.buffer(size);

            for (int i = 0; i < count; i++) {
              parser.nextToken();
              buf.writeInt((int) parser.getLongValue());
            }

            return buf;
          }
        };

    BufferReader UINT8 =
        new BufferReader() {
          @Override
          protected ArrowBuf read(BufferAllocator allocator, int count) throws IOException {
            final long size = (long) count * BigIntVector.TYPE_WIDTH;
            ArrowBuf buf = allocator.buffer(size);

            for (int i = 0; i < count; i++) {
              parser.nextToken();
              BigInteger value = new BigInteger(parser.getValueAsString());
              buf.writeLong(value.longValue());
            }

            return buf;
          }
        };

    BufferReader FLOAT4 =
        new BufferReader() {
          @Override
          protected ArrowBuf read(BufferAllocator allocator, int count) throws IOException {
            final long size = (long) count * Float4Vector.TYPE_WIDTH;
            ArrowBuf buf = allocator.buffer(size);

            for (int i = 0; i < count; i++) {
              parser.nextToken();
              buf.writeFloat(parser.getFloatValue());
            }

            return buf;
          }
        };

    BufferReader FLOAT8 =
        new BufferReader() {
          @Override
          protected ArrowBuf read(BufferAllocator allocator, int count) throws IOException {
            final long size = (long) count * Float8Vector.TYPE_WIDTH;
            ArrowBuf buf = allocator.buffer(size);

            for (int i = 0; i < count; i++) {
              parser.nextToken();
              buf.writeDouble(parser.getDoubleValue());
            }

            return buf;
          }
        };

    BufferReader DECIMAL =
        new BufferReader() {
          @Override
          protected ArrowBuf read(BufferAllocator allocator, int count) throws IOException {
            final long size = (long) count * DecimalVector.TYPE_WIDTH;
            ArrowBuf buf = allocator.buffer(size);

            for (int i = 0; i < count; i++) {
              parser.nextToken();
              BigDecimal decimalValue = new BigDecimal(parser.readValueAs(String.class));
              DecimalUtility.writeBigDecimalToArrowBuf(
                  decimalValue, buf, i, DecimalVector.TYPE_WIDTH);
            }

            buf.writerIndex(size);
            return buf;
          }
        };

    BufferReader DECIMAL256 =
        new BufferReader() {
          @Override
          protected ArrowBuf read(BufferAllocator allocator, int count) throws IOException {
            final long size = (long) count * Decimal256Vector.TYPE_WIDTH;
            ArrowBuf buf = allocator.buffer(size);

            for (int i = 0; i < count; i++) {
              parser.nextToken();
              BigDecimal decimalValue = new BigDecimal(parser.readValueAs(String.class));
              DecimalUtility.writeBigDecimalToArrowBuf(
                  decimalValue, buf, i, Decimal256Vector.TYPE_WIDTH);
            }

            buf.writerIndex(size);
            return buf;
          }
        };

    ArrowBuf readBinaryValues(BufferAllocator allocator, int count) throws IOException {
      ArrayList<byte[]> values = new ArrayList<>(count);
      long bufferSize = 0L;
      for (int i = 0; i < count; i++) {
        parser.nextToken();
        final byte[] value = decodeHexSafe(parser.readValueAs(String.class));
        values.add(value);
        bufferSize += value.length;
      }

      ArrowBuf buf = allocator.buffer(bufferSize);

      for (byte[] value : values) {
        buf.writeBytes(value);
      }

      return buf;
    }

    ArrowBuf readStringValues(BufferAllocator allocator, int count) throws IOException {
      ArrayList<byte[]> values = new ArrayList<>(count);
      long bufferSize = 0L;
      for (int i = 0; i < count; i++) {
        parser.nextToken();
        final byte[] value = parser.getValueAsString().getBytes(StandardCharsets.UTF_8);
        values.add(value);
        bufferSize += value.length;
      }

      ArrowBuf buf = allocator.buffer(bufferSize);

      for (byte[] value : values) {
        buf.writeBytes(value);
      }

      return buf;
    }

    BufferReader FIXEDSIZEBINARY =
        new BufferReader() {
          @Override
          protected ArrowBuf read(BufferAllocator allocator, int count) throws IOException {
            return readBinaryValues(allocator, count);
          }
        };

    BufferReader VARCHAR =
        new BufferReader() {
          @Override
          protected ArrowBuf read(BufferAllocator allocator, int count) throws IOException {
            return readStringValues(allocator, count);
          }
        };

    BufferReader LARGEVARCHAR =
        new BufferReader() {
          @Override
          protected ArrowBuf read(BufferAllocator allocator, int count) throws IOException {
            return readStringValues(allocator, count);
          }
        };

    BufferReader VARBINARY =
        new BufferReader() {
          @Override
          protected ArrowBuf read(BufferAllocator allocator, int count) throws IOException {
            return readBinaryValues(allocator, count);
          }
        };

    BufferReader LARGEVARBINARY =
        new BufferReader() {
          @Override
          protected ArrowBuf read(BufferAllocator allocator, int count) throws IOException {
            return readBinaryValues(allocator, count);
          }
        };
  }

  private List<ArrowBuf> readIntoBuffer(
      BufferAllocator allocator,
      BufferType bufferType,
      MinorType type,
      int count,
      List<Integer> variadicBufferIndices)
      throws IOException {
    ArrowBuf buf;

    BufferHelper helper = new BufferHelper();
    BufferReader reader;

    if (bufferType.equals(VALIDITY)) {
      reader = helper.BIT;
    } else if (bufferType.equals(OFFSET) || bufferType.equals(SIZE)) {
      if (type == MinorType.LARGELIST
          || type == MinorType.LARGEVARCHAR
          || type == MinorType.LARGEVARBINARY) {
        reader = helper.INT8;
      } else {
        reader = helper.INT4;
      }
    } else if (bufferType.equals(TYPE)) {
      reader = helper.INT1;
    } else if (bufferType.equals(DATA)) {
      switch (type) {
        case BIT:
          reader = helper.BIT;
          break;
        case TINYINT:
          reader = helper.INT1;
          break;
        case SMALLINT:
          reader = helper.INT2;
          break;
        case INT:
          reader = helper.INT4;
          break;
        case BIGINT:
          reader = helper.INT8;
          break;
        case UINT1:
          reader = helper.UINT1;
          break;
        case UINT2:
          reader = helper.UINT2;
          break;
        case UINT4:
          reader = helper.UINT4;
          break;
        case UINT8:
          reader = helper.UINT8;
          break;
        case FLOAT4:
          reader = helper.FLOAT4;
          break;
        case FLOAT8:
          reader = helper.FLOAT8;
          break;
        case DECIMAL:
          reader = helper.DECIMAL;
          break;
        case DECIMAL256:
          reader = helper.DECIMAL256;
          break;
        case FIXEDSIZEBINARY:
          reader = helper.FIXEDSIZEBINARY;
          break;
        case VARCHAR:
          reader = helper.VARCHAR;
          break;
        case LARGEVARCHAR:
          reader = helper.LARGEVARCHAR;
          break;
        case VARBINARY:
          reader = helper.VARBINARY;
          break;
        case LARGEVARBINARY:
          reader = helper.LARGEVARBINARY;
          break;
        case DATEDAY:
          reader = helper.INT4;
          break;
        case DATEMILLI:
          reader = helper.INT8;
          break;
        case TIMESEC:
        case TIMEMILLI:
          reader = helper.INT4;
          break;
        case TIMEMICRO:
        case TIMENANO:
          reader = helper.INT8;
          break;
        case TIMESTAMPNANO:
        case TIMESTAMPMICRO:
        case TIMESTAMPMILLI:
        case TIMESTAMPSEC:
        case TIMESTAMPNANOTZ:
        case TIMESTAMPMICROTZ:
        case TIMESTAMPMILLITZ:
        case TIMESTAMPSECTZ:
          reader = helper.INT8;
          break;
        case INTERVALYEAR:
          reader = helper.INT4;
          break;
        case INTERVALDAY:
          reader = helper.DAY_MILLIS;
          break;
        case INTERVALMONTHDAYNANO:
          reader = helper.MONTH_DAY_NANOS;
          break;
        case DURATION:
          reader = helper.INT8;
          break;
        default:
          throw new UnsupportedOperationException("Cannot read array of type " + type);
      }
    } else if (bufferType.equals(VIEWS)) {
      return Collections.singletonList(
          readViewBuffers(allocator, count, variadicBufferIndices, type));
    } else if (bufferType.equals(VARIADIC_DATA_BUFFERS)) {
      return readVariadicBuffers(allocator, variadicBufferIndices.size());
    } else {
      throw new InvalidArrowFileException("Unrecognized buffer type " + bufferType);
    }

    buf = reader.readBuffer(allocator, count);
    Preconditions.checkNotNull(buf);
    return Collections.singletonList(buf);
  }

  private void readFromJsonIntoVector(Field field, FieldVector vector) throws IOException {
    ArrowType type = field.getType();
    TypeLayout typeLayout = TypeLayout.getTypeLayout(type);
    List<BufferType> vectorTypes = typeLayout.getBufferTypes();
    List<ArrowBuf> vectorBuffers = new ArrayList<>(vectorTypes.size());
    List<Integer> variadicBufferIndices = new ArrayList<>();

    if (!typeLayout.isFixedBufferCount()) {
      vectorTypes.add(VARIADIC_DATA_BUFFERS);
    }
    /*
     * The order of inner buffers is:
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
      if (started && !Objects.equals(field.getName(), name)) {
        throw new IllegalArgumentException(
            "Expected field " + field.getName() + " but got " + name);
      }

      /* Initialize the vector with required capacity but don't allocateNew since we would
       * be doing loadFieldBuffers.
       */
      int valueCount = readNextField("count", Integer.class);

      vector.setInitialCapacity(valueCount);

      for (int v = 0; v < vectorTypes.size(); v++) {
        BufferType bufferType = vectorTypes.get(v);
        nextFieldIs(bufferType.getName());
        int innerBufferValueCount = valueCount;
        if (bufferType.equals(OFFSET) && !(type instanceof Union) && !(type instanceof ListView)) {
          /* offset buffer has 1 additional value capacity except for dense unions and ListView */
          innerBufferValueCount = valueCount + 1;
        }

        vectorBuffers.addAll(
            readIntoBuffer(
                allocator,
                bufferType,
                vector.getMinorType(),
                innerBufferValueCount,
                variadicBufferIndices));
      }

      int nullCount = 0;
      if (type instanceof ArrowType.Null) {
        nullCount = valueCount;
      } else if (!(type instanceof Union)) {
        nullCount = BitVectorHelper.getNullCount(vectorBuffers.get(0), valueCount);
      }
      final ArrowFieldNode fieldNode = new ArrowFieldNode(valueCount, nullCount);
      vector.loadFieldBuffers(fieldNode, vectorBuffers);

      /* read child vectors (if any) */
      List<Field> fields = field.getChildren();
      if (!fields.isEmpty()) {
        List<FieldVector> vectorChildren = vector.getChildrenFromFields();
        if (fields.size() != vectorChildren.size()) {
          throw new IllegalArgumentException(
              "fields and children are not the same size: "
                  + fields.size()
                  + " != "
                  + vectorChildren.size());
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

    for (ArrowBuf buffer : vectorBuffers) {
      buffer.getReferenceManager().release();
    }
  }

  private byte[] decodeHexSafe(String hexString) throws IOException {
    try {
      return Hex.decodeHex(hexString.toCharArray());
    } catch (DecoderException e) {
      throw new IOException("Unable to decode hex string: " + hexString, e);
    }
  }

  @Override
  public void close() throws IOException {
    parser.close();
    if (dictionaries != null) {
      for (Dictionary dictionary : dictionaries.values()) {
        dictionary.getVector().close();
      }
    }
  }

  private <T> T readNextField(String expectedFieldName, Class<T> c)
      throws IOException, JsonParseException {
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
