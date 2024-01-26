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

package org.apache.arrow.dataset;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import org.apache.arrow.dataset.file.DatasetFileWriter;
import org.apache.arrow.dataset.file.FileFormat;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.util.Float16;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.Decimal256Vector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.DurationVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.Float2Vector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.LargeVarBinaryVector;
import org.apache.arrow.vector.LargeVarCharVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.TimeStampMilliTZVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.TimeStampNanoTZVector;
import org.apache.arrow.vector.TimeStampNanoVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.UInt1Vector;
import org.apache.arrow.vector.UInt2Vector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.LargeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.impl.UnionFixedSizeListWriter;
import org.apache.arrow.vector.complex.impl.UnionLargeListWriter;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.test.util.ArrowTestDataUtil;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.ByteArrayReadableSeekableByteChannel;
import org.apache.arrow.vector.util.Text;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestAllTypes extends TestDataset {

  @ClassRule
  public static final TemporaryFolder TMP = new TemporaryFolder();

  private VectorSchemaRoot generateAllTypesVector(BufferAllocator allocator) {
    // Notes:
    // - IntervalMonthDayNano is not supported by Parquet.
    // - Map (GH-38250) and SparseUnion are resulting in serialization errors when writing with the Dataset API.
    // "Unhandled type for Arrow to Parquet schema conversion" errors: IntervalDay, IntervalYear, DenseUnion
    List<Field> childFields = new ArrayList<>();
    childFields.add(new Field("int-child",
        new FieldType(false, new ArrowType.Int(32, true), null, null), null));
    Field structField = new Field("struct",
        new FieldType(true, ArrowType.Struct.INSTANCE, null, null), childFields);
    Field[] fields = new Field[] {
        Field.nullablePrimitive("null", ArrowType.Null.INSTANCE),
        Field.nullablePrimitive("bool", ArrowType.Bool.INSTANCE),
        Field.nullablePrimitive("int8", new ArrowType.Int(8, true)),
        Field.nullablePrimitive("int16", new ArrowType.Int(16, true)),
        Field.nullablePrimitive("int32", new ArrowType.Int(32, true)),
        Field.nullablePrimitive("int64", new ArrowType.Int(64, true)),
        Field.nullablePrimitive("uint8", new ArrowType.Int(8, false)),
        Field.nullablePrimitive("uint16", new ArrowType.Int(16, false)),
        Field.nullablePrimitive("uint32", new ArrowType.Int(32, false)),
        Field.nullablePrimitive("uint64", new ArrowType.Int(64, false)),
        Field.nullablePrimitive("float16", new ArrowType.FloatingPoint(FloatingPointPrecision.HALF)),
        Field.nullablePrimitive("float32", new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)),
        Field.nullablePrimitive("float64", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
        Field.nullablePrimitive("utf8", ArrowType.Utf8.INSTANCE),
        Field.nullablePrimitive("binary", ArrowType.Binary.INSTANCE),
        Field.nullablePrimitive("largeutf8", ArrowType.LargeUtf8.INSTANCE),
        Field.nullablePrimitive("largebinary", ArrowType.LargeBinary.INSTANCE),
        Field.nullablePrimitive("fixed_size_binary", new ArrowType.FixedSizeBinary(1)),
        Field.nullablePrimitive("date_ms", new ArrowType.Date(DateUnit.MILLISECOND)),
        Field.nullablePrimitive("time_ms", new ArrowType.Time(TimeUnit.MILLISECOND, 32)),
        Field.nullablePrimitive("timestamp_ms", new ArrowType.Timestamp(TimeUnit.MILLISECOND, null)),
        Field.nullablePrimitive("timestamptz_ms", new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC")),
        Field.nullablePrimitive("time_ns", new ArrowType.Time(TimeUnit.NANOSECOND, 64)),
        Field.nullablePrimitive("timestamp_ns", new ArrowType.Timestamp(TimeUnit.NANOSECOND, null)),
        Field.nullablePrimitive("timestamptz_ns", new ArrowType.Timestamp(TimeUnit.NANOSECOND, "UTC")),
        Field.nullablePrimitive("duration", new ArrowType.Duration(TimeUnit.MILLISECOND)),
        Field.nullablePrimitive("decimal128", new ArrowType.Decimal(10, 2, 128)),
        Field.nullablePrimitive("decimal256", new ArrowType.Decimal(10, 2, 256)),
        new Field("list", FieldType.nullable(ArrowType.List.INSTANCE),
            Collections.singletonList(Field.nullable("items", new ArrowType.Int(32, true)))),
        new Field("largelist", FieldType.nullable(ArrowType.LargeList.INSTANCE),
            Collections.singletonList(Field.nullable("items", new ArrowType.Int(32, true)))),
        new Field("fixedsizelist", FieldType.nullable(new ArrowType.FixedSizeList(2)),
            Collections.singletonList(Field.nullable("items", new ArrowType.Int(32, true)))),
        structField
    };
    VectorSchemaRoot root = VectorSchemaRoot.create(new Schema(Arrays.asList(fields)), allocator);
    root.allocateNew();
    root.setRowCount(2);

    root.getVector("null").setNull(0);
    root.getVector("bool").setNull(0);
    root.getVector("int8").setNull(0);
    root.getVector("int16").setNull(0);
    root.getVector("int32").setNull(0);
    root.getVector("int64").setNull(0);
    root.getVector("uint8").setNull(0);
    root.getVector("uint16").setNull(0);
    root.getVector("uint32").setNull(0);
    root.getVector("uint64").setNull(0);
    root.getVector("float16").setNull(0);
    root.getVector("float32").setNull(0);
    root.getVector("float64").setNull(0);
    root.getVector("utf8").setNull(0);
    root.getVector("binary").setNull(0);
    root.getVector("largeutf8").setNull(0);
    root.getVector("largebinary").setNull(0);
    root.getVector("fixed_size_binary").setNull(0);
    root.getVector("date_ms").setNull(0);
    root.getVector("time_ms").setNull(0);
    root.getVector("time_ns").setNull(0);
    root.getVector("timestamp_ms").setNull(0);
    root.getVector("timestamp_ns").setNull(0);
    root.getVector("timestamptz_ms").setNull(0);
    root.getVector("timestamptz_ns").setNull(0);
    root.getVector("duration").setNull(0);
    root.getVector("decimal128").setNull(0);
    root.getVector("decimal256").setNull(0);
    root.getVector("fixedsizelist").setNull(0);
    root.getVector("list").setNull(0);
    root.getVector("largelist").setNull(0);
    root.getVector("struct").setNull(0);

    root.getVector("null").setNull(1);
    ((BitVector) root.getVector("bool")).set(1, 1);
    ((TinyIntVector) root.getVector("int8")).set(1, 1);
    ((SmallIntVector) root.getVector("int16")).set(1, 1);
    ((IntVector) root.getVector("int32")).set(1, 1);
    ((BigIntVector) root.getVector("int64")).set(1, 1);
    ((UInt1Vector) root.getVector("uint8")).set(1, 1);
    ((UInt2Vector) root.getVector("uint16")).set(1, 1);
    ((UInt4Vector) root.getVector("uint32")).set(1, 1);
    ((UInt8Vector) root.getVector("uint64")).set(1, 1);
    ((Float2Vector) root.getVector("float16")).set(1, Float16.toFloat16(+32.875f));
    ((Float4Vector) root.getVector("float32")).set(1, 1.0f);
    ((Float8Vector) root.getVector("float64")).set(1, 1.0);
    ((VarCharVector) root.getVector("utf8")).set(1, new Text("a"));
    ((VarBinaryVector) root.getVector("binary")).set(1, new byte[] {0x01});
    ((LargeVarCharVector) root.getVector("largeutf8")).set(1, new Text("a"));
    ((LargeVarBinaryVector) root.getVector("largebinary")).set(1, new byte[] {0x01});
    ((FixedSizeBinaryVector) root.getVector("fixed_size_binary")).set(1, new byte[] {0x01});
    ((DateMilliVector) root.getVector("date_ms")).set(1, 0);
    ((TimeMilliVector) root.getVector("time_ms")).set(1, 0);
    ((TimeNanoVector) root.getVector("time_ns")).set(1, 0);
    ((TimeStampMilliVector) root.getVector("timestamp_ms")).set(1, 0);
    ((TimeStampNanoVector) root.getVector("timestamp_ns")).set(1, 0);
    ((TimeStampMilliTZVector) root.getVector("timestamptz_ms")).set(1, 0);
    ((TimeStampNanoTZVector) root.getVector("timestamptz_ns")).set(1, 0);
    ((DurationVector) root.getVector("duration")).set(1, 0);
    ((DecimalVector) root.getVector("decimal128")).set(1, 0);
    ((Decimal256Vector) root.getVector("decimal256")).set(1, 0);
    UnionFixedSizeListWriter fixedListWriter = ((FixedSizeListVector) root.getVector("fixedsizelist")).getWriter();
    fixedListWriter.allocate();
    fixedListWriter.setPosition(1);
    fixedListWriter.startList();
    fixedListWriter.integer().writeInt(1);
    fixedListWriter.endList();

    UnionListWriter listWriter = ((ListVector) root.getVector("list")).getWriter();
    listWriter.allocate();
    listWriter.setPosition(1);
    listWriter.startList();
    listWriter.integer().writeInt(1);
    listWriter.endList();

    UnionLargeListWriter largeListWriter = ((LargeListVector) root.getVector("largelist")).getWriter();
    largeListWriter.allocate();
    largeListWriter.setPosition(1);
    largeListWriter.startList();
    largeListWriter.integer().writeInt(1);
    largeListWriter.endList();

    ((StructVector) root.getVector("struct")).getChild("int-child", IntVector.class).set(1, 1);
    return root;
  }

  private byte[] serializeFile(VectorSchemaRoot root) {
    try (
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        WritableByteChannel channel = Channels.newChannel(out);
        ArrowStreamWriter writer = new ArrowStreamWriter(root, null, channel)
    ) {
      writer.start();
      writer.writeBatch();
      writer.end();
      return out.toByteArray();
    } catch (IOException e) {
      throw new IllegalArgumentException("Failed to serialize arrow file", e);
    }
  }

  @Test
  public void testAllTypesParquet() throws Exception {
    try (VectorSchemaRoot root = generateAllTypesVector(rootAllocator())) {
      byte[] featherData = serializeFile(root);
      try (SeekableByteChannel channel = new ByteArrayReadableSeekableByteChannel(featherData)) {
        try (ArrowStreamReader reader = new ArrowStreamReader(channel, rootAllocator())) {
          TMP.create();
          final File writtenFolder = TMP.newFolder();
          final String writtenParquet = writtenFolder.toURI().toString();
          DatasetFileWriter.write(rootAllocator(), reader, FileFormat.PARQUET,
              writtenParquet);

          // Load the reference file from the test resources and write to a temporary file on the OS.
          String referenceFile = ArrowTestDataUtil.getTestDataRoot()
              .resolve("parquet")
              .resolve("alltypes-java.parquet")
              .toUri().toString();
          assertParquetFileEquals(referenceFile,
              Objects.requireNonNull(writtenFolder.listFiles())[0].toURI().toString());
        }
      }
    }
  }
}
