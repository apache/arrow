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
package org.apache.arrow.vector.types;

import static org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE;
import static org.apache.arrow.vector.types.FloatingPointPrecision.HALF;
import static org.apache.arrow.vector.types.FloatingPointPrecision.SINGLE;
import static org.apache.arrow.vector.types.UnionMode.Dense;
import static org.apache.arrow.vector.types.UnionMode.Sparse;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.Decimal256Vector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.DurationVector;
import org.apache.arrow.vector.ExtensionTypeVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.Float2Vector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.IntervalDayVector;
import org.apache.arrow.vector.IntervalMonthDayNanoVector;
import org.apache.arrow.vector.IntervalYearVector;
import org.apache.arrow.vector.LargeVarBinaryVector;
import org.apache.arrow.vector.LargeVarCharVector;
import org.apache.arrow.vector.NullVector;
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
import org.apache.arrow.vector.UInt1Vector;
import org.apache.arrow.vector.UInt2Vector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.ViewVarBinaryVector;
import org.apache.arrow.vector.ViewVarCharVector;
import org.apache.arrow.vector.complex.DenseUnionVector;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.LargeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.ListViewVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.UnionVector;
import org.apache.arrow.vector.complex.impl.BigIntWriterImpl;
import org.apache.arrow.vector.complex.impl.BitWriterImpl;
import org.apache.arrow.vector.complex.impl.DateDayWriterImpl;
import org.apache.arrow.vector.complex.impl.DateMilliWriterImpl;
import org.apache.arrow.vector.complex.impl.Decimal256WriterImpl;
import org.apache.arrow.vector.complex.impl.DecimalWriterImpl;
import org.apache.arrow.vector.complex.impl.DenseUnionWriter;
import org.apache.arrow.vector.complex.impl.DurationWriterImpl;
import org.apache.arrow.vector.complex.impl.FixedSizeBinaryWriterImpl;
import org.apache.arrow.vector.complex.impl.Float2WriterImpl;
import org.apache.arrow.vector.complex.impl.Float4WriterImpl;
import org.apache.arrow.vector.complex.impl.Float8WriterImpl;
import org.apache.arrow.vector.complex.impl.IntWriterImpl;
import org.apache.arrow.vector.complex.impl.IntervalDayWriterImpl;
import org.apache.arrow.vector.complex.impl.IntervalMonthDayNanoWriterImpl;
import org.apache.arrow.vector.complex.impl.IntervalYearWriterImpl;
import org.apache.arrow.vector.complex.impl.LargeVarBinaryWriterImpl;
import org.apache.arrow.vector.complex.impl.LargeVarCharWriterImpl;
import org.apache.arrow.vector.complex.impl.NullableStructWriter;
import org.apache.arrow.vector.complex.impl.SmallIntWriterImpl;
import org.apache.arrow.vector.complex.impl.TimeMicroWriterImpl;
import org.apache.arrow.vector.complex.impl.TimeMilliWriterImpl;
import org.apache.arrow.vector.complex.impl.TimeNanoWriterImpl;
import org.apache.arrow.vector.complex.impl.TimeSecWriterImpl;
import org.apache.arrow.vector.complex.impl.TimeStampMicroTZWriterImpl;
import org.apache.arrow.vector.complex.impl.TimeStampMicroWriterImpl;
import org.apache.arrow.vector.complex.impl.TimeStampMilliTZWriterImpl;
import org.apache.arrow.vector.complex.impl.TimeStampMilliWriterImpl;
import org.apache.arrow.vector.complex.impl.TimeStampNanoTZWriterImpl;
import org.apache.arrow.vector.complex.impl.TimeStampNanoWriterImpl;
import org.apache.arrow.vector.complex.impl.TimeStampSecTZWriterImpl;
import org.apache.arrow.vector.complex.impl.TimeStampSecWriterImpl;
import org.apache.arrow.vector.complex.impl.TinyIntWriterImpl;
import org.apache.arrow.vector.complex.impl.UInt1WriterImpl;
import org.apache.arrow.vector.complex.impl.UInt2WriterImpl;
import org.apache.arrow.vector.complex.impl.UInt4WriterImpl;
import org.apache.arrow.vector.complex.impl.UInt8WriterImpl;
import org.apache.arrow.vector.complex.impl.UnionLargeListWriter;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.complex.impl.UnionWriter;
import org.apache.arrow.vector.complex.impl.VarBinaryWriterImpl;
import org.apache.arrow.vector.complex.impl.VarCharWriterImpl;
import org.apache.arrow.vector.complex.impl.ViewVarBinaryWriterImpl;
import org.apache.arrow.vector.complex.impl.ViewVarCharWriterImpl;
import org.apache.arrow.vector.complex.writer.FieldWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.ArrowType.ArrowTypeVisitor;
import org.apache.arrow.vector.types.pojo.ArrowType.Binary;
import org.apache.arrow.vector.types.pojo.ArrowType.BinaryView;
import org.apache.arrow.vector.types.pojo.ArrowType.Bool;
import org.apache.arrow.vector.types.pojo.ArrowType.Date;
import org.apache.arrow.vector.types.pojo.ArrowType.Decimal;
import org.apache.arrow.vector.types.pojo.ArrowType.Duration;
import org.apache.arrow.vector.types.pojo.ArrowType.ExtensionType;
import org.apache.arrow.vector.types.pojo.ArrowType.FixedSizeBinary;
import org.apache.arrow.vector.types.pojo.ArrowType.FixedSizeList;
import org.apache.arrow.vector.types.pojo.ArrowType.FloatingPoint;
import org.apache.arrow.vector.types.pojo.ArrowType.Int;
import org.apache.arrow.vector.types.pojo.ArrowType.Interval;
import org.apache.arrow.vector.types.pojo.ArrowType.LargeBinary;
import org.apache.arrow.vector.types.pojo.ArrowType.LargeUtf8;
import org.apache.arrow.vector.types.pojo.ArrowType.List;
import org.apache.arrow.vector.types.pojo.ArrowType.ListView;
import org.apache.arrow.vector.types.pojo.ArrowType.Map;
import org.apache.arrow.vector.types.pojo.ArrowType.Null;
import org.apache.arrow.vector.types.pojo.ArrowType.Struct;
import org.apache.arrow.vector.types.pojo.ArrowType.Time;
import org.apache.arrow.vector.types.pojo.ArrowType.Timestamp;
import org.apache.arrow.vector.types.pojo.ArrowType.Union;
import org.apache.arrow.vector.types.pojo.ArrowType.Utf8;
import org.apache.arrow.vector.types.pojo.ArrowType.Utf8View;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.CallBack;

/** An enumeration of all logical types supported by this library. */
public class Types {

  /** The actual enumeration of types. */
  public enum MinorType {
    NULL(Null.INSTANCE) {
      @Override
      public FieldVector getNewVector(
          Field field, BufferAllocator allocator, CallBack schemaChangeCallback) {
        return new NullVector(field.getName());
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        return null;
      }
    },
    STRUCT(Struct.INSTANCE) {
      @Override
      public FieldVector getNewVector(
          Field field, BufferAllocator allocator, CallBack schemaChangeCallback) {
        return new StructVector(
            field.getName(), allocator, field.getFieldType(), schemaChangeCallback);
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        return new NullableStructWriter((StructVector) vector);
      }
    },
    TINYINT(new Int(8, true)) {
      @Override
      public FieldVector getNewVector(
          Field field, BufferAllocator allocator, CallBack schemaChangeCallback) {
        return new TinyIntVector(field, allocator);
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        return new TinyIntWriterImpl((TinyIntVector) vector);
      }
    },
    SMALLINT(new Int(16, true)) {
      @Override
      public FieldVector getNewVector(
          Field field, BufferAllocator allocator, CallBack schemaChangeCallback) {
        return new SmallIntVector(field, allocator);
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        return new SmallIntWriterImpl((SmallIntVector) vector);
      }
    },
    INT(new Int(32, true)) {
      @Override
      public FieldVector getNewVector(
          Field field, BufferAllocator allocator, CallBack schemaChangeCallback) {
        return new IntVector(field, allocator);
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        return new IntWriterImpl((IntVector) vector);
      }
    },
    BIGINT(new Int(64, true)) {
      @Override
      public FieldVector getNewVector(
          Field field, BufferAllocator allocator, CallBack schemaChangeCallback) {
        return new BigIntVector(field, allocator);
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        return new BigIntWriterImpl((BigIntVector) vector);
      }
    },
    DATEDAY(new Date(DateUnit.DAY)) {
      @Override
      public FieldVector getNewVector(
          Field field, BufferAllocator allocator, CallBack schemaChangeCallback) {
        return new DateDayVector(field, allocator);
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        return new DateDayWriterImpl((DateDayVector) vector);
      }
    },
    DATEMILLI(new Date(DateUnit.MILLISECOND)) {
      @Override
      public FieldVector getNewVector(
          Field field, BufferAllocator allocator, CallBack schemaChangeCallback) {
        return new DateMilliVector(field, allocator);
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        return new DateMilliWriterImpl((DateMilliVector) vector);
      }
    },
    TIMESEC(new Time(TimeUnit.SECOND, 32)) {
      @Override
      public FieldVector getNewVector(
          Field field, BufferAllocator allocator, CallBack schemaChangeCallback) {
        return new TimeSecVector(field, allocator);
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        return new TimeSecWriterImpl((TimeSecVector) vector);
      }
    },
    TIMEMILLI(new Time(TimeUnit.MILLISECOND, 32)) {
      @Override
      public FieldVector getNewVector(
          Field field, BufferAllocator allocator, CallBack schemaChangeCallback) {
        return new TimeMilliVector(field, allocator);
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        return new TimeMilliWriterImpl((TimeMilliVector) vector);
      }
    },
    TIMEMICRO(new Time(TimeUnit.MICROSECOND, 64)) {
      @Override
      public FieldVector getNewVector(
          Field field, BufferAllocator allocator, CallBack schemaChangeCallback) {
        return new TimeMicroVector(field, allocator);
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        return new TimeMicroWriterImpl((TimeMicroVector) vector);
      }
    },
    TIMENANO(new Time(TimeUnit.NANOSECOND, 64)) {
      @Override
      public FieldVector getNewVector(
          Field field, BufferAllocator allocator, CallBack schemaChangeCallback) {
        return new TimeNanoVector(field, allocator);
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        return new TimeNanoWriterImpl((TimeNanoVector) vector);
      }
    },
    // time in second from the Unix epoch, 00:00:00 on 1 January 1970, UTC.
    TIMESTAMPSEC(new Timestamp(org.apache.arrow.vector.types.TimeUnit.SECOND, null)) {
      @Override
      public FieldVector getNewVector(
          Field field, BufferAllocator allocator, CallBack schemaChangeCallback) {
        return new TimeStampSecVector(field, allocator);
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        return new TimeStampSecWriterImpl((TimeStampSecVector) vector);
      }
    },
    // time in millis from the Unix epoch, 00:00:00.000 on 1 January 1970, UTC.
    TIMESTAMPMILLI(new Timestamp(org.apache.arrow.vector.types.TimeUnit.MILLISECOND, null)) {
      @Override
      public FieldVector getNewVector(
          Field field, BufferAllocator allocator, CallBack schemaChangeCallback) {
        return new TimeStampMilliVector(field, allocator);
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        return new TimeStampMilliWriterImpl((TimeStampMilliVector) vector);
      }
    },
    // time in microsecond from the Unix epoch, 00:00:00.000000 on 1 January 1970, UTC.
    TIMESTAMPMICRO(new Timestamp(org.apache.arrow.vector.types.TimeUnit.MICROSECOND, null)) {
      @Override
      public FieldVector getNewVector(
          Field field, BufferAllocator allocator, CallBack schemaChangeCallback) {
        return new TimeStampMicroVector(field, allocator);
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        return new TimeStampMicroWriterImpl((TimeStampMicroVector) vector);
      }
    },
    // time in nanosecond from the Unix epoch, 00:00:00.000000000 on 1 January 1970, UTC.
    TIMESTAMPNANO(new Timestamp(org.apache.arrow.vector.types.TimeUnit.NANOSECOND, null)) {
      @Override
      public FieldVector getNewVector(
          Field field, BufferAllocator allocator, CallBack schemaChangeCallback) {
        return new TimeStampNanoVector(field, allocator);
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        return new TimeStampNanoWriterImpl((TimeStampNanoVector) vector);
      }
    },
    INTERVALDAY(new Interval(IntervalUnit.DAY_TIME)) {
      @Override
      public FieldVector getNewVector(
          Field field, BufferAllocator allocator, CallBack schemaChangeCallback) {
        return new IntervalDayVector(field, allocator);
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        return new IntervalDayWriterImpl((IntervalDayVector) vector);
      }
    },
    INTERVALMONTHDAYNANO(new Interval(IntervalUnit.MONTH_DAY_NANO)) {
      @Override
      public FieldVector getNewVector(
          Field field, BufferAllocator allocator, CallBack schemaChangeCallback) {
        return new IntervalMonthDayNanoVector(field, allocator);
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        return new IntervalMonthDayNanoWriterImpl((IntervalMonthDayNanoVector) vector);
      }
    },
    DURATION(null) {
      @Override
      public FieldVector getNewVector(
          Field field, BufferAllocator allocator, CallBack schemaChangeCallback) {
        return new DurationVector(field, allocator);
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        return new DurationWriterImpl((DurationVector) vector);
      }
    },

    INTERVALYEAR(new Interval(IntervalUnit.YEAR_MONTH)) {
      @Override
      public FieldVector getNewVector(
          Field field, BufferAllocator allocator, CallBack schemaChangeCallback) {
        return new IntervalYearVector(field, allocator);
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        return new IntervalYearWriterImpl((IntervalYearVector) vector);
      }
    },
    FLOAT2(new FloatingPoint(HALF)) {
      @Override
      public FieldVector getNewVector(
          Field field, BufferAllocator allocator, CallBack schemaChangeCallback) {
        return new Float2Vector(field, allocator);
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        return new Float2WriterImpl((Float2Vector) vector);
      }
    },
    //  4 byte ieee 754
    FLOAT4(new FloatingPoint(SINGLE)) {
      @Override
      public FieldVector getNewVector(
          Field field, BufferAllocator allocator, CallBack schemaChangeCallback) {
        return new Float4Vector(field, allocator);
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        return new Float4WriterImpl((Float4Vector) vector);
      }
    },
    //  8 byte ieee 754
    FLOAT8(new FloatingPoint(DOUBLE)) {
      @Override
      public FieldVector getNewVector(
          Field field, BufferAllocator allocator, CallBack schemaChangeCallback) {
        return new Float8Vector(field, allocator);
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        return new Float8WriterImpl((Float8Vector) vector);
      }
    },
    BIT(Bool.INSTANCE) {
      @Override
      public FieldVector getNewVector(
          Field field, BufferAllocator allocator, CallBack schemaChangeCallback) {
        return new BitVector(field, allocator);
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        return new BitWriterImpl((BitVector) vector);
      }
    },
    VARCHAR(Utf8.INSTANCE) {
      @Override
      public FieldVector getNewVector(
          Field field, BufferAllocator allocator, CallBack schemaChangeCallback) {
        return new VarCharVector(field, allocator);
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        return new VarCharWriterImpl((VarCharVector) vector);
      }
    },
    VIEWVARCHAR(Utf8View.INSTANCE) {
      @Override
      public FieldVector getNewVector(
          Field field, BufferAllocator allocator, CallBack schemaChangeCallback) {
        return new ViewVarCharVector(field, allocator);
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        return new ViewVarCharWriterImpl((ViewVarCharVector) vector);
      }
    },
    LARGEVARCHAR(LargeUtf8.INSTANCE) {
      @Override
      public FieldVector getNewVector(
          Field field, BufferAllocator allocator, CallBack schemaChangeCallback) {
        return new LargeVarCharVector(field, allocator);
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        return new LargeVarCharWriterImpl((LargeVarCharVector) vector);
      }
    },
    LARGEVARBINARY(LargeBinary.INSTANCE) {
      @Override
      public FieldVector getNewVector(
          Field field, BufferAllocator allocator, CallBack schemaChangeCallback) {
        return new LargeVarBinaryVector(field, allocator);
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        return new LargeVarBinaryWriterImpl((LargeVarBinaryVector) vector);
      }
    },
    VARBINARY(Binary.INSTANCE) {
      @Override
      public FieldVector getNewVector(
          Field field, BufferAllocator allocator, CallBack schemaChangeCallback) {
        return new VarBinaryVector(field, allocator);
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        return new VarBinaryWriterImpl((VarBinaryVector) vector);
      }
    },
    VIEWVARBINARY(BinaryView.INSTANCE) {
      @Override
      public FieldVector getNewVector(
          Field field, BufferAllocator allocator, CallBack schemaChangeCallback) {
        return new ViewVarBinaryVector(field, allocator);
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        return new ViewVarBinaryWriterImpl((ViewVarBinaryVector) vector);
      }
    },
    DECIMAL(null) {
      @Override
      public FieldVector getNewVector(
          Field field, BufferAllocator allocator, CallBack schemaChangeCallback) {
        return new DecimalVector(field, allocator);
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        return new DecimalWriterImpl((DecimalVector) vector);
      }
    },
    DECIMAL256(null) {
      @Override
      public FieldVector getNewVector(
          Field field, BufferAllocator allocator, CallBack schemaChangeCallback) {
        return new Decimal256Vector(field, allocator);
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        return new Decimal256WriterImpl((Decimal256Vector) vector);
      }
    },
    FIXEDSIZEBINARY(null) {
      @Override
      public FieldVector getNewVector(
          Field field, BufferAllocator allocator, CallBack schemaChangeCallback) {
        return new FixedSizeBinaryVector(field, allocator);
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        return new FixedSizeBinaryWriterImpl((FixedSizeBinaryVector) vector);
      }
    },
    UINT1(new Int(8, false)) {
      @Override
      public FieldVector getNewVector(
          Field field, BufferAllocator allocator, CallBack schemaChangeCallback) {
        return new UInt1Vector(field, allocator);
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        return new UInt1WriterImpl((UInt1Vector) vector);
      }
    },
    UINT2(new Int(16, false)) {
      @Override
      public FieldVector getNewVector(
          Field field, BufferAllocator allocator, CallBack schemaChangeCallback) {
        return new UInt2Vector(field, allocator);
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        return new UInt2WriterImpl((UInt2Vector) vector);
      }
    },
    UINT4(new Int(32, false)) {
      @Override
      public FieldVector getNewVector(
          Field field, BufferAllocator allocator, CallBack schemaChangeCallback) {
        return new UInt4Vector(field, allocator);
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        return new UInt4WriterImpl((UInt4Vector) vector);
      }
    },
    UINT8(new Int(64, false)) {
      @Override
      public FieldVector getNewVector(
          Field field, BufferAllocator allocator, CallBack schemaChangeCallback) {
        return new UInt8Vector(field, allocator);
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        return new UInt8WriterImpl((UInt8Vector) vector);
      }
    },
    LIST(List.INSTANCE) {
      @Override
      public FieldVector getNewVector(
          Field field, BufferAllocator allocator, CallBack schemaChangeCallback) {
        return new ListVector(
            field.getName(), allocator, field.getFieldType(), schemaChangeCallback);
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        return new UnionListWriter((ListVector) vector);
      }
    },
    LISTVIEW(ListView.INSTANCE) {
      @Override
      public FieldVector getNewVector(
          Field field, BufferAllocator allocator, CallBack schemaChangeCallback) {
        return new ListViewVector(
            field.getName(), allocator, field.getFieldType(), schemaChangeCallback);
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        return new UnionListWriter((ListVector) vector);
      }
    },
    LARGELIST(ArrowType.LargeList.INSTANCE) {
      @Override
      public FieldVector getNewVector(
          Field field, BufferAllocator allocator, CallBack schemaChangeCallback) {
        return new LargeListVector(
            field.getName(), allocator, field.getFieldType(), schemaChangeCallback);
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        return new UnionLargeListWriter((LargeListVector) vector);
      }
    },
    FIXED_SIZE_LIST(null) {
      @Override
      public FieldVector getNewVector(
          Field field, BufferAllocator allocator, CallBack schemaChangeCallback) {
        return new FixedSizeListVector(
            field.getName(), allocator, field.getFieldType(), schemaChangeCallback);
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        throw new UnsupportedOperationException(
            "FieldWriter not implemented for FixedSizeList " + "type");
      }
    },
    UNION(new Union(Sparse, null)) {
      @Override
      public FieldVector getNewVector(
          Field field, BufferAllocator allocator, CallBack schemaChangeCallback) {
        if (field.getFieldType().getDictionary() != null) {
          throw new UnsupportedOperationException(
              "Dictionary encoding not supported for complex " + "types");
        }
        return new UnionVector(
            field.getName(), allocator, field.getFieldType(), schemaChangeCallback);
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        return new UnionWriter((UnionVector) vector);
      }
    },
    DENSEUNION(new Union(Dense, null)) {
      @Override
      public FieldVector getNewVector(
          Field field, BufferAllocator allocator, CallBack schemaChangeCallback) {
        if (field.getFieldType().getDictionary() != null) {
          throw new UnsupportedOperationException(
              "Dictionary encoding not supported for complex " + "types");
        }
        return new DenseUnionVector(
            field.getName(), allocator, field.getFieldType(), schemaChangeCallback);
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        return new DenseUnionWriter((DenseUnionVector) vector);
      }
    },
    MAP(null) {
      @Override
      public FieldVector getNewVector(
          Field field, BufferAllocator allocator, CallBack schemaChangeCallback) {
        return new MapVector(
            field.getName(), allocator, field.getFieldType(), schemaChangeCallback);
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        return new UnionListWriter((MapVector) vector);
      }
    },
    TIMESTAMPSECTZ(null) {
      @Override
      public FieldVector getNewVector(
          Field field, BufferAllocator allocator, CallBack schemaChangeCallback) {
        return new TimeStampSecTZVector(field, allocator);
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        return new TimeStampSecTZWriterImpl((TimeStampSecTZVector) vector);
      }
    },
    TIMESTAMPMILLITZ(null) {
      @Override
      public FieldVector getNewVector(
          Field field, BufferAllocator allocator, CallBack schemaChangeCallback) {
        return new TimeStampMilliTZVector(field, allocator);
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        return new TimeStampMilliTZWriterImpl((TimeStampMilliTZVector) vector);
      }
    },
    TIMESTAMPMICROTZ(null) {
      @Override
      public FieldVector getNewVector(
          Field field, BufferAllocator allocator, CallBack schemaChangeCallback) {
        return new TimeStampMicroTZVector(field, allocator);
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        return new TimeStampMicroTZWriterImpl((TimeStampMicroTZVector) vector);
      }
    },
    TIMESTAMPNANOTZ(null) {
      @Override
      public FieldVector getNewVector(
          Field field, BufferAllocator allocator, CallBack schemaChangeCallback) {
        return new TimeStampNanoTZVector(field, allocator);
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        return new TimeStampNanoTZWriterImpl((TimeStampNanoTZVector) vector);
      }
    },
    EXTENSIONTYPE(null) {
      @Override
      public FieldVector getNewVector(
          Field field, BufferAllocator allocator, CallBack schemaChangeCallback) {
        return ((ExtensionType) field.getFieldType().getType())
            .getNewVector(field.getName(), field.getFieldType(), allocator);
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        return ((ExtensionTypeVector) vector)
            .getUnderlyingVector()
            .getMinorType()
            .getNewFieldWriter(vector);
      }
    },
    ;

    private final ArrowType type;

    MinorType(ArrowType type) {
      this.type = type;
    }

    /** Returns the {@link ArrowType} equivalent of this type. */
    public final ArrowType getType() {
      if (type == null) {
        throw new UnsupportedOperationException("Cannot get simple type for type " + name());
      }
      return type;
    }

    /** Constructs a new vector for the given type. */
    public final FieldVector getNewVector(
        String name,
        FieldType fieldType,
        BufferAllocator allocator,
        CallBack schemaChangeCallback) {
      return getNewVector(new Field(name, fieldType, null), allocator, schemaChangeCallback);
    }

    /** Constructs a new vector for the given type. */
    public abstract FieldVector getNewVector(
        Field field, BufferAllocator allocator, CallBack schemaChangeCallback);

    public abstract FieldWriter getNewFieldWriter(ValueVector vector);
  }

  /** Maps the ArrowType to the java implementations MinorType. */
  public static MinorType getMinorTypeForArrowType(ArrowType arrowType) {
    return arrowType.accept(
        new ArrowTypeVisitor<MinorType>() {
          @Override
          public MinorType visit(Null type) {
            return MinorType.NULL;
          }

          @Override
          public MinorType visit(Struct type) {
            return MinorType.STRUCT;
          }

          @Override
          public MinorType visit(List type) {
            return MinorType.LIST;
          }

          @Override
          public MinorType visit(FixedSizeList type) {
            return MinorType.FIXED_SIZE_LIST;
          }

          @Override
          public MinorType visit(Union type) {
            switch (type.getMode()) {
              case Sparse:
                return MinorType.UNION;
              case Dense:
                return MinorType.DENSEUNION;
              default:
                throw new IllegalArgumentException(
                    "only Dense or Sparse unions supported: " + type);
            }
          }

          @Override
          public MinorType visit(Map type) {
            return MinorType.MAP;
          }

          @Override
          public MinorType visit(ArrowType.LargeList type) {
            return MinorType.LARGELIST;
          }

          @Override
          public MinorType visit(Int type) {
            switch (type.getBitWidth()) {
              case 8:
                return type.getIsSigned() ? MinorType.TINYINT : MinorType.UINT1;
              case 16:
                return type.getIsSigned() ? MinorType.SMALLINT : MinorType.UINT2;
              case 32:
                return type.getIsSigned() ? MinorType.INT : MinorType.UINT4;
              case 64:
                return type.getIsSigned() ? MinorType.BIGINT : MinorType.UINT8;
              default:
                throw new IllegalArgumentException("only 8, 16, 32, 64 supported: " + type);
            }
          }

          @Override
          public MinorType visit(FloatingPoint type) {
            switch (type.getPrecision()) {
              case HALF:
                return MinorType.FLOAT2;
              case SINGLE:
                return MinorType.FLOAT4;
              case DOUBLE:
                return MinorType.FLOAT8;
              default:
                throw new IllegalArgumentException("unknown precision: " + type);
            }
          }

          @Override
          public MinorType visit(Utf8 type) {
            return MinorType.VARCHAR;
          }

          @Override
          public MinorType visit(Utf8View type) {
            return MinorType.VIEWVARCHAR;
          }

          @Override
          public Types.MinorType visit(LargeUtf8 type) {
            return MinorType.LARGEVARCHAR;
          }

          @Override
          public MinorType visit(Binary type) {
            return MinorType.VARBINARY;
          }

          @Override
          public MinorType visit(BinaryView type) {
            return MinorType.VIEWVARBINARY;
          }

          @Override
          public MinorType visit(LargeBinary type) {
            return MinorType.LARGEVARBINARY;
          }

          @Override
          public MinorType visit(Bool type) {
            return MinorType.BIT;
          }

          @Override
          public MinorType visit(Decimal type) {
            if (type.getBitWidth() == 256) {
              return MinorType.DECIMAL256;
            }
            return MinorType.DECIMAL;
          }

          @Override
          public MinorType visit(FixedSizeBinary type) {
            return MinorType.FIXEDSIZEBINARY;
          }

          @Override
          public MinorType visit(Date type) {
            switch (type.getUnit()) {
              case DAY:
                return MinorType.DATEDAY;
              case MILLISECOND:
                return MinorType.DATEMILLI;
              default:
                throw new IllegalArgumentException("unknown unit: " + type);
            }
          }

          @Override
          public MinorType visit(Time type) {
            switch (type.getUnit()) {
              case SECOND:
                return MinorType.TIMESEC;
              case MILLISECOND:
                return MinorType.TIMEMILLI;
              case MICROSECOND:
                return MinorType.TIMEMICRO;
              case NANOSECOND:
                return MinorType.TIMENANO;
              default:
                throw new IllegalArgumentException("unknown unit: " + type);
            }
          }

          @Override
          public MinorType visit(Timestamp type) {
            String tz = type.getTimezone();
            switch (type.getUnit()) {
              case SECOND:
                return tz == null ? MinorType.TIMESTAMPSEC : MinorType.TIMESTAMPSECTZ;
              case MILLISECOND:
                return tz == null ? MinorType.TIMESTAMPMILLI : MinorType.TIMESTAMPMILLITZ;
              case MICROSECOND:
                return tz == null ? MinorType.TIMESTAMPMICRO : MinorType.TIMESTAMPMICROTZ;
              case NANOSECOND:
                return tz == null ? MinorType.TIMESTAMPNANO : MinorType.TIMESTAMPNANOTZ;
              default:
                throw new IllegalArgumentException("unknown unit: " + type);
            }
          }

          @Override
          public MinorType visit(Interval type) {
            switch (type.getUnit()) {
              case DAY_TIME:
                return MinorType.INTERVALDAY;
              case YEAR_MONTH:
                return MinorType.INTERVALYEAR;
              case MONTH_DAY_NANO:
                return MinorType.INTERVALMONTHDAYNANO;
              default:
                throw new IllegalArgumentException("unknown unit: " + type);
            }
          }

          @Override
          public MinorType visit(Duration type) {
            return MinorType.DURATION;
          }

          @Override
          public MinorType visit(ListView type) {
            return MinorType.LISTVIEW;
          }

          @Override
          public MinorType visit(ExtensionType type) {
            return MinorType.EXTENSIONTYPE;
          }
        });
  }
}
