/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.arrow.vector.types;

import static org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE;
import static org.apache.arrow.vector.types.FloatingPointPrecision.SINGLE;
import static org.apache.arrow.vector.types.UnionMode.Sparse;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.NullableBigIntVector;
import org.apache.arrow.vector.NullableBitVector;
import org.apache.arrow.vector.NullableDateDayVector;
import org.apache.arrow.vector.NullableDateMilliVector;
import org.apache.arrow.vector.NullableDecimalVector;
import org.apache.arrow.vector.NullableFloat4Vector;
import org.apache.arrow.vector.NullableFloat8Vector;
import org.apache.arrow.vector.NullableIntVector;
import org.apache.arrow.vector.NullableIntervalDayVector;
import org.apache.arrow.vector.NullableIntervalYearVector;
import org.apache.arrow.vector.NullableSmallIntVector;
import org.apache.arrow.vector.NullableTimeMicroVector;
import org.apache.arrow.vector.NullableTimeMilliVector;
import org.apache.arrow.vector.NullableTimeNanoVector;
import org.apache.arrow.vector.NullableTimeSecVector;
import org.apache.arrow.vector.NullableTimeStampMicroVector;
import org.apache.arrow.vector.NullableTimeStampMilliVector;
import org.apache.arrow.vector.NullableTimeStampNanoVector;
import org.apache.arrow.vector.NullableTimeStampSecVector;
import org.apache.arrow.vector.NullableTinyIntVector;
import org.apache.arrow.vector.NullableUInt1Vector;
import org.apache.arrow.vector.NullableUInt2Vector;
import org.apache.arrow.vector.NullableUInt4Vector;
import org.apache.arrow.vector.NullableUInt8Vector;
import org.apache.arrow.vector.NullableVarBinaryVector;
import org.apache.arrow.vector.NullableVarCharVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.ZeroVector;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.NullableMapVector;
import org.apache.arrow.vector.complex.UnionVector;
import org.apache.arrow.vector.complex.impl.BigIntWriterImpl;
import org.apache.arrow.vector.complex.impl.BitWriterImpl;
import org.apache.arrow.vector.complex.impl.DateDayWriterImpl;
import org.apache.arrow.vector.complex.impl.DateMilliWriterImpl;
import org.apache.arrow.vector.complex.impl.DecimalWriterImpl;
import org.apache.arrow.vector.complex.impl.Float4WriterImpl;
import org.apache.arrow.vector.complex.impl.Float8WriterImpl;
import org.apache.arrow.vector.complex.impl.IntWriterImpl;
import org.apache.arrow.vector.complex.impl.IntervalDayWriterImpl;
import org.apache.arrow.vector.complex.impl.IntervalYearWriterImpl;
import org.apache.arrow.vector.complex.impl.NullableMapWriter;
import org.apache.arrow.vector.complex.impl.SmallIntWriterImpl;
import org.apache.arrow.vector.complex.impl.TimeMicroWriterImpl;
import org.apache.arrow.vector.complex.impl.TimeMilliWriterImpl;
import org.apache.arrow.vector.complex.impl.TimeNanoWriterImpl;
import org.apache.arrow.vector.complex.impl.TimeSecWriterImpl;
import org.apache.arrow.vector.complex.impl.TimeStampMicroWriterImpl;
import org.apache.arrow.vector.complex.impl.TimeStampMilliWriterImpl;
import org.apache.arrow.vector.complex.impl.TimeStampNanoWriterImpl;
import org.apache.arrow.vector.complex.impl.TimeStampSecWriterImpl;
import org.apache.arrow.vector.complex.impl.TinyIntWriterImpl;
import org.apache.arrow.vector.complex.impl.UInt1WriterImpl;
import org.apache.arrow.vector.complex.impl.UInt2WriterImpl;
import org.apache.arrow.vector.complex.impl.UInt4WriterImpl;
import org.apache.arrow.vector.complex.impl.UInt8WriterImpl;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.complex.impl.UnionWriter;
import org.apache.arrow.vector.complex.impl.VarBinaryWriterImpl;
import org.apache.arrow.vector.complex.impl.VarCharWriterImpl;
import org.apache.arrow.vector.complex.writer.FieldWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.ArrowType.ArrowTypeVisitor;
import org.apache.arrow.vector.types.pojo.ArrowType.Binary;
import org.apache.arrow.vector.types.pojo.ArrowType.Bool;
import org.apache.arrow.vector.types.pojo.ArrowType.Date;
import org.apache.arrow.vector.types.pojo.ArrowType.Decimal;
import org.apache.arrow.vector.types.pojo.ArrowType.FixedSizeList;
import org.apache.arrow.vector.types.pojo.ArrowType.FloatingPoint;
import org.apache.arrow.vector.types.pojo.ArrowType.Int;
import org.apache.arrow.vector.types.pojo.ArrowType.Interval;
import org.apache.arrow.vector.types.pojo.ArrowType.List;
import org.apache.arrow.vector.types.pojo.ArrowType.Null;
import org.apache.arrow.vector.types.pojo.ArrowType.Struct;
import org.apache.arrow.vector.types.pojo.ArrowType.Time;
import org.apache.arrow.vector.types.pojo.ArrowType.Timestamp;
import org.apache.arrow.vector.types.pojo.ArrowType.Union;
import org.apache.arrow.vector.types.pojo.ArrowType.Utf8;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.CallBack;

public class Types {

  public enum MinorType {
    NULL(Null.INSTANCE) {
      @Override
      public FieldVector getNewVector(String name, FieldType fieldType, BufferAllocator allocator, CallBack schemaChangeCallback) {
        return ZeroVector.INSTANCE;
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        return null;
      }
    },
    MAP(Struct.INSTANCE) {
      @Override
      public FieldVector getNewVector(String name, FieldType fieldType, BufferAllocator allocator, CallBack schemaChangeCallback) {
        return new NullableMapVector(name, allocator, fieldType, schemaChangeCallback);
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        return new NullableMapWriter((NullableMapVector) vector);
      }
    },
    TINYINT(new Int(8, true)) {
      @Override
      public FieldVector getNewVector(String name, FieldType fieldType, BufferAllocator allocator, CallBack schemaChangeCallback) {
        return new NullableTinyIntVector(name, fieldType, allocator);
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        return new TinyIntWriterImpl((NullableTinyIntVector) vector);
      }
    },
    SMALLINT(new Int(16, true)) {
      @Override
      public FieldVector getNewVector(String name, FieldType fieldType, BufferAllocator allocator, CallBack schemaChangeCallback) {
        return new NullableSmallIntVector(name, fieldType, allocator);
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        return new SmallIntWriterImpl((NullableSmallIntVector) vector);
      }
    },
    INT(new Int(32, true)) {
      @Override
      public FieldVector getNewVector(String name, FieldType fieldType, BufferAllocator allocator, CallBack schemaChangeCallback) {
        return new NullableIntVector(name, fieldType, allocator);
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        return new IntWriterImpl((NullableIntVector) vector);
      }
    },
    BIGINT(new Int(64, true)) {
      @Override
      public FieldVector getNewVector(String name, FieldType fieldType, BufferAllocator allocator, CallBack schemaChangeCallback) {
        return new NullableBigIntVector(name, fieldType, allocator);
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        return new BigIntWriterImpl((NullableBigIntVector) vector);
      }
    },
    DATEDAY(new Date(DateUnit.DAY)) {
      @Override
      public FieldVector getNewVector(String name, FieldType fieldType, BufferAllocator allocator, CallBack schemaChangeCallback) {
        return new NullableDateDayVector(name, fieldType, allocator);
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        return new DateDayWriterImpl((NullableDateDayVector) vector);
      }
    },
    DATEMILLI(new Date(DateUnit.MILLISECOND)) {
      @Override
      public FieldVector getNewVector(String name, FieldType fieldType, BufferAllocator allocator, CallBack schemaChangeCallback) {
        return new NullableDateMilliVector(name, fieldType, allocator);
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        return new DateMilliWriterImpl((NullableDateMilliVector) vector);
      }
    },
    TIMESEC(new Time(TimeUnit.SECOND, 32)) {
      @Override
      public FieldVector getNewVector(String name, FieldType fieldType, BufferAllocator allocator, CallBack schemaChangeCallback) {
        return new NullableTimeSecVector(name, fieldType, allocator);
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        return new TimeSecWriterImpl((NullableTimeSecVector) vector);
      }
    },
    TIMEMILLI(new Time(TimeUnit.MILLISECOND, 32)) {
      @Override
      public FieldVector getNewVector(String name, FieldType fieldType, BufferAllocator allocator, CallBack schemaChangeCallback) {
        return new NullableTimeMilliVector(name, fieldType, allocator);
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        return new TimeMilliWriterImpl((NullableTimeMilliVector) vector);
      }
    },
    TIMEMICRO(new Time(TimeUnit.MICROSECOND, 64)) {
      @Override
      public FieldVector getNewVector(String name, FieldType fieldType, BufferAllocator allocator, CallBack schemaChangeCallback) {
        return new NullableTimeMicroVector(name, fieldType, allocator);
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        return new TimeMicroWriterImpl((NullableTimeMicroVector) vector);
      }
    },
    TIMENANO(new Time(TimeUnit.NANOSECOND, 64)) {
      @Override
      public FieldVector getNewVector(String name, FieldType fieldType, BufferAllocator allocator, CallBack schemaChangeCallback) {
        return new NullableTimeNanoVector(name, fieldType, allocator);
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        return new TimeNanoWriterImpl((NullableTimeNanoVector) vector);
      }
    },
    // time in second from the Unix epoch, 00:00:00.000000 on 1 January 1970, UTC.
    TIMESTAMPSEC(new Timestamp(org.apache.arrow.vector.types.TimeUnit.SECOND, null)) {
      @Override
      public FieldVector getNewVector(String name, FieldType fieldType, BufferAllocator allocator, CallBack schemaChangeCallback) {
        return new NullableTimeStampSecVector(name, fieldType, allocator);
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        return new TimeStampSecWriterImpl((NullableTimeStampSecVector) vector);
      }
    },
    // time in millis from the Unix epoch, 00:00:00.000 on 1 January 1970, UTC.
    TIMESTAMPMILLI(new Timestamp(org.apache.arrow.vector.types.TimeUnit.MILLISECOND, null)) {
      @Override
      public FieldVector getNewVector(String name, FieldType fieldType, BufferAllocator allocator, CallBack schemaChangeCallback) {
        return new NullableTimeStampMilliVector(name, fieldType, allocator);
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        return new TimeStampMilliWriterImpl((NullableTimeStampMilliVector) vector);
      }
    },
    // time in microsecond from the Unix epoch, 00:00:00.000000 on 1 January 1970, UTC.
    TIMESTAMPMICRO(new Timestamp(org.apache.arrow.vector.types.TimeUnit.MICROSECOND, null)) {
      @Override
      public FieldVector getNewVector(String name, FieldType fieldType, BufferAllocator allocator, CallBack schemaChangeCallback) {
        return new NullableTimeStampMicroVector(name, fieldType, allocator);
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        return new TimeStampMicroWriterImpl((NullableTimeStampMicroVector) vector);
      }
    },
    // time in nanosecond from the Unix epoch, 00:00:00.000000000 on 1 January 1970, UTC.
    TIMESTAMPNANO(new Timestamp(org.apache.arrow.vector.types.TimeUnit.NANOSECOND, null)) {
      @Override
      public FieldVector getNewVector(String name, FieldType fieldType, BufferAllocator allocator, CallBack schemaChangeCallback) {
        return new NullableTimeStampNanoVector(name, fieldType, allocator);
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        return new TimeStampNanoWriterImpl((NullableTimeStampNanoVector) vector);
      }
    },
    INTERVALDAY(new Interval(IntervalUnit.DAY_TIME)) {
      @Override
      public FieldVector getNewVector(String name, FieldType fieldType, BufferAllocator allocator, CallBack schemaChangeCallback) {
        return new NullableIntervalDayVector(name, fieldType, allocator);
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        return new IntervalDayWriterImpl((NullableIntervalDayVector) vector);
      }
    },
    INTERVALYEAR(new Interval(IntervalUnit.YEAR_MONTH)) {
      @Override
      public FieldVector getNewVector(String name, FieldType fieldType, BufferAllocator allocator, CallBack schemaChangeCallback) {
        return new NullableIntervalYearVector(name, fieldType, allocator);
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        return new IntervalYearWriterImpl((NullableIntervalYearVector) vector);
      }
    },
    //  4 byte ieee 754
    FLOAT4(new FloatingPoint(SINGLE)) {
      @Override
      public FieldVector getNewVector(String name, FieldType fieldType, BufferAllocator allocator, CallBack schemaChangeCallback) {
        return new NullableFloat4Vector(name, fieldType, allocator);
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        return new Float4WriterImpl((NullableFloat4Vector) vector);
      }
    },
    //  8 byte ieee 754
    FLOAT8(new FloatingPoint(DOUBLE)) {
      @Override
      public FieldVector getNewVector(String name, FieldType fieldType, BufferAllocator allocator, CallBack schemaChangeCallback) {
        return new NullableFloat8Vector(name, fieldType, allocator);
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        return new Float8WriterImpl((NullableFloat8Vector) vector);
      }
    },
    BIT(Bool.INSTANCE) {
      @Override
      public FieldVector getNewVector(String name, FieldType fieldType, BufferAllocator allocator, CallBack schemaChangeCallback) {
        return new NullableBitVector(name, fieldType, allocator);
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        return new BitWriterImpl((NullableBitVector) vector);
      }
    },
    VARCHAR(Utf8.INSTANCE) {
      @Override
      public FieldVector getNewVector(String name, FieldType fieldType, BufferAllocator allocator, CallBack schemaChangeCallback) {
        return new NullableVarCharVector(name, fieldType, allocator);
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        return new VarCharWriterImpl((NullableVarCharVector) vector);
      }
    },
    VARBINARY(Binary.INSTANCE) {
      @Override
      public FieldVector getNewVector(String name, FieldType fieldType, BufferAllocator allocator, CallBack schemaChangeCallback) {
        return new NullableVarBinaryVector(name, fieldType, allocator);
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        return new VarBinaryWriterImpl((NullableVarBinaryVector) vector);
      }
    },
    DECIMAL(null) {
      @Override
      public ArrowType getType() {
        throw new UnsupportedOperationException("Cannot get simple type for Decimal type");
      }

      @Override
      public FieldVector getNewVector(String name, FieldType fieldType, BufferAllocator allocator, CallBack schemaChangeCallback) {
        return new NullableDecimalVector(name, fieldType, allocator);
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        return new DecimalWriterImpl((NullableDecimalVector) vector);
      }
    },
    UINT1(new Int(8, false)) {
      @Override
      public FieldVector getNewVector(String name, FieldType fieldType, BufferAllocator allocator, CallBack schemaChangeCallback) {
        return new NullableUInt1Vector(name, fieldType, allocator);
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        return new UInt1WriterImpl((NullableUInt1Vector) vector);
      }
    },
    UINT2(new Int(16, false)) {
      @Override
      public FieldVector getNewVector(String name, FieldType fieldType, BufferAllocator allocator, CallBack schemaChangeCallback) {
        return new NullableUInt2Vector(name, fieldType, allocator);
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        return new UInt2WriterImpl((NullableUInt2Vector) vector);
      }
    },
    UINT4(new Int(32, false)) {
      @Override
      public FieldVector getNewVector(String name, FieldType fieldType, BufferAllocator allocator, CallBack schemaChangeCallback) {
        return new NullableUInt4Vector(name, fieldType, allocator);
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        return new UInt4WriterImpl((NullableUInt4Vector) vector);
      }
    },
    UINT8(new Int(64, false)) {
      @Override
      public FieldVector getNewVector(String name, FieldType fieldType, BufferAllocator allocator, CallBack schemaChangeCallback) {
        return new NullableUInt8Vector(name, fieldType, allocator);
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        return new UInt8WriterImpl((NullableUInt8Vector) vector);
      }
    },
    LIST(List.INSTANCE) {
      @Override
      public FieldVector getNewVector(String name, FieldType fieldType, BufferAllocator allocator, CallBack schemaChangeCallback) {
        return new ListVector(name, allocator, fieldType, schemaChangeCallback);
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        return new UnionListWriter((ListVector) vector);
      }
    },
    FIXED_SIZE_LIST(null) {
      @Override
      public ArrowType getType() {
        throw new UnsupportedOperationException("Cannot get simple type for FixedSizeList type");
      }

      @Override
      public FieldVector getNewVector(String name, FieldType fieldType, BufferAllocator allocator, CallBack schemaChangeCallback) {
        return new FixedSizeListVector(name, allocator, fieldType, schemaChangeCallback);
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        throw new UnsupportedOperationException("FieldWriter not implemented for FixedSizeList type");
      }
    },
    UNION(new Union(Sparse, null)) {
      @Override
      public FieldVector getNewVector(String name, FieldType fieldType, BufferAllocator allocator, CallBack schemaChangeCallback) {
        if (fieldType.getDictionary() != null) {
          throw new UnsupportedOperationException("Dictionary encoding not supported for complex types");
        }
        return new UnionVector(name, allocator, schemaChangeCallback);
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        return new UnionWriter((UnionVector) vector);
      }
    };

    private final ArrowType type;

    MinorType(ArrowType type) {
      this.type = type;
    }

    public ArrowType getType() {
      return type;
    }

    public abstract FieldVector getNewVector(String name, FieldType fieldType, BufferAllocator allocator, CallBack schemaChangeCallback);

    public abstract FieldWriter getNewFieldWriter(ValueVector vector);
  }

  public static MinorType getMinorTypeForArrowType(ArrowType arrowType) {
    return arrowType.accept(new ArrowTypeVisitor<MinorType>() {
      @Override public MinorType visit(Null type) {
        return MinorType.NULL;
      }

      @Override public MinorType visit(Struct type) {
        return MinorType.MAP;
      }

      @Override public MinorType visit(List type) {
        return MinorType.LIST;
      }

      @Override public MinorType visit(FixedSizeList type) {
        return MinorType.FIXED_SIZE_LIST;
      }

      @Override public MinorType visit(Union type) {
        return MinorType.UNION;
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
          throw new UnsupportedOperationException("NYI: " + type);
        case SINGLE:
          return MinorType.FLOAT4;
        case DOUBLE:
          return MinorType.FLOAT8;
        default:
          throw new IllegalArgumentException("unknown precision: " + type);
        }
      }

      @Override public MinorType visit(Utf8 type) {
        return MinorType.VARCHAR;
      }

      @Override public MinorType visit(Binary type) {
        return MinorType.VARBINARY;
      }

      @Override public MinorType visit(Bool type) {
        return MinorType.BIT;
      }

      @Override public MinorType visit(Decimal type) {
        return MinorType.DECIMAL;
      }

      @Override public MinorType visit(Date type) {
        switch (type.getUnit()) {
          case DAY:
            return MinorType.DATEDAY;
          case MILLISECOND:
            return MinorType.DATEMILLI;
          default:
            throw new IllegalArgumentException("unknown unit: " + type);
        }
      }

      @Override public MinorType visit(Time type) {
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

      @Override public MinorType visit(Timestamp type) {
        if (type.getTimezone() != null) {
          throw new IllegalArgumentException("only timezone-less timestamps are supported for now: " + type);
        }
        switch (type.getUnit()) {
          case SECOND:
            return MinorType.TIMESTAMPSEC;
          case MILLISECOND:
            return MinorType.TIMESTAMPMILLI;
          case MICROSECOND:
            return MinorType.TIMESTAMPMICRO;
          case NANOSECOND:
            return MinorType.TIMESTAMPNANO;
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
        default:
          throw new IllegalArgumentException("unknown unit: " + type);
        }
      }
    });
  }

}
