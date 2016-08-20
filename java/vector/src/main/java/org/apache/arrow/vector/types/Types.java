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

import org.apache.arrow.flatbuf.Type;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.NullableBigIntVector;
import org.apache.arrow.vector.NullableBitVector;
import org.apache.arrow.vector.NullableDateVector;
import org.apache.arrow.vector.NullableDecimalVector;
import org.apache.arrow.vector.NullableFloat4Vector;
import org.apache.arrow.vector.NullableFloat8Vector;
import org.apache.arrow.vector.NullableIntVector;
import org.apache.arrow.vector.NullableIntervalDayVector;
import org.apache.arrow.vector.NullableIntervalYearVector;
import org.apache.arrow.vector.NullableSmallIntVector;
import org.apache.arrow.vector.NullableTimeStampVector;
import org.apache.arrow.vector.NullableTimeVector;
import org.apache.arrow.vector.NullableTinyIntVector;
import org.apache.arrow.vector.NullableUInt1Vector;
import org.apache.arrow.vector.NullableUInt2Vector;
import org.apache.arrow.vector.NullableUInt4Vector;
import org.apache.arrow.vector.NullableUInt8Vector;
import org.apache.arrow.vector.NullableVarBinaryVector;
import org.apache.arrow.vector.NullableVarCharVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.ZeroVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.UnionVector;
import org.apache.arrow.vector.complex.impl.BigIntWriterImpl;
import org.apache.arrow.vector.complex.impl.BitWriterImpl;
import org.apache.arrow.vector.complex.impl.DateWriterImpl;
import org.apache.arrow.vector.complex.impl.DecimalWriterImpl;
import org.apache.arrow.vector.complex.impl.Float4WriterImpl;
import org.apache.arrow.vector.complex.impl.Float8WriterImpl;
import org.apache.arrow.vector.complex.impl.IntWriterImpl;
import org.apache.arrow.vector.complex.impl.IntervalDayWriterImpl;
import org.apache.arrow.vector.complex.impl.IntervalYearWriterImpl;
import org.apache.arrow.vector.complex.impl.SingleMapWriter;
import org.apache.arrow.vector.complex.impl.SmallIntWriterImpl;
import org.apache.arrow.vector.complex.impl.TimeStampWriterImpl;
import org.apache.arrow.vector.complex.impl.TimeWriterImpl;
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
import org.apache.arrow.vector.types.pojo.ArrowType.Binary;
import org.apache.arrow.vector.types.pojo.ArrowType.Bool;
import org.apache.arrow.vector.types.pojo.ArrowType.Date;
import org.apache.arrow.vector.types.pojo.ArrowType.FloatingPoint;
import org.apache.arrow.vector.types.pojo.ArrowType.Int;
import org.apache.arrow.vector.types.pojo.ArrowType.IntervalDay;
import org.apache.arrow.vector.types.pojo.ArrowType.IntervalYear;
import org.apache.arrow.vector.types.pojo.ArrowType.List;
import org.apache.arrow.vector.types.pojo.ArrowType.Null;
import org.apache.arrow.vector.types.pojo.ArrowType.Time;
import org.apache.arrow.vector.types.pojo.ArrowType.Timestamp;
import org.apache.arrow.vector.types.pojo.ArrowType.Tuple;
import org.apache.arrow.vector.types.pojo.ArrowType.Union;
import org.apache.arrow.vector.types.pojo.ArrowType.Utf8;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.CallBack;

import java.util.HashMap;
import java.util.Map;

public class Types {

  public static final Field NULL_FIELD = new Field("", true, Null.INSTANCE, null);
  public static final Field TINYINT_FIELD = new Field("", true, new Int(8, true), null);
  public static final Field SMALLINT_FIELD = new Field("", true, new Int(16, true), null);
  public static final Field INT_FIELD = new Field("", true, new Int(32, true), null);
  public static final Field BIGINT_FIELD = new Field("", true, new Int(64, true), null);
  public static final Field UINT1_FIELD = new Field("", true, new Int(8, false), null);
  public static final Field UINT2_FIELD = new Field("", true, new Int(16, false), null);
  public static final Field UINT4_FIELD = new Field("", true, new Int(32, false), null);
  public static final Field UINT8_FIELD = new Field("", true, new Int(64, false), null);
  public static final Field DATE_FIELD = new Field("", true, Date.INSTANCE, null);
  public static final Field TIME_FIELD = new Field("", true, Time.INSTANCE, null);
  public static final Field TIMESTAMP_FIELD = new Field("", true, new Timestamp(""), null);
  public static final Field INTERVALDAY_FIELD = new Field("", true, IntervalDay.INSTANCE, null);
  public static final Field INTERVALYEAR_FIELD = new Field("", true, IntervalYear.INSTANCE, null);
  public static final Field FLOAT4_FIELD = new Field("", true, new FloatingPoint(0), null);
  public static final Field FLOAT8_FIELD = new Field("", true, new FloatingPoint(1), null);
  public static final Field LIST_FIELD = new Field("", true, List.INSTANCE, null);
  public static final Field VARCHAR_FIELD = new Field("", true, Utf8.INSTANCE, null);
  public static final Field VARBINARY_FIELD = new Field("", true, Binary.INSTANCE, null);
  public static final Field BIT_FIELD = new Field("", true, Bool.INSTANCE, null);


  public enum MinorType {
    NULL(Null.INSTANCE) {
      @Override
      public Field getField() {
        return NULL_FIELD;
      }

      @Override
      public ValueVector getNewVector(String name, BufferAllocator allocator, CallBack callBack, int... precisionScale) {
        return ZeroVector.INSTANCE;
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        return null;
      }
    },
    MAP(Tuple.INSTANCE) {
      @Override
      public Field getField() {
        throw new UnsupportedOperationException("Cannot get simple field for Map type");
      }

      @Override
      public ValueVector getNewVector(String name, BufferAllocator allocator, CallBack callBack, int... precisionScale) {
         return new MapVector(name, allocator, callBack);
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        return new SingleMapWriter((MapVector) vector);
      }
    },   //  an empty map column.  Useful for conceptual setup.  Children listed within here

    TINYINT(new Int(8, true)) {
      @Override
      public Field getField() {
        return TINYINT_FIELD;
      }

      @Override
      public ValueVector getNewVector(String name, BufferAllocator allocator, CallBack callBack, int... precisionScale) {
        return new NullableTinyIntVector(name, allocator);
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        return new TinyIntWriterImpl((NullableTinyIntVector) vector);
      }
    },   //  single byte signed integer
    SMALLINT(new Int(16, true)) {
      @Override
      public Field getField() {
        return SMALLINT_FIELD;
      }

      @Override
      public ValueVector getNewVector(String name, BufferAllocator allocator, CallBack callBack, int... precisionScale) {
        return new SmallIntVector(name, allocator);
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        return new SmallIntWriterImpl((NullableSmallIntVector) vector);
      }
    },   //  two byte signed integer
    INT(new Int(32, true)) {
      @Override
      public Field getField() {
        return INT_FIELD;
      }

      @Override
      public ValueVector getNewVector(String name, BufferAllocator allocator, CallBack callBack, int... precisionScale) {
        return new NullableIntVector(name, allocator);
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        return new IntWriterImpl((NullableIntVector) vector);
      }
    },   //  four byte signed integer
    BIGINT(new Int(64, true)) {
      @Override
      public Field getField() {
        return BIGINT_FIELD;
      }

      @Override
      public ValueVector getNewVector(String name, BufferAllocator allocator, CallBack callBack, int... precisionScale) {
        return new NullableBigIntVector(name, allocator);
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        return new BigIntWriterImpl((NullableBigIntVector) vector);
      }
    },   //  eight byte signed integer
    DATE(Date.INSTANCE) {
      @Override
      public Field getField() {
        return DATE_FIELD;
      }

      @Override
      public ValueVector getNewVector(String name, BufferAllocator allocator, CallBack callBack, int... precisionScale) {
        return new NullableDateVector(name, allocator);
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        return new DateWriterImpl((NullableDateVector) vector);
      }
    },   //  days since 4713bc
    TIME(Time.INSTANCE) {
      @Override
      public Field getField() {
        return TIME_FIELD;
      }

      @Override
      public ValueVector getNewVector(String name, BufferAllocator allocator, CallBack callBack, int... precisionScale) {
        return new NullableTimeVector(name, allocator);
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        return new TimeWriterImpl((NullableTimeVector) vector);
      }
    },   //  time in micros before or after 2000/1/1
    TIMESTAMP(new Timestamp("")) {
      @Override
      public Field getField() {
        return TIMESTAMP_FIELD;
      }

      @Override
      public ValueVector getNewVector(String name, BufferAllocator allocator, CallBack callBack, int... precisionScale) {
        return new NullableTimeStampVector(name, allocator);
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        return new TimeStampWriterImpl((NullableTimeStampVector) vector);
      }
    },
    INTERVALDAY(IntervalDay.INSTANCE) {
      @Override
      public Field getField() {
        return INTERVALDAY_FIELD;
      }

      @Override
      public ValueVector getNewVector(String name, BufferAllocator allocator, CallBack callBack, int... precisionScale) {
        return new NullableIntervalDayVector(name, allocator);
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        return new IntervalDayWriterImpl((NullableIntervalDayVector) vector);
      }
    },
    INTERVALYEAR(IntervalYear.INSTANCE) {
      @Override
      public Field getField() {
        return INTERVALYEAR_FIELD;
      }

      @Override
      public ValueVector getNewVector(String name, BufferAllocator allocator, CallBack callBack, int... precisionScale) {
        return new NullableIntervalDayVector(name, allocator);
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        return new IntervalYearWriterImpl((NullableIntervalYearVector) vector);
      }
    },
    FLOAT4(new FloatingPoint(0)) {
      @Override
      public Field getField() {
        return FLOAT4_FIELD;
      }

      @Override
      public ValueVector getNewVector(String name, BufferAllocator allocator, CallBack callBack, int... precisionScale) {
        return new NullableFloat4Vector(name, allocator);
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        return new Float4WriterImpl((NullableFloat4Vector) vector);
      }
    },   //  4 byte ieee 754
    FLOAT8(new FloatingPoint(1)) {
      @Override
      public Field getField() {
        return FLOAT8_FIELD;
      }

      @Override
      public ValueVector getNewVector(String name, BufferAllocator allocator, CallBack callBack, int... precisionScale) {
        return new NullableFloat8Vector(name, allocator);
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        return new Float8WriterImpl((NullableFloat8Vector) vector);
      }
    },   //  8 byte ieee 754
    BIT(Bool.INSTANCE) {
      @Override
      public Field getField() {
        return BIT_FIELD;
      }

      @Override
      public ValueVector getNewVector(String name, BufferAllocator allocator, CallBack callBack, int... precisionScale) {
        return new NullableBitVector(name, allocator);
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        return new BitWriterImpl((NullableBitVector) vector);
      }
    },  //  single bit value (boolean)
    VARCHAR(Utf8.INSTANCE) {
      @Override
      public Field getField() {
        return VARCHAR_FIELD;
      }

      @Override
      public ValueVector getNewVector(String name, BufferAllocator allocator, CallBack callBack, int... precisionScale) {
        return new NullableVarCharVector(name, allocator);
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        return new VarCharWriterImpl((NullableVarCharVector) vector);
      }
    },   //  utf8 variable length string
    VARBINARY(Binary.INSTANCE) {
      @Override
      public Field getField() {
        return VARBINARY_FIELD;
      }

      @Override
      public ValueVector getNewVector(String name, BufferAllocator allocator, CallBack callBack, int... precisionScale) {
        return new NullableVarBinaryVector(name, allocator);
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        return new VarBinaryWriterImpl((NullableVarBinaryVector) vector);
      }
    },   //  variable length binary
    DECIMAL(null) {
      @Override
      public ArrowType getType() {
        throw new UnsupportedOperationException("Cannot get simple type for Decimal type");
      }
      @Override
      public Field getField() {
        throw new UnsupportedOperationException("Cannot get simple field for Decimal type");
      }

      @Override
      public ValueVector getNewVector(String name, BufferAllocator allocator, CallBack callBack, int... precisionScale) {
        return new NullableDecimalVector(name, allocator, precisionScale[0], precisionScale[1]);
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        return new DecimalWriterImpl((NullableDecimalVector) vector);
      }
    },   //  variable length binary
    UINT1(new Int(8, false)) {
      @Override
      public Field getField() {
        return UINT1_FIELD;
      }

      @Override
      public ValueVector getNewVector(String name, BufferAllocator allocator, CallBack callBack, int... precisionScale) {
        return new NullableUInt1Vector(name, allocator);
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        return new UInt1WriterImpl((NullableUInt1Vector) vector);
      }
    },  //  unsigned 1 byte integer
    UINT2(new Int(16, false)) {
      @Override
      public Field getField() {
        return UINT2_FIELD;
      }

      @Override
      public ValueVector getNewVector(String name, BufferAllocator allocator, CallBack callBack, int... precisionScale) {
        return new NullableUInt2Vector(name, allocator);
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        return new UInt2WriterImpl((NullableUInt2Vector) vector);
      }
    },  //  unsigned 2 byte integer
    UINT4(new Int(32, false)) {
      @Override
      public Field getField() {
        return UINT8_FIELD;
      }

      @Override
      public ValueVector getNewVector(String name, BufferAllocator allocator, CallBack callBack, int... precisionScale) {
        return new NullableUInt4Vector(name, allocator);
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        return new UInt4WriterImpl((NullableUInt4Vector) vector);
      }
    },   //  unsigned 4 byte integer
    UINT8(new Int(64, false)) {
      @Override
      public Field getField() {
        return UINT8_FIELD;
      }

      @Override
      public ValueVector getNewVector(String name, BufferAllocator allocator, CallBack callBack, int... precisionScale) {
        return new NullableUInt8Vector(name, allocator);
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        return new UInt8WriterImpl((NullableUInt8Vector) vector);
      }
    },   //  unsigned 8 byte integer
    LIST(List.INSTANCE) {
      @Override
      public Field getField() {
        throw new UnsupportedOperationException("Cannot get simple field for List type");
      }

      @Override
      public ValueVector getNewVector(String name, BufferAllocator allocator, CallBack callBack, int... precisionScale) {
        return new ListVector(name, allocator, callBack);
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        return new UnionListWriter((ListVector) vector);
      }
    },
    UNION(Union.INSTANCE) {
      @Override
      public Field getField() {
        throw new UnsupportedOperationException("Cannot get simple field for Union type");
      }

      @Override
      public ValueVector getNewVector(String name, BufferAllocator allocator, CallBack callBack, int... precisionScale) {
        return new UnionVector(name, allocator, callBack);
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

    public abstract Field getField();

    public abstract ValueVector getNewVector(String name, BufferAllocator allocator, CallBack callBack, int... precisionScale);

    public abstract FieldWriter getNewFieldWriter(ValueVector vector);
  }

  private static final Map<ArrowType,MinorType> ARROW_TYPE_MINOR_TYPE_MAP;

  public static MinorType getMinorTypeForArrowType(ArrowType arrowType) {
    if (arrowType.getTypeType() == Type.Decimal) {
      return MinorType.DECIMAL;
    }
    return ARROW_TYPE_MINOR_TYPE_MAP.get(arrowType);
  }

  static {
    ARROW_TYPE_MINOR_TYPE_MAP = new HashMap<>();
    for (MinorType minorType : MinorType.values()) {
      if (minorType != MinorType.DECIMAL) {
        ARROW_TYPE_MINOR_TYPE_MAP.put(minorType.getType(), minorType);
      }
    }
  }

}
