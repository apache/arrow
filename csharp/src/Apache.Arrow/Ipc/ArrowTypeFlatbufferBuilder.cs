﻿// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System;
using Apache.Arrow.Flatbuf;
using Apache.Arrow.Types;
using Google.FlatBuffers;
using DateUnit = Apache.Arrow.Flatbuf.DateUnit;
using TimeUnit = Apache.Arrow.Types.TimeUnit;

namespace Apache.Arrow.Ipc
{
    internal class ArrowTypeFlatbufferBuilder
    {
        public struct FieldType
        {
            public readonly Flatbuf.Type Type;
            public readonly int Offset;

            public static FieldType Build<T>(Flatbuf.Type type, Offset<T> offset)
                where T : struct =>
                new FieldType(type, offset.Value);

            public FieldType(Flatbuf.Type type, int offset)
            {
                Type = type;
                Offset = offset;
            }
        }

        class TypeVisitor :
            IArrowTypeVisitor<BooleanType>,
            IArrowTypeVisitor<Int8Type>,
            IArrowTypeVisitor<Int16Type>,
            IArrowTypeVisitor<Int32Type>,
            IArrowTypeVisitor<Int64Type>,
            IArrowTypeVisitor<UInt8Type>,
            IArrowTypeVisitor<UInt16Type>,
            IArrowTypeVisitor<UInt32Type>,
            IArrowTypeVisitor<UInt64Type>,
#if NET5_0_OR_GREATER
            IArrowTypeVisitor<HalfFloatType>,
#endif
            IArrowTypeVisitor<FloatType>,
            IArrowTypeVisitor<DoubleType>,
            IArrowTypeVisitor<StringType>,
            IArrowTypeVisitor<StringViewType>,
            IArrowTypeVisitor<LargeStringType>,
            IArrowTypeVisitor<Date32Type>,
            IArrowTypeVisitor<Date64Type>,
            IArrowTypeVisitor<Time32Type>,
            IArrowTypeVisitor<Time64Type>,
            IArrowTypeVisitor<DurationType>,
            IArrowTypeVisitor<IntervalType>,
            IArrowTypeVisitor<BinaryType>,
            IArrowTypeVisitor<BinaryViewType>,
            IArrowTypeVisitor<LargeBinaryType>,
            IArrowTypeVisitor<TimestampType>,
            IArrowTypeVisitor<ListType>,
            IArrowTypeVisitor<ListViewType>,
            IArrowTypeVisitor<LargeListType>,
            IArrowTypeVisitor<FixedSizeListType>,
            IArrowTypeVisitor<UnionType>,
            IArrowTypeVisitor<StructType>,
            IArrowTypeVisitor<Decimal32Type>,
            IArrowTypeVisitor<Decimal64Type>,
            IArrowTypeVisitor<Decimal128Type>,
            IArrowTypeVisitor<Decimal256Type>,
            IArrowTypeVisitor<DictionaryType>,
            IArrowTypeVisitor<FixedSizeBinaryType>,
            IArrowTypeVisitor<MapType>,
            IArrowTypeVisitor<NullType>
        {
            private FlatBufferBuilder Builder { get; }

            public FieldType Result { get; private set; }

            public TypeVisitor(FlatBufferBuilder builder)
            {
                Builder = builder;
            }

            public void Visit(Int8Type type) => CreateIntType(type);
            public void Visit(Int16Type type) => CreateIntType(type);
            public void Visit(Int32Type type) => CreateIntType(type);
            public void Visit(Int64Type type) => CreateIntType(type);
            public void Visit(UInt8Type type) => CreateIntType(type);
            public void Visit(UInt16Type type) => CreateIntType(type);
            public void Visit(UInt32Type type) => CreateIntType(type);
            public void Visit(UInt64Type type) => CreateIntType(type);

            public void Visit(BooleanType type)
            {
                Flatbuf.Bool.StartBool(Builder);
                Result = FieldType.Build(
                    Flatbuf.Type.Bool,
                    Flatbuf.Bool.EndBool(Builder));
            }

            public void Visit(BinaryType type)
            {
                Flatbuf.Binary.StartBinary(Builder);
                Result = FieldType.Build(
                    Flatbuf.Type.Binary,
                    Flatbuf.Binary.EndBinary(Builder));
            }

            public void Visit(BinaryViewType type)
            {
                Flatbuf.BinaryView.StartBinaryView(Builder);
                Offset<BinaryView> offset = Flatbuf.BinaryView.EndBinaryView(Builder);
                Result = FieldType.Build(
                    Flatbuf.Type.BinaryView, offset);
            }

            public void Visit(LargeBinaryType type)
            {
                Flatbuf.LargeBinary.StartLargeBinary(Builder);
                Offset<LargeBinary> offset = Flatbuf.LargeBinary.EndLargeBinary(Builder);
                Result = FieldType.Build(
                    Flatbuf.Type.LargeBinary, offset);
            }

            public void Visit(ListType type)
            {
                Flatbuf.List.StartList(Builder);
                Result = FieldType.Build(
                    Flatbuf.Type.List,
                    Flatbuf.List.EndList(Builder));
            }

            public void Visit(ListViewType type)
            {
                Flatbuf.ListView.StartListView(Builder);
                Result = FieldType.Build(
                    Flatbuf.Type.ListView,
                    Flatbuf.ListView.EndListView(Builder));
            }

            public void Visit(LargeListType type)
            {
                Flatbuf.LargeList.StartLargeList(Builder);
                Result = FieldType.Build(
                    Flatbuf.Type.LargeList,
                    Flatbuf.LargeList.EndLargeList(Builder));
            }

            public void Visit(FixedSizeListType type)
            {
                Result = FieldType.Build(
                    Flatbuf.Type.FixedSizeList,
                    Flatbuf.FixedSizeList.CreateFixedSizeList(Builder, type.ListSize));
            }

            public void Visit(UnionType type)
            {
                Result = FieldType.Build(
                    Flatbuf.Type.Union,
                    Flatbuf.Union.CreateUnion(Builder, ToFlatBuffer(type.Mode), Flatbuf.Union.CreateTypeIdsVector(Builder, type.TypeIds)));
            }

            public void Visit(StringType type)
            {
                Flatbuf.Utf8.StartUtf8(Builder);
                Offset<Utf8> offset = Flatbuf.Utf8.EndUtf8(Builder);
                Result = FieldType.Build(
                    Flatbuf.Type.Utf8, offset);
            }

            public void Visit(StringViewType type)
            {
                Flatbuf.Utf8View.StartUtf8View(Builder);
                Offset<Utf8View> offset = Flatbuf.Utf8View.EndUtf8View(Builder);
                Result = FieldType.Build(
                    Flatbuf.Type.Utf8View, offset);
            }

            public void Visit(LargeStringType type)
            {
                Flatbuf.LargeUtf8.StartLargeUtf8(Builder);
                Offset<LargeUtf8> offset = Flatbuf.LargeUtf8.EndLargeUtf8(Builder);
                Result = FieldType.Build(
                    Flatbuf.Type.LargeUtf8, offset);
            }

            public void Visit(TimestampType type)
            {
                StringOffset timezoneStringOffset = default;

                if (!string.IsNullOrWhiteSpace(type.Timezone))
                    timezoneStringOffset = Builder.CreateString(type.Timezone);

                Result = FieldType.Build(
                    Flatbuf.Type.Timestamp,
                    Flatbuf.Timestamp.CreateTimestamp(Builder, ToFlatBuffer(type.Unit), timezoneStringOffset));
            }

            public void Visit(Date32Type type)
            {
                Result = FieldType.Build(
                    Flatbuf.Type.Date,
                    Flatbuf.Date.CreateDate(Builder, DateUnit.DAY));
            }

            public void Visit(Date64Type type)
            {
                Result = FieldType.Build(
                    Flatbuf.Type.Date,
                    Flatbuf.Date.CreateDate(Builder));
            }

            public void Visit(Time32Type type)
            {
                Result = FieldType.Build(
                    Flatbuf.Type.Time,
                    Flatbuf.Time.CreateTime(Builder, ToFlatBuffer(type.Unit)));
            }

#if NET5_0_OR_GREATER
            public void Visit(HalfFloatType type)
            {
                Result = FieldType.Build(
                    Flatbuf.Type.FloatingPoint,
                    Flatbuf.FloatingPoint.CreateFloatingPoint(Builder, Precision.HALF));
            }
#endif

            public void Visit(FloatType type)
            {
                Result = FieldType.Build(
                    Flatbuf.Type.FloatingPoint,
                    Flatbuf.FloatingPoint.CreateFloatingPoint(Builder, Precision.SINGLE));
            }

            public void Visit(DoubleType type)
            {
                Result = FieldType.Build(
                    Flatbuf.Type.FloatingPoint,
                    Flatbuf.FloatingPoint.CreateFloatingPoint(Builder, Precision.DOUBLE));
            }

            public void Visit(Time64Type type)
            {
                Result = FieldType.Build(
                    Flatbuf.Type.Time,
                    Flatbuf.Time.CreateTime(Builder, ToFlatBuffer(type.Unit), 64));
            }

            public void Visit(DurationType type)
            {
                Result = FieldType.Build(
                    Flatbuf.Type.Duration,
                    Flatbuf.Duration.CreateDuration(Builder, ToFlatBuffer(type.Unit)));
            }

            public void Visit(IntervalType type)
            {
                Result = FieldType.Build(
                    Flatbuf.Type.Interval,
                    Flatbuf.Interval.CreateInterval(Builder, ToFlatBuffer(type.Unit)));
            }

            public void Visit(StructType type)
            {
                Flatbuf.Struct_.StartStruct_(Builder);
                Result = FieldType.Build(Flatbuf.Type.Struct_, Flatbuf.Struct_.EndStruct_(Builder));
            }

            public void Visit(Decimal32Type type)
            {
                Result = FieldType.Build(
                    Flatbuf.Type.Decimal,
                    Flatbuf.Decimal.CreateDecimal(Builder, type.Precision, type.Scale, type.BitWidth));
            }

            public void Visit(Decimal64Type type)
            {
                Result = FieldType.Build(
                    Flatbuf.Type.Decimal,
                    Flatbuf.Decimal.CreateDecimal(Builder, type.Precision, type.Scale, type.BitWidth));
            }

            public void Visit(Decimal128Type type)
            {
                Result = FieldType.Build(
                    Flatbuf.Type.Decimal,
                    Flatbuf.Decimal.CreateDecimal(Builder, type.Precision, type.Scale, type.BitWidth));
            }

            public void Visit(Decimal256Type type)
            {
                Result = FieldType.Build(
                    Flatbuf.Type.Decimal,
                    Flatbuf.Decimal.CreateDecimal(Builder, type.Precision, type.Scale, type.BitWidth));
            }

            private void CreateIntType(NumberType type)
            {
                Result = FieldType.Build(
                    Flatbuf.Type.Int,
                    Flatbuf.Int.CreateInt(Builder, type.BitWidth, type.IsSigned));
            }

            public void Visit(DictionaryType type)
            {
                // In this library, the dictionary "type" is a logical construct. Here we
                // pass through to the value type, as we've already captured the index
                // type in the DictionaryEncoding metadata in the parent field
                type.ValueType.Accept(this);
            }
            
            public void Visit(FixedSizeBinaryType type)
            {
                Result = FieldType.Build(
                    Flatbuf.Type.FixedSizeBinary,
                    Flatbuf.FixedSizeBinary.CreateFixedSizeBinary(Builder, type.ByteWidth));
            }

            public void Visit(MapType type)
            {
                Result = FieldType.Build(
                    Flatbuf.Type.Map,
                    Flatbuf.Map.CreateMap(Builder, type.KeySorted));
            }

            public void Visit(NullType type)
            {
                Flatbuf.Null.StartNull(Builder);
                Result = FieldType.Build(
                    Flatbuf.Type.Null,
                    Flatbuf.Null.EndNull(Builder));
            }

            public void Visit(IArrowType type)
            {
                throw new NotImplementedException($"Cannot visit type {type}");
            }
        }

        private readonly TypeVisitor _visitor;

        public ArrowTypeFlatbufferBuilder(FlatBufferBuilder builder)
        {
            _visitor = new TypeVisitor(builder);
        }

        public FieldType BuildFieldType(Field field)
        {
            field.DataType.Accept(_visitor);
            return _visitor.Result;
        }

        private static Flatbuf.TimeUnit ToFlatBuffer(TimeUnit unit)
        {
            Flatbuf.TimeUnit result;

            switch (unit)
            {
                case TimeUnit.Microsecond:
                    result = Flatbuf.TimeUnit.MICROSECOND;
                    break;
                case TimeUnit.Millisecond:
                    result = Flatbuf.TimeUnit.MILLISECOND;
                    break;
                case TimeUnit.Nanosecond:
                    result = Flatbuf.TimeUnit.NANOSECOND;
                    break;
                case TimeUnit.Second:
                    result = Flatbuf.TimeUnit.SECOND;
                    break;
                default:
                    throw new ArgumentException(nameof(unit),
                        $"unsupported timestamp unit <{unit}>");
            }

            return result;
        }

        private static Flatbuf.UnionMode ToFlatBuffer(Types.UnionMode mode)
        {
            return mode switch
            {
                Types.UnionMode.Dense => Flatbuf.UnionMode.Dense,
                Types.UnionMode.Sparse => Flatbuf.UnionMode.Sparse,
                _ => throw new ArgumentException($"unsupported union mode <{mode}>", nameof(mode)),
            };
        }

        private static Flatbuf.IntervalUnit ToFlatBuffer(Types.IntervalUnit unit)
        {
            return unit switch
            {
                Types.IntervalUnit.YearMonth => Flatbuf.IntervalUnit.YEAR_MONTH,
                Types.IntervalUnit.DayTime => Flatbuf.IntervalUnit.DAY_TIME,
                Types.IntervalUnit.MonthDayNanosecond => Flatbuf.IntervalUnit.MONTH_DAY_NANO,
                _ => throw new ArgumentException($"unsupported interval unit <{unit}>", nameof(unit))
            };
        }
    }
}
