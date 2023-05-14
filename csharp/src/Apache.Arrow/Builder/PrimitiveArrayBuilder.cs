using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using Apache.Arrow.Memory;
using Apache.Arrow.Reflection;
using Apache.Arrow.Types;

namespace Apache.Arrow.Builder
{
    public class VariableBinaryArrayBuilder : ArrayBuilder
    {
        public ITypedBufferBuilder ValuesBuffer { get; }

        // From the docs:
        //
        // The offsets buffer contains length + 1 signed integers (either 32-bit or 64-bit, depending on the
        // logical type), which encode the start position of each slot in the data buffer. The length of the
        // value in each slot is computed using the difference between the offset at that slot’s index and the
        // subsequent offset.
        //
        // In this builder, we choose to append the first offset (zero) upon construction, and each trailing
        // offset is then added after each individual item has been appended.
        public ITypedBufferBuilder<int> OffsetsBuffer { get; }

        public int CurrentOffset { get; internal set; }

        public VariableBinaryArrayBuilder(IArrowType dataType, int capacity = 32)
            : this(dataType, new TypedBufferBuilder<bool>(capacity), new TypedBufferBuilder<int>(capacity), new TypedBufferBuilder(-1, capacity))
        {
        }

        public VariableBinaryArrayBuilder(
            IArrowType dataType,
            ITypedBufferBuilder<bool> validity, ITypedBufferBuilder<int> offsets, ITypedBufferBuilder values
            ) : base(dataType, new ITypedBufferBuilder[] { validity, offsets, values })
        {
            ValuesBuffer = values;
            OffsetsBuffer = offsets;
            CurrentOffset = 0;

            OffsetsBuffer.AppendValue(CurrentOffset);
        }

        public override Status AppendNull()
        {
            // Append Offset
            OffsetsBuffer.AppendValue(CurrentOffset);

            // Not Append Value, get is based on offsets
            // ValuesBuffer.AppendValue(nullValue);
            return base.AppendNull();
        }

        public override Status AppendNulls(int count)
        {
            // Append Offset
            OffsetsBuffer.AppendValues(CurrentOffset, count);

            // Not Append Values, get is based on offsets
            // ValuesBuffer.AppendValues(nullValues);
            return base.AppendNulls(count);
        }

        public virtual Status AppendValue(byte value)
        {
            // Append Offset
            CurrentOffset++;
            OffsetsBuffer.AppendValue(CurrentOffset);

            // Not Append Value, get is based on offsets
            ValuesBuffer.AppendByte(value);
            return AppendValid();
        }

        public virtual Status AppendValue(ReadOnlySpan<byte> value)
        {
            // Append Offset
            CurrentOffset += value.Length;
            OffsetsBuffer.AppendValue(CurrentOffset);
            ValuesBuffer.AppendBytes(value);
            return AppendValid();
        }

        public virtual Status AppendValues(IEnumerable<byte[]> bytes)
        {
            foreach (var value in bytes)
            {
                if (value == null)
                {
                    AppendNull();
                }
                else
                {
                    ValuesBuffer.AppendBytes(value);
                    CurrentOffset += value.Length;
                    OffsetsBuffer.AppendValue(CurrentOffset);
                    AppendValid();
                }
            }
            return Status.OK;
        }

        public override Status AppendScalar(IScalar value)
        {
            return value switch
            {
                IBaseBinaryScalar binary => AppendScalar(binary),
                INullableScalar nullable => nullable.IsValid ? AppendScalar(nullable.Value) : AppendNull(),
                _ => throw new ArgumentException($"Cannot append scalar {value} in {this}, must implement IBaseBinaryScalar")
            };
        }

        public Status AppendScalar(IBaseBinaryScalar value)
        {
            if (!Validate(DataType, value.Type))
                throw new ArgumentException($"Cannot append '{value}': {value.Type} != {DataType}");
            return AppendValue(value.View());
        }

        internal override Status AppendPrimitiveValueOffset(ArrayData data)
        {
            if (data.Length == 0)
                return Status.OK;

            // Value Offsets
            var offsets = data.Buffers[1].Span.Slice(data.Offset * 4, (data.Length + 1) * 4).CastTo<int>();
            int offsetStart = offsets[0];
            int offsetEnd = offsets[offsets.Length - 1];
            int currentOffset = CurrentOffset;
            Span<int> newOffsets = new int[data.Length];

            // Element length = array[index + 1] - array[index]
            for (int i = 0; i < data.Length; i++)
            {
                // Add element length to current offset
                currentOffset += offsets[i + 1] - offsets[i];
                newOffsets[i] = currentOffset;
            }

            CurrentOffset = currentOffset;
            OffsetsBuffer.AppendValues(newOffsets);

            // Values
            // Variable byte encoded, cannot append variable bit based
            ValuesBuffer.AppendBytes(data.Buffers[2].Span.Slice(offsetStart, offsetEnd - offsetStart));

            return Status.OK;
        }
    }

    public class FixedBinaryArrayBuilder : ArrayBuilder
    {
        private readonly int _bitSize;
        private readonly int _byteSize;

        private readonly bool _isFullByte;

        public ITypedBufferBuilder ValuesBuffer { get; }

        public FixedBinaryArrayBuilder(FixedWidthType dtype, int capacity = 32)
            : this(dtype, new TypedBufferBuilder<bool>(capacity), new TypedBufferBuilder(dtype.BitWidth, capacity))
        {
        }

        public FixedBinaryArrayBuilder(
            IArrowType dataType,
            ITypedBufferBuilder<bool> validity, ITypedBufferBuilder values
            ) : base(dataType, new ITypedBufferBuilder[] { validity, values })
        {
            ValuesBuffer = values;

            _bitSize = (dataType as FixedWidthType).BitWidth;
            _byteSize = _bitSize / 8;

            _isFullByte = _bitSize % 8 == 0;
        }

        public override Status AppendNull()
        {
            // Append Empty values
            if (_isFullByte)
                ValuesBuffer.AppendEmptyBytes(_byteSize);
            else
                ValuesBuffer.AppendEmptyBits(_bitSize);

            return base.AppendNull();
        }

        public override Status AppendNulls(int count)
        {
            // Append Empty values
            if (_isFullByte)
                ValuesBuffer.AppendEmptyBytes(_byteSize * count);
            else
                ValuesBuffer.AppendEmptyBits(_bitSize * count);

            return base.AppendNulls(count);
        }

        public override Status AppendScalar(IScalar value)
        {
            return value switch
            {
                IPrimitiveScalarBase primitive => AppendScalar(primitive),
                INullableScalar nullable => nullable.IsValid ? AppendScalar(nullable.Value) : AppendNull(),
                _ => throw new ArgumentException($"Cannot append scalar {value} in {this}, must implement IPrimitiveScalarBase")
            };
        }

        public Status AppendScalar(IPrimitiveScalarBase value)
        {
            if (!Validate(value.Type))
                throw new ArgumentException($"Cannot append '{value}': {value.Type} != {DataType}");
            return AppendBytes(value.View(), 1);
        }

        // Bulk raw struct several bytes
        public Status AppendBytes(ICollection<byte[]> values)
        {
            Span<bool> mask = new bool[values.Count];
            ValuesBuffer.AppendFixedSizeBytes(values, _byteSize, mask, out int nullCount);

            return AppendValidity(mask, nullCount);
        }

        public Status AppendValue(bool value)
        {
            ValuesBuffer.AppendBit(value);
            return AppendValid();
        }

        public Status AppendValue(ReadOnlySpan<byte> values)
            => AppendBytes(values.Slice(0, _byteSize), 1);

        public Status AppendBytes(ReadOnlySpan<byte> values, int length)
        {
            ValuesBuffer.AppendBytes(values);
            return AppendValidity(true, length);
        }

        public Status AppendRepeatBytes(ReadOnlySpan<byte> value, int count)
        {
            ValuesBuffer.AppendBytes(value, count);
            return AppendValidity(true, count);
        }

        public Status AppendBytes(ReadOnlySpan<byte> values, ReadOnlySpan<bool> validity)
        {
            ValuesBuffer.AppendBytes(values);
            return AppendValidity(validity);
        }

        internal override Status AppendPrimitiveValueOffset(ArrayData data)
        {
            // Values
            var dataValues = data.Buffers[1];
            int bitSize = ValuesBuffer.ValueBitSize;
            int byteSize = ValuesBuffer.ValueByteSize;

            if (bitSize % 8 == 0)
            {
                // Fixed byte encoded
                var span = dataValues.Span.Slice(data.Offset * byteSize, data.Length * byteSize);
                ValuesBuffer.AppendBytes(span);
            }
            else if (data.Offset == 0)
            {
                int dataBitLength = data.Length * bitSize;
                int dataByteLength = data.Length * byteSize;

                // Bulk copy bytes
                ValuesBuffer.AppendBytes(dataValues.Span.Slice(0, data.Length * byteSize));

                // Copy remaining bits
                Span<bool> remainingBits = BitUtility
                    .BytesToBits(dataValues.Span.Slice(dataByteLength))
                    .Slice(0, dataBitLength - dataByteLength * 8);

                ValuesBuffer.AppendBits(remainingBits);
            }
            else
            {
                int dataBitLength = data.Length * bitSize;
                Span<bool> bits = new bool[dataBitLength];
                bool allTrue = true;
                bool allFalse = true;
                int bitOffset = data.Offset * bitSize;

                for (int j = 0; j < dataBitLength; j++)
                {
                    bool bit = BitUtility.GetBit(dataValues.Span, bitOffset + j);

                    if (bit)
                        allFalse = false;
                    else
                        allTrue = false;

                    bits[j] = bit;
                }

                if (allFalse)
                    ValuesBuffer.AppendBits(false, dataBitLength);
                else if (allTrue)
                    ValuesBuffer.AppendBits(true, dataBitLength);
                else
                    ValuesBuffer.AppendBits(bits);
            }

            return Status.OK;
        }
    }

    public class FixedBinaryArrayBuilder<T> : FixedBinaryArrayBuilder where T : struct
    {
        public new ITypedBufferBuilder<T> ValuesBuffer { get; }

        public FixedBinaryArrayBuilder(int capacity = 32) : this(TypeReflection<T>.ArrowType as FixedWidthType, capacity)
        {
        }

        public FixedBinaryArrayBuilder(FixedWidthType dtype, int capacity = 32)
            : this(dtype, new TypedBufferBuilder<T>(dtype.BitWidth, capacity))
        {
        }

        internal FixedBinaryArrayBuilder(FixedWidthType dtype, ITypedBufferBuilder<T> values, int capacity = 32)
            : base(dtype, new TypedBufferBuilder<bool>(capacity), values)
        {
            ValuesBuffer = values;
        }

        public virtual Status AppendValue(T value)
            => AppendBytes(MemoryMarshal.AsBytes(TypeReflection.CreateReadOnlySpan(ref value)), 1);

        public virtual Status AppendValues(ReadOnlySpan<T> values)
            => AppendBytes(MemoryMarshal.AsBytes(values), values.Length);

        public virtual Status AppendValues(ICollection<T?> values)
        {
            Span<bool> mask = new bool[values.Count];
            ValuesBuffer.AppendValues(values, mask, out int nullCount);
            return AppendValidity(mask, nullCount);
        }

        public virtual Status AppendValues(T[] values)
            => AppendBytes(MemoryMarshal.AsBytes<T>(values), values.Length);

        public virtual Status AppendValues(T value, int count)
            => AppendRepeatBytes(MemoryMarshal.AsBytes(TypeReflection.CreateReadOnlySpan(ref value)), count);

        public virtual Status AppendValues(ReadOnlySpan<T> values, ReadOnlySpan<bool> validity)
            => AppendBytes(MemoryMarshal.AsBytes(values), validity);
    }

    public class BooleanArrayBuilder : FixedBinaryArrayBuilder<bool>
    {
        public BooleanArrayBuilder(int capacity = 32) : this(BooleanType.Default, capacity)
        {
        }

        public BooleanArrayBuilder(BooleanType dtype, int capacity = 32) : base(dtype, capacity)
        {
        }

        public override Status AppendScalar(IScalar value)
        {
            return value switch
            {
                BooleanScalar boolean => AppendScalar(boolean),
                INullableScalar nullable => nullable.IsValid ? AppendScalar(nullable.Value) : AppendNull(),
                _ => throw new ArgumentException($"Cannot append scalar {value} in {this}, must implement BooleanScalar")
            };
        }
        public Status AppendScalar(BooleanScalar value) => AppendValue(value.Value);

        public override Status AppendValue(bool value)
        {
            ValuesBuffer.AppendBit(value);
            return AppendValid();
        }

        public override Status AppendValues(ReadOnlySpan<bool> values)
        {
            ValuesBuffer.AppendBits(values);
            return AppendValidity(true, values.Length);
        }

        public override Status AppendValues(bool[] values)
        {
            ValuesBuffer.AppendBits(values);
            return AppendValidity(true, values.Length);
        }

        public override Status AppendValues(ReadOnlySpan<bool> values, ReadOnlySpan<bool> validity)
        {
            ValuesBuffer.AppendBits(values);
            return AppendValidity(validity);
        }

        public override Status AppendValues(bool value, int count)
        {
            ValuesBuffer.AppendBits(value, count);
            return AppendValidity(true, count);
        }

        public override IArrowArray Build(MemoryAllocator allocator = default) => Build(allocator);

        public BooleanArray Build(MemoryAllocator allocator = default, bool _ = true)
            => new BooleanArray(FinishInternal(allocator));
    }
}
