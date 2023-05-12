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
        public IValueBufferBuilder ValuesBuffer { get; }

        // From the docs:
        //
        // The offsets buffer contains length + 1 signed integers (either 32-bit or 64-bit, depending on the
        // logical type), which encode the start position of each slot in the data buffer. The length of the
        // value in each slot is computed using the difference between the offset at that slot’s index and the
        // subsequent offset.
        //
        // In this builder, we choose to append the first offset (zero) upon construction, and each trailing
        // offset is then added after each individual item has been appended.
        public IPrimitiveBufferBuilder<int> OffsetsBuffer { get; }

        public int CurrentOffset { get; internal set; }

        public VariableBinaryArrayBuilder(IArrowType dataType, int capacity = 32)
            : this(dataType, new ValueBufferBuilder<bool>(capacity), new ValueBufferBuilder<int>(capacity), new ValueBufferBuilder(64, capacity))
        {
        }

        public VariableBinaryArrayBuilder(
            IArrowType dataType,
            IPrimitiveBufferBuilder<bool> validity, IPrimitiveBufferBuilder<int> offsets, IValueBufferBuilder values
            ) : base(dataType, new IValueBufferBuilder[] { validity, offsets, values })
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

        public override Status AppendScalar(IScalar value)
        {
            return value switch
            {
                IBaseBinaryScalar binary => AppendScalar(binary),
                INullableScalar nullable => nullable.IsValid ? AppendScalar(nullable.Value) : AppendNull(),
                _ => throw new ArgumentException($"Cannot append scalar {value} in {this}, must implement IBaseBinaryScalar")
            };
        }

        public Status AppendScalar(IBaseBinaryScalar value) => AppendValue(value.View());
    }

    public class FixedBinaryArrayBuilder : ArrayBuilder
    {
        private readonly int _bitSize;
        private readonly int _byteSize;

        private readonly bool _isFullByte;

        public IValueBufferBuilder ValuesBuffer { get; }

        public FixedBinaryArrayBuilder(FixedWidthType dtype, int capacity = 32)
            : this(dtype, new ValueBufferBuilder<bool>(capacity), new ValueBufferBuilder(dtype.BitWidth, capacity))
        {
        }

        public FixedBinaryArrayBuilder(
            IArrowType dataType,
            IPrimitiveBufferBuilder<bool> validity, IValueBufferBuilder values
            ) : base(dataType, new IValueBufferBuilder[] { validity, values })
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
            Validate(value.Type);
            return AppendBytes(value.View(), 1);
        }

        // Bulk raw struct several bytes
        public Status AppendBytes(ICollection<byte[]> values)
        {
            Span<bool> mask = new bool[values.Count];
            ValuesBuffer.AppendFixedSizeBytes(values, _byteSize, mask, out int nullCount);

            return AppendValidity(mask, nullCount);
        }

        public Status AppendValue(ReadOnlySpan<byte> values)
            => AppendBytes(values.Slice(0, _byteSize), 1);

        public Status AppendBytes(ReadOnlySpan<byte> values, int length)
        {
            ValuesBuffer.AppendBytes(values);
            return AppendValidity(true, length);
        }

        public Status AppendBytes(ReadOnlySpan<byte> values, ReadOnlySpan<bool> validity)
        {
            ValuesBuffer.AppendBytes(values);
            return AppendValidity(validity);
        }
    }

    public class FixedBinaryArrayBuilder<T> : FixedBinaryArrayBuilder where T : struct
    {
        public FixedBinaryArrayBuilder(int capacity = 32) : this(TypeReflection<T>.ArrowType as FixedWidthType, capacity)
        {
        }

        public FixedBinaryArrayBuilder(FixedWidthType dtype, int capacity = 32) : base(dtype, capacity)
        {
        }

        public virtual Status AppendValue(T value)
            => AppendBytes(MemoryMarshal.AsBytes(TypeReflection.CreateReadOnlySpan(ref value)), 1);

        public virtual Status AppendValues(ReadOnlySpan<T> values)
            => AppendBytes(MemoryMarshal.AsBytes(values), values.Length);

        public virtual Status AppendValues(T[] values)
            => AppendBytes(MemoryMarshal.AsBytes<T>(values), values.Length);

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

        public override IArrowArray Build(MemoryAllocator allocator = default) => Build(allocator);

        public BooleanArray Build(MemoryAllocator allocator = default, bool _ = true)
            => new BooleanArray(FinishInternal(allocator));
    }
}
