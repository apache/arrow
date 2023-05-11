using System;
using System.Linq;
using Apache.Arrow.Arrays;
using Apache.Arrow.Memory;
using Apache.Arrow.Types;

namespace Apache.Arrow.Builder
{
    public abstract class ArrayBuilder : IArrayBuilder
    {
        public IArrowType DataType { get; }

        public int Length { get; protected set; }

        public int NullCount { get; protected set; }

        public int Offset { get; }

        public IValueBufferBuilder[] Buffers { get; }
        public IValueBufferBuilder ValidityBuffer => Buffers[ValidityBufferIndex];
        private int ValidityBufferIndex => 0;

        public IArrayBuilder[] Children { get; }

        public IArrayBuilder Dictionary { get; }

        protected ArrayBuilder(
            IArrowType dataType,
            IValueBufferBuilder[] buffers,
            IArrayBuilder[] children = null,
            IArrayBuilder dictionary = null
        )
        {
            DataType = dataType;

            Buffers = buffers;
            Children = children;
            Dictionary = dictionary;

            Length = 0;
            NullCount = 0;
            Offset = 0;
        }

        TBuilder IArrayBuilder.As<TBuilder>() => As<TBuilder>();
        TBuilder As<TBuilder>() where TBuilder : IArrayBuilder
        {
            if (this is not TBuilder casted)
            {
                throw new InvalidOperationException($"Cannot cast {this} as {typeof(TBuilder)}");
            }
            return casted;
        }

        public virtual Status AppendNull()
        {
            ValidityBuffer.AppendBit(false);
            NullCount++;
            Length++;
            return Status.OK;
        }

        public virtual Status AppendNulls(int count)
        {
            Reserve(count);
            ValidityBuffer.AppendBits(false, count);
            NullCount += count;
            Length += count;
            return Status.OK;
        }

        internal virtual Status AppendValid()
        {
            ValidityBuffer.AppendBit(true);
            Length++;
            return Status.OK;
        }

        internal virtual Status AppendValidity(bool isValid, int count)
        {
            ValidityBuffer.AppendBits(isValid, count);
            Length += count;
            if (!isValid)
                NullCount += count;
            return Status.OK;
        }

        internal virtual Status AppendValidity(ReadOnlySpan<bool> mask, int nullCount)
        {
            if (nullCount == 0)
                ValidityBuffer.AppendBits(true, mask.Length);
            else if (nullCount == mask.Length)
                ValidityBuffer.AppendBits(false, nullCount);
            else
                ValidityBuffer.AppendBits(mask);

            Length += mask.Length;

            NullCount += nullCount;

            return Status.OK;
        }

        internal virtual Status AppendValidity(ReadOnlySpan<bool> mask)
        {
            int nullCount = 0;

            foreach (bool isValid in mask)
                if (!isValid)
                    nullCount++;

            return AppendValidity(mask, nullCount);
        }

        // Append values from arrow objects
        internal void Validate(IArrowType dtype)
        {
            if (dtype.TypeId != DataType.TypeId)
                throw new ArgumentException($"Cannot append data type {dtype} in builder with data type {DataType}");
        }

        public virtual Status AppendScalar(IScalar value)
        {
            switch (DataType.TypeId)
            {
                case ArrowTypeId.String:
                case ArrowTypeId.Binary:
                    return As<VariableBinaryArrayBuilder>().AppendScalar(value);
                case ArrowTypeId.Boolean:
                    return As<BooleanArrayBuilder>().AppendScalar(value);
                case ArrowTypeId.UInt8:
                case ArrowTypeId.Int8:
                case ArrowTypeId.UInt16:
                case ArrowTypeId.Int16:
                case ArrowTypeId.UInt32:
                case ArrowTypeId.Int32:
                case ArrowTypeId.UInt64:
                case ArrowTypeId.Int64:
#if NET5_0_OR_GREATER
                case ArrowTypeId.HalfFloat:
#endif
                case ArrowTypeId.Float:
                case ArrowTypeId.Double:
                case ArrowTypeId.Decimal128:
                case ArrowTypeId.Decimal256:
                    return As<FixedBinaryArrayBuilder>().AppendScalar(value);
                case ArrowTypeId.Struct:
                    return As<StructArrayBuilder>().AppendScalar(value);
                case ArrowTypeId.List:
                    return As<ListArrayBuilder>().AppendScalar(value);
                case ArrowTypeId.Null:
                    return AppendNull();
                default:
                    Validate(value.Type);
                    throw new ArgumentException($"Cannot append {value} in builder with data type {DataType}");
            }
        }

        public virtual Status AppendArray(IArrowArray array) => AppendArray(array.Data);

        public virtual Status AppendArray(ArrayData data)
        {
            // TODO: Make better / recursive fields data type check
            Validate(data.DataType);

            // Add Length and reserve
            Reserve(data.Length);

            // Handle validity
            var dataValidity = data.Buffers[ValidityBufferIndex];
            
            // Check if need to recalculate null count
            if (data.NullCount < 0)
            {
                Span<bool> bits = new bool[data.Length];
                int nullCount = 0;

                // Need recalculate nulls
                for (int i = data.Offset; i < data.Length; i++)
                {
                    bool isValid = BitUtility.GetBit(dataValidity.Span, i);

                    if (!isValid)
                        nullCount++;

                    bits[i] = isValid;
                }

                AppendValidity(bits, nullCount);
                // Update it since we calculated it
                data.NullCount = nullCount;
            }
            else
            {
                int nullCount = data.NullCount;

                if (nullCount == 0)
                    AppendValidity(true, data.Length);
                else if (nullCount == data.Length)
                    AppendValidity(false, data.Length);
                else if (data.Offset == 0)
                {
                    // Should use AppendValidity, but optimized for bulk byte copy
                    // Bulk copy full bytes
                    int end = (data.Length * ValidityBuffer.ValueBitSize) / 8;
                    ValidityBuffer.AppendBytes(dataValidity.Span.Slice(0, end));

                    // Copy remaining bits
                    Span<bool> bits = BitUtility.BytesToBits(dataValidity.Span.Slice(end)).Slice(0, data.Length - end * 8);
                    ValidityBuffer.AppendBits(bits);

                    NullCount += nullCount;
                    Length += data.Length;
                }
                else
                {
                    Span<bool> bits = new bool[data.Length];

                    for (int i = data.Offset; i < data.Length; i++)
                        bits[i] = BitUtility.GetBit(dataValidity.Span, i);

                    AppendValidity(bits, nullCount);
                }
            }
            
            for (int i = 0; i < Buffers.Length; i++)
            {
                if (i != ValidityBufferIndex) // already handled before
                {
                    IValueBufferBuilder current = Buffers[i];
                    ArrowBuffer other = data.Buffers[i];
                    int dataBitLength = data.Length * current.ValueBitSize;
                    int dataByteLength = dataBitLength / 8;

                    if (current.ValueBitSize % 8 == 0)
                        // Full byte encoded
                        current.AppendBytes(other.Span.Slice((data.Offset * current.ValueBitSize) / 8, dataByteLength));
                    else if (data.Offset == 0)
                    {
                        // Bulk copy bytes
                        current.AppendBytes(other.Span.Slice(0, dataByteLength));

                        // Copy remaining bits
                        Span<bool> remainingBits = BitUtility
                            .BytesToBits(other.Span.Slice(dataByteLength))
                            .Slice(0, dataBitLength - dataByteLength * 8);

                        current.AppendBits(remainingBits);
                    }
                    else
                    {
                        Span<bool> bits = new bool[dataBitLength];
                        bool allTrue = true;
                        bool allFalse = true;

                        for (int j = data.Offset; j < dataBitLength; j++)
                        {
                            bool bit = BitUtility.GetBit(other.Span, j);

                            if (bit)
                                allFalse = false;
                            else
                                allTrue = false;

                            bits[j] = bit;
                        }

                        if (allFalse)
                            current.AppendBits(false, dataBitLength);
                        else if (allTrue)
                            current.AppendBits(true, dataBitLength);
                        else
                            current.AppendBits(bits);
                    }
                }
                
            }

            // Append children data
            if (Children != null && data.Children != null)
            {
                for (int i = 0; i < Children.Length; i++)
                {
                    Children[i].AppendArray(data.Children[i]);
                }
            }

            if (Dictionary != null && data.Dictionary != null)
            {
                Dictionary.AppendArray(data.Dictionary);
            }

            return Status.OK;
        }

        public ArrayData FinishInternal(MemoryAllocator allocator = null)
        {
            MemoryAllocator memoryAllocator = allocator ?? MemoryAllocator.Default.Value;

            return new ArrayData(
                DataType, Length, NullCount, Offset,
                Buffers.Select(b => b.Build(memoryAllocator)).ToArray(),
                Children?.Select(c => c.FinishInternal(memoryAllocator)).ToArray(),
                Dictionary?.FinishInternal()
            );
        }

        public virtual IArrowArray Build(MemoryAllocator allocator = null)
            => ArrayBuilderFactory.MakeArray(FinishInternal(allocator));

        // Memory management
        public Status Reserve(int additionnalCapacity)
        {
            foreach (IValueBufferBuilder buffer in Buffers)
                buffer.Reserve(additionnalCapacity);

            if (Children != null)
                foreach (IArrayBuilder builder in Children)
                    builder.Reserve(additionnalCapacity);

            if (Dictionary != null)
                Dictionary.Reserve(additionnalCapacity);

            return Status.OK;
        }

        public Status Resize(int capacity)
        {
            foreach (IValueBufferBuilder buffer in Buffers)
                buffer.Resize(capacity);

            if (Children != null)
                foreach (IArrayBuilder builder in Children)
                    builder.Resize(capacity);

            if (Dictionary != null)
                Dictionary.Resize(capacity);

            return Status.OK;
        }

        public void Clear()
        {
            foreach (IValueBufferBuilder buffer in Buffers)
                buffer.Clear();

            if (Children != null)
                foreach (IArrayBuilder builder in Children)
                    builder.Clear();

            if (Dictionary != null)
                Dictionary.Clear();
        }
    }

    public static class ArrayBuilderFactory
    {
        public static IArrayBuilder MakeBuilder(IArrowType dtype, int capacity = 32)
        {
            switch (dtype.TypeId)
            {
                case ArrowTypeId.Boolean:
                    return new BooleanArrayBuilder(dtype as BooleanType, capacity);
                case ArrowTypeId.UInt8:
                    return new FixedBinaryArrayBuilder<byte>(dtype as FixedWidthType, capacity);
                case ArrowTypeId.Int8:
                    return new FixedBinaryArrayBuilder<sbyte>(dtype as FixedWidthType, capacity);
                case ArrowTypeId.UInt16:
                    return new FixedBinaryArrayBuilder<ushort>(dtype as FixedWidthType, capacity);
                case ArrowTypeId.Int16:
                    return new FixedBinaryArrayBuilder<short>(dtype as FixedWidthType, capacity);
                case ArrowTypeId.UInt32:
                    return new FixedBinaryArrayBuilder<uint>(dtype as FixedWidthType, capacity);
                case ArrowTypeId.Int32:
                    return new FixedBinaryArrayBuilder<int>(dtype as FixedWidthType, capacity);
                case ArrowTypeId.UInt64:
                    return new FixedBinaryArrayBuilder<ulong>(dtype as FixedWidthType, capacity);
                case ArrowTypeId.Int64:
                    return new FixedBinaryArrayBuilder<long>(dtype as FixedWidthType, capacity);
#if NET5_0_OR_GREATER
                case ArrowTypeId.HalfFloat:
                    return new FixedBinaryArrayBuilder<Half>(dtype as FixedWidthType, capacity);
#endif
                case ArrowTypeId.Float:
                    return new FixedBinaryArrayBuilder<float>(dtype as FixedWidthType, capacity);
                case ArrowTypeId.Double:
                    return new FixedBinaryArrayBuilder<double>(dtype as FixedWidthType, capacity);
                case ArrowTypeId.FixedSizedBinary:
                    return new FixedBinaryArrayBuilder(dtype as FixedWidthType, capacity);
                case ArrowTypeId.String:
                    return new StringArrayBuilder(dtype, capacity);
                case ArrowTypeId.Binary:
                    return new BinaryArrayBuilder(dtype, capacity);
                case ArrowTypeId.Struct:
                    return new StructArrayBuilder(dtype as StructType, capacity);
                case ArrowTypeId.List:
                    return new ListArrayBuilder(dtype as ListType, capacity);
                case ArrowTypeId.Timestamp:
                    return new TimestampArrayBuilder(dtype as TimestampType, capacity);
                case ArrowTypeId.Time32:
                    return new Time32ArrayBuilder(dtype as TimeType, capacity);
                case ArrowTypeId.Time64:
                    return new Time64ArrayBuilder(dtype as TimeType, capacity);
                case ArrowTypeId.Decimal128:
                case ArrowTypeId.Decimal256:
                    return new FixedBinaryArrayBuilder(dtype as FixedWidthType, capacity);
                case ArrowTypeId.Date32:
                    return new Date32ArrayBuilder(dtype as DateType, capacity);
                case ArrowTypeId.Date64:
                    return new Date64ArrayBuilder(dtype as DateType, capacity);
                case ArrowTypeId.Interval:
                case ArrowTypeId.Union:
                case ArrowTypeId.Dictionary:
                case ArrowTypeId.Map:
                case ArrowTypeId.Null:
                default:
                    throw new ArgumentException($"Cannot create arrow array builder from {dtype.TypeId}");
            }
        }

        public static IArrowArray MakeArray(ArrayData data)
        {
            switch (data.DataType.TypeId)
            {
                case ArrowTypeId.Boolean:
                    return new BooleanArray(data);
                case ArrowTypeId.UInt8:
                    return new UInt8Array(data);
                case ArrowTypeId.Int8:
                    return new Int8Array(data);
                case ArrowTypeId.UInt16:
                    return new UInt16Array(data);
                case ArrowTypeId.Int16:
                    return new Int16Array(data);
                case ArrowTypeId.UInt32:
                    return new UInt32Array(data);
                case ArrowTypeId.Int32:
                    return new Int32Array(data);
                case ArrowTypeId.UInt64:
                    return new UInt64Array(data);
                case ArrowTypeId.Int64:
                    return new Int64Array(data);
                case ArrowTypeId.Float:
                    return new FloatArray(data);
                case ArrowTypeId.Double:
                    return new DoubleArray(data);
                case ArrowTypeId.String:
                    return new StringArray(data);
                case ArrowTypeId.FixedSizedBinary:
                    return new FixedSizeBinaryArray(data);
                case ArrowTypeId.Binary:
                    return new BinaryArray(data);
                case ArrowTypeId.Timestamp:
                    return new TimestampArray(data);
                case ArrowTypeId.List:
                    return new ListArray(data);
                case ArrowTypeId.Struct:
                    return new StructArray(data);
                case ArrowTypeId.Union:
                    return new UnionArray(data);
                case ArrowTypeId.Date64:
                    return new Date64Array(data);
                case ArrowTypeId.Date32:
                    return new Date32Array(data);
                case ArrowTypeId.Time32:
                    return new Time32Array(data);
                case ArrowTypeId.Time64:
                    return new Time64Array(data);
                case ArrowTypeId.Decimal128:
                    return new Decimal128Array(data);
                case ArrowTypeId.Decimal256:
                    return new Decimal256Array(data);
                case ArrowTypeId.Dictionary:
                    return new DictionaryArray(data);
                case ArrowTypeId.HalfFloat:
#if NET5_0_OR_GREATER
                    return new HalfFloatArray(data);
#else
                    throw new NotSupportedException("Half-float arrays are not supported by this target framework.");
#endif
                case ArrowTypeId.Interval:
                case ArrowTypeId.Map:
                case ArrowTypeId.Null:
                default:
                    throw new NotSupportedException($"An ArrowArray cannot be built for type {data.DataType.TypeId}.");
            }
        }
    }
}
