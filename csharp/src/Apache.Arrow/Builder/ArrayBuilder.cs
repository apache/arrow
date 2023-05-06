using System;
using System.Linq;
using System.Threading.Tasks;
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
        public IValueBufferBuilder ValidityBuffer => Buffers[0];

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

        public virtual IArrayBuilder AppendNull()
        {
            ValidityBuffer.AppendBit(false);
            NullCount++;
            Length++;
            return this;
        }

        public virtual IArrayBuilder AppendNulls(int count)
        {
            Reserve(count);
            ValidityBuffer.AppendBits(false, count);
            NullCount += count;
            Length += count;
            return this;
        }

        internal virtual IArrayBuilder AppendValid()
        {
            ValidityBuffer.AppendBit(true);
            Length++;
            return this;
        }

        internal virtual IArrayBuilder AppendValidity(bool isValid) => isValid ? AppendValid() : AppendNull();
        internal virtual IArrayBuilder AppendValidity(bool isValid, int count)
        {
            ValidityBuffer.AppendBits(isValid, count);
            Length += count;
            if (!isValid)
                NullCount += count;
            return this;
        }
        internal virtual IArrayBuilder AppendValidity(ReadOnlySpan<bool> mask, int nullCount)
        {
            if (nullCount == mask.Length)
                ValidityBuffer.AppendBits(false, nullCount);
            else
                ValidityBuffer.AppendBits(mask);

            Length += mask.Length;

            NullCount += nullCount;

            return this;
        }
        internal virtual IArrayBuilder AppendValidity(ReadOnlySpan<bool> mask)
        {
            int nullCount = 0;

            foreach (bool isValid in mask)
                if (!isValid)
                    nullCount++;

            return AppendValidity(mask, nullCount);
        }

        public virtual IArrayBuilder AppendValues(IArrowArray array) => AppendValues(array.Data);

        public virtual IArrayBuilder AppendValues(ArrayData data)
        {
            // TODO: Make better / recursive fields data type check
            if (data.DataType.TypeId != DataType.TypeId)
                throw new ArgumentException($"Cannot append data type {data.DataType} in builder with data type {DataType}");

            NullCount += data.NullCount;
            Length += data.Length;

            Reserve(data.Length);

            for (int i = 0; i < Buffers.Length; i++)
            {
                IValueBufferBuilder current = Buffers[i];
                ArrowBuffer other = data.Buffers[i];

                if (current.ValueBitSize % 8 == 0)
                {
                    // Full byte encoded
                    current.AppendBytes(other.Span);
                }
                else
                {
                    // Safe copy Bytes and remaining bits
                    int end = (data.Length * current.ValueBitSize) / 8;

                    current.AppendBytes(other.Span.Slice(0, end));

                    Span<bool> bits = BitUtility.BytesToBits(other.Span.Slice(end)).Slice(0, data.Length - end * 8);
                    current.AppendBits(bits);
                }
            }

            if (Children != null && data.Children != null)
            {
                for (int i = 0; i < Children.Length; i++)
                {
                    Children[i].AppendValues(data.Children[i]);
                }
            }

            if (Dictionary != null && data.Dictionary != null)
            {
                Dictionary.AppendValues(data.Dictionary);
            }

            return this;
        }

        internal static Span<bool> ValidityMask(int count, bool isValid)
        {
            Span<bool> values = new bool[count]; // create a new bool array of length count

            for (int i = 0; i < count; i++)
            {
                values[i] = isValid; // set each value in the array to isValid
            }

            return values;
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
        public IArrayBuilder Reserve(int capacity)
        {
            foreach (IValueBufferBuilder buffer in Buffers)
                buffer.Reserve(capacity);

            if (Children != null)
                foreach (IArrayBuilder builder in Children)
                    builder.Reserve(capacity);

            if (Dictionary != null)
                Dictionary.Reserve(capacity);

            return this;
        }

        public IArrayBuilder Resize(int capacity)
        {
            foreach (IValueBufferBuilder buffer in Buffers)
                buffer.Resize(capacity);

            if (Children != null)
                foreach (IArrayBuilder builder in Children)
                    builder.Resize(capacity);

            if (Dictionary != null)
                Dictionary.Resize(capacity);

            return this;
        }

        public IArrayBuilder Clear()
        {
            foreach (IValueBufferBuilder buffer in Buffers)
                buffer.Clear();

            if (Children != null)
                foreach (IArrayBuilder builder in Children)
                    builder.Clear();

            if (Dictionary != null)
                Dictionary.Clear();

            return this;
        }
    }

    public static class ArrayBuilderFactory
    {
        public static IArrayBuilder MakeBuilder(IArrowType dtype, int capacity = 64)
        {
            switch (dtype.TypeId)
            {
                case ArrowTypeId.Boolean:
                    return new FixedBinaryArrayBuilder(dtype as FixedWidthType, capacity);
                case ArrowTypeId.UInt8:
                    return new FixedBinaryArrayBuilder(dtype as FixedWidthType, capacity);
                case ArrowTypeId.Int8:
                    return new FixedBinaryArrayBuilder(dtype as FixedWidthType, capacity);
                case ArrowTypeId.UInt16:
                    return new FixedBinaryArrayBuilder(dtype as FixedWidthType, capacity);
                case ArrowTypeId.Int16:
                    return new FixedBinaryArrayBuilder(dtype as FixedWidthType, capacity);
                case ArrowTypeId.UInt32:
                    return new FixedBinaryArrayBuilder(dtype as FixedWidthType, capacity);
                case ArrowTypeId.Int32:
                    return new FixedBinaryArrayBuilder(dtype as FixedWidthType, capacity);
                case ArrowTypeId.UInt64:
                    return new FixedBinaryArrayBuilder(dtype as FixedWidthType, capacity);
                case ArrowTypeId.Int64:
                    return new FixedBinaryArrayBuilder(dtype as FixedWidthType, capacity);
#if NET5_0_OR_GREATER
                case ArrowTypeId.HalfFloat:
                    return new FixedBinaryArrayBuilder(dtype as FixedWidthType, capacity);
#endif
                case ArrowTypeId.Float:
                    return new FixedBinaryArrayBuilder(dtype as FixedWidthType, capacity);
                case ArrowTypeId.Double:
                    return new FixedBinaryArrayBuilder(dtype as FixedWidthType, capacity);
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
