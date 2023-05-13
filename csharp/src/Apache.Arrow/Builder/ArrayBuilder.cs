using System;
using System.Linq;
using System.Threading.Tasks;
using Apache.Arrow.Arrays;
using Apache.Arrow.Flatbuf;
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
        internal bool Validate(IArrowType type0, IArrowType type1)
            => type0.TypeId == type1.TypeId;

        internal bool Validate(IArrowType dtype)
        {
            if (!Validate(DataType, dtype))
                return false;

            switch (dtype.TypeId)
            {
                // Nested types
                case ArrowTypeId.List:
                case ArrowTypeId.Struct:
                case ArrowTypeId.Map:
                    var currentFields = (DataType as NestedType).Fields;
                    var nestedFields = (dtype as NestedType).Fields;

                    for (int i = 0; i < currentFields.Count; i++)
                        if (!Validate(currentFields.ElementAt(i).DataType, nestedFields.ElementAt(i).DataType))
                            return false;
                    return true;
                default:
                    return true;
            }
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
                    throw new ArgumentException($"Cannot append {value} in builder with data type {DataType}");
            }
        }

        public virtual Status AppendArray(IArrowArray array) => AppendArray(array.Data);

        public async Task<Status> AppendArrayAsync(IArrowArray data) => await AppendArrayAsync(data);

        private Status AppendValidity(ArrayData data)
        {
            // Handle validity
            var dataValidity = data.Buffers[ValidityBufferIndex];

            // Check if need to recalculate null count
            if (data.NullCount < 0)
            {
                Span<bool> bits = new bool[data.Length];
                int nullCount = 0;

                // Need recalculate nulls
                for (int i = 0; i < data.Length; i++)
                {
                    bool isValid = BitUtility.GetBit(dataValidity.Span, data.Offset + i);

                    if (isValid)
                        bits[i] = isValid;
                    else
                        nullCount++;
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

                    for (int i = 0; i < data.Length; i++)
                        bits[i] = BitUtility.GetBit(dataValidity.Span, data.Offset + i);

                    AppendValidity(bits, nullCount);
                }
            }
            return Status.OK;
        }

        internal abstract Status AppendPrimitiveValueOffset(ArrayData data);

        public Status AppendArray(ArrayData data)
        {
            // TODO: Make better / recursive fields data type check
            if (!Validate(data.DataType))
                throw new ArgumentException($"Cannot append Arrow.Array<{data.DataType}> in {this}<{DataType}>");

            // Add Length and reserve
            Reserve(data.Length);

            AppendValidity(data);
            AppendPrimitiveValueOffset(data);

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

        public async Task<Status> AppendArrayAsync(ArrayData data)
        {
            // TODO: Make better / recursive fields data type check
            if (!Validate(data.DataType))
                throw new ArgumentException($"Cannot append Arrow.Array<{data.DataType}> in {this}<{DataType}>");

            // Add Length and reserve
            Reserve(data.Length);

            AppendValidity(data);
            AppendPrimitiveValueOffset(data);

            // Append children data
            if (Children != null && data.Children != null)
            {
                Task[] childTasks = new Task[Children.Length];
                for (int i = 0; i < Children.Length; i++)
                {
                    int index = i; // Create a local copy of the loop variable
                    childTasks[index] = Children[index].AppendArrayAsync(data.Children[index]);
                }
                await Task.WhenAll(childTasks);
            }

            if (Dictionary != null && data.Dictionary != null)
            {
                await Dictionary.AppendArrayAsync(data.Dictionary);
            }

            return Status.OK;
        }

        public ArrayData FinishInternal(MemoryAllocator allocator = null)
            => new ArrayData(
                DataType, Length, NullCount, Offset,
                Buffers.Select(b => b.Build(allocator)).ToArray(),
                Children?.Select(c => c.FinishInternal(allocator)).ToArray(),
                Dictionary?.FinishInternal()
            );

        public async Task<ArrayData> FinishInternalAsync(MemoryAllocator allocator = null)
            => new ArrayData(
                DataType, Length, NullCount, Offset,
                await Task.WhenAll(Buffers.Select(b => b.BuildAsync(allocator))),
                Children != null ? await Task.WhenAll(Children.Select(c => c.FinishInternalAsync(allocator))) : null,
                Dictionary != null ? await Dictionary.FinishInternalAsync() : null
            );

        public virtual IArrowArray Build(MemoryAllocator allocator = null)
            => ArrayBuilderFactory.MakeArray(FinishInternal(allocator));

        public virtual async Task<IArrowArray> BuildAsync(MemoryAllocator allocator = null)
            => ArrayBuilderFactory.MakeArray(await FinishInternalAsync(allocator));

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
