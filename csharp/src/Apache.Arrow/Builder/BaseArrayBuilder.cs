using System;
using System.Linq;
using Apache.Arrow.Memory;
using Apache.Arrow.Types;

namespace Apache.Arrow.Builder
{
    public abstract class BaseArrayBuilder : IArrayBuilder
    {
        public IArrowType DataType { get; }

        public int Length { get; internal set; }

        public int NullCount { get; internal set; }

        public int Offset { get; }

        public IValueBufferBuilder[] Buffers { get; }
        public IValueBufferBuilder<bool> ValidityBuffer { get; }

        public IArrayBuilder[] Children { get; }

        public IArrayBuilder Dictionary { get; }

        public BaseArrayBuilder(
            IArrowType dataType,
            IValueBufferBuilder<bool> validityBuffer,
            IValueBufferBuilder[] buffers,
            IArrayBuilder[] children = null,
            IArrayBuilder dictionary = null
        )
        {
            DataType = dataType;

            ValidityBuffer = validityBuffer;
            Buffers = buffers;
            Children = children;
            Dictionary = dictionary;

            Length = 0;
            NullCount = 0;
            Offset = 0;
        }

        public virtual IArrayBuilder AppendNull()
        {
            ValidityBuffer.AppendBit(true);
            NullCount++;
            Length++;
            return this;
        }

        internal virtual IArrayBuilder AppendNull(bool isValid)
        {
            ValidityBuffer.AppendBit(isValid);
            if (!isValid)
                NullCount++;
            Length++;
            return this;
        }

        public virtual IArrayBuilder AppendNulls(int count)
        {
            ValidityBuffer.AppendBits(new bool[count]);
            NullCount += count;
            Length += count;
            return this;
        }

        internal virtual IArrayBuilder AppendNulls(ReadOnlySpan<bool> mask)
        {
            ValidityBuffer.AppendBits(mask);

            Length += mask.Length;

            int nullCount = 0;

            foreach (bool isValid in mask)
                if (!isValid)
                    nullCount++;

            NullCount += nullCount;

            return this;
        }

        public virtual IArrayBuilder AppendValues(ArrayData data)
        {
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

                    Span<bool> bits = BitUtility.ToBits(other.Span.Slice(end)).Slice(0, data.Length - end * 8);
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
            => new ArrayData(
                DataType, Length, NullCount, Offset,
                Buffers.Select(b => b.Build()).ToArray(),
                Children?.Select(c => c.FinishInternal()).ToArray(),
                Dictionary?.FinishInternal()
            );

        public virtual IArrowArray Build(MemoryAllocator allocator = null)
            => ArrowArrayFactory.BuildArray(FinishInternal(allocator));

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
}
