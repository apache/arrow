using System;
using System.Collections.Generic;
using System.Linq;
using Apache.Arrow.Memory;
using Apache.Arrow.Reflection;
using Apache.Arrow.Types;

namespace Apache.Arrow.Builder
{
    public class NestedArrayBuilder : ArrayBuilder
    {
        public NestedArrayBuilder(NestedType dataType, int capacity = 8)
            : base(
                  dataType,
                  new IValueBufferBuilder[] { new ValueBufferBuilder<bool>(capacity) },
                  dataType.Fields.Select(field => ArrayBuilderFactory.MakeBuilder(field.DataType)).ToArray()
                  )
        {
        }

        internal NestedArrayBuilder(ListType dataType, int capacity = 8)
            : base(
                  dataType,
                  new IValueBufferBuilder[] { new ValueBufferBuilder<bool>(capacity), new ValueBufferBuilder<int>(capacity) },
                  dataType.Fields.Select(field => ArrayBuilderFactory.MakeBuilder(field.DataType)).ToArray()
                  )
        {
        }

        public TBuilder GetBuilderAs<TBuilder>(int index) where TBuilder : ArrayBuilder => Children[index].As<TBuilder>();
    }

    public class ListArrayBuilder : NestedArrayBuilder
    {
        public int CurrentOffset { get; private set; }

        public ListArrayBuilder(ListType dataType, int capacity = 8) : base(dataType, capacity)
        {
            CurrentOffset = 0;
            OffsetsBuffer.AppendValue(CurrentOffset);
        }

        public IPrimitiveBufferBuilder<int> OffsetsBuffer => Buffers[1] as IPrimitiveBufferBuilder<int>;

        public FixedBinaryArrayBuilder GetBuilderAs<T>() where T : struct
            => GetBuilderAs<FixedBinaryArrayBuilder>(0);

        // Append Valididty
        public virtual ListArrayBuilder Append()
        {
            AppendValid();
            return this;
        }
        public override IArrayBuilder AppendNull() => AppendNull(false);
        public virtual ListArrayBuilder AppendNull(bool recursive = false)
        {
            OffsetsBuffer.AppendValue(CurrentOffset);
            base.AppendNull();
            return this;
        }

        public override IArrayBuilder AppendNulls(int count) => AppendNulls(count, false);
        public virtual ListArrayBuilder AppendNulls(int count, bool recursive = false)
        {
            OffsetsBuffer.AppendValues(CurrentOffset, count);
            base.AppendNulls(count);
            return this;
        }

        // Append Value
        public ListArrayBuilder AppendValue<T>(ReadOnlySpan<T> value) where T : struct
        {
            CurrentOffset++;
            OffsetsBuffer.AppendValue(CurrentOffset);
            GetBuilderAs<T>().AppendValues(value);
            AppendValid();

            return this;
        }

        public virtual ListArrayBuilder AppendValues<T>(ICollection<T[]> values) where T : struct
        {
            Span<T> memory = new T[values.Sum(row => row.Length)];
            Span<int> offsets = new int[values.Count];
            Span<bool> mask = new bool[offsets.Length];
            int offset = 0;
            int i = 0;
            bool allValid = true;
            int nullCount = 0;

            foreach (T[] value in values)
            {
                if (value == null)
                {
                    offsets[i] = CurrentOffset;
                    // default is already false
                    // mask[i] = false;
                    allValid = false;
                    nullCount++;
                }
                else
                {
                    // Copy to memory
                    value.CopyTo(memory.Slice(offset, value.Length));

                    offset += value.Length;
                    CurrentOffset += value.Length;

                    // Fill other buffers
                    offsets[i] = CurrentOffset;
                    mask[i] = true;
                }
                i++;
            }

            return AppendValues<T>(memory, offsets, mask, allValid, nullCount);
        }

        internal virtual ListArrayBuilder AppendValues<T>(
            ReadOnlySpan<T> values, ReadOnlySpan<int> offsets, ReadOnlySpan<bool> mask, bool allValid, int nullCount
            ) where T : struct
        {
            if (allValid)
                AppendValidity(true, mask.Length);
            else
                AppendValidity(mask, nullCount);

            // Append Offset
            CurrentOffset = offsets[offsets.Length - 1];
            OffsetsBuffer.AppendValues(offsets);

            // Append Values
            GetBuilderAs<T>().AppendValues(values);
            return this;
        }

        public override IArrowArray Build(MemoryAllocator allocator = default) => Build(allocator);

        public ListArray Build(MemoryAllocator allocator = default, bool _ = true)
            => new ListArray(FinishInternal(allocator));
    }

    public class StructArrayBuilder : NestedArrayBuilder
    {
        public StructArrayBuilder(StructType dataType, int capacity = 8) : base(dataType, capacity)
        {
        }

        // Append Valididty
        public virtual StructArrayBuilder Append()
        {
            AppendValid();
            return this;
        }

        public override IArrayBuilder AppendNull() => AppendNull(true);

        public virtual StructArrayBuilder AppendNull(bool recursive = true)
        {
            base.AppendNull();

            foreach (IArrayBuilder child in Children)
                child.AppendNull();

            return this;
        }

        public override IArrayBuilder AppendNulls(int count) => AppendNulls(count, true);
        public virtual StructArrayBuilder AppendNulls(int count, bool recursive = true)
        {
            base.AppendNulls(count);

            foreach (IArrayBuilder child in Children)
                child.AppendNulls(count);

            return this;
        }

        public override IArrowArray Build(MemoryAllocator allocator = default) => Build(allocator);

        public StructArray Build(MemoryAllocator allocator = default, bool _ = true)
            => new StructArray(FinishInternal(allocator));
    }

    public class StructArrayBuilder<T> : StructArrayBuilder where T : struct
    {
        public StructArrayBuilder(int capacity = 8) : base(TypeReflection<T>.ArrowType as StructType, capacity)
        {
        }
    }
}

