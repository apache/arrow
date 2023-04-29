using System;
using System.Collections;
using System.Collections.Generic;
using Apache.Arrow.Memory;
using Apache.Arrow.Types;

namespace Apache.Arrow.Builder
{
    public interface IArrayBuilder
    {
        IArrowType DataType { get; }
        int Length { get; }
        int NullCount { get; }
        int Offset { get; }

        IValueBufferBuilder[] Buffers { get; }
        IArrayBuilder[] Children { get; }
        IArrayBuilder Dictionary { get; } // Only used for dictionary type

        IArrayBuilder AppendNull();
        IArrayBuilder AppendNulls(int count);

        IArrayBuilder AppendValue(object value);
        IArrayBuilder AppendValue(object value, bool isValid);
        IArrayBuilder AppendValues(object value, int count);
        IArrayBuilder AppendValues(IEnumerable<object> value, int batchSize = 64);
        IArrayBuilder AppendValues(ArrayData data);

        /// <summary>
        /// Clear all contents appended so far.
        /// </summary>
        IArrayBuilder Clear();

        /// <summary>
        /// Reserve a given number of items' additional capacity.
        /// </summary>
        /// <param name="capacity">Number of new values.</param>
        IArrayBuilder Reserve(int capacity);

        /// <summary>
        /// Resize the buffer to a given size.
        /// </summary>
        /// <remarks>
        /// Note that if the required capacity is larger than the current length of the populated buffer so far,
        /// the buffer's contents in the new, expanded region are undefined.
        /// </remarks>
        /// <remarks>
        /// Note that if the required capacity is smaller than the current length of the populated buffer so far,
        /// the buffer will be truncated and items at the end of the buffer will be lost.
        /// </remarks>
        /// <param name="capacity">Number of values.</param>
        IArrayBuilder Resize(int capacity);

        ArrayData FinishInternal(MemoryAllocator allocator = default);

        IArrowArray Build(MemoryAllocator allocator = default);
    }
}
