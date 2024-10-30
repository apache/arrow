using System;
using System.Collections.Generic;
using System.Text;

namespace Apache.Arrow.Memory
{
    internal readonly struct LongReadOnlySpan<T>
    {
        private readonly LongMemory<T> _memory;

        public LongReadOnlySpan(LongMemory<T> memory)
        {
            _memory = memory;
        }

        public long length
        {
            get { return _memory.Length; }
        }

        public T this[long index]
        {
            get
            {
                if (index < 0 || index >= _memory.Length)
                {
                    throw new IndexOutOfRangeException("Index is out of range.");
                }
                return _memory[index];
            }
        }

        public Span<T> AsSpan()
        {
            return _memory.Span;
        }

    }
}
