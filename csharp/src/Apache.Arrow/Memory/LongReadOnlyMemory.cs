using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;

namespace Apache.Arrow.Memory
{
    internal readonly struct LongReadOnlyMemory<T>
    {
        private readonly LongMemory<T> _memory;
        public LongReadOnlyMemory(LongMemory<T> memory)
        {
            _memory = memory;
        }

        public long Length
        {
            get { return _memory.Length; }
        }

        public T this[long index]
        {
            get
            {
                if (index < 0 || index >= Length)
                    throw new IndexOutOfRangeException("Index is out of range.");
                return _memory[index];
            }
        }

        public LongSpan<T> Span {
            get
            {
                return new LongSpan<T>(_memory);
            }
        }

        public LongReadOnlySpan<T> ReadOnlySpan
        {
            get { return new LongReadOnlySpan<T>(_memory); }
        }

    }
}
