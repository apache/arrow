namespace Apache.Arrow.Nullable
{
    using global::Apache.Arrow;
    using System;

    public class ValidityBitmap
    {
        private Memory<byte> memory;

        private int count;

        public ValidityBitmap(int count)
        {
            if (count <= 0)
            {
                throw new ArgumentException("count must be positive integer", nameof(count));
            }

            this.count = count;

            this.NullCount = count;

            var nullbytes = (int)Math.Ceiling(count / 8.0);
            memory = new Memory<byte>(new byte[nullbytes]);
        }


        public int NullCount { get; private set; }

        public ArrowBuffer Build()
        {
            if (this.NullCount == 0)
            {
                return ArrowBuffer.Empty;
            }

            return new ArrowBuffer(this.memory.ToArray());
        }

        public void SetValid(int index)
        {
            if (index >= count || index < 0)
            {
                throw new IndexOutOfRangeException($"capacity:{count} index:{index}");
            }

            var isValid = BitUtility.GetBit(this.memory.Span, index);

            if (!isValid)
            {
                BitUtility.SetBit(this.memory.Span, index);
                --NullCount;
            }
        }
    }
}