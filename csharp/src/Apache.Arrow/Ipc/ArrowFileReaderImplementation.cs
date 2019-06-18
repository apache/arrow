// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using Apache.Arrow.Memory;
using System;
using System.Buffers;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Apache.Arrow.Ipc
{
    internal sealed class ArrowFileReaderImplementation : ArrowStreamReaderImplementation
    {
        public bool IsFileValid { get; private set; }

        /// <summary>
        /// When using GetNextRecordBatch this value 
        /// is to remember what index is next
        /// </summary>
        private int _recordBatchIndex;

        /// <summary>
        /// Notes what byte position where the footer data is in the stream
        /// </summary>
        private int _footerStartPostion;

        private ArrowFooter _footer;

        public ArrowFileReaderImplementation(Stream stream, MemoryAllocator allocator, bool leaveOpen)
            : base(stream, allocator, leaveOpen)
        {
        }

        public async ValueTask<int> RecordBatchCountAsync()
        {
            if (!HasReadSchema)
            {
                await ReadSchemaAsync().ConfigureAwait(false);
            }

            return _footer.RecordBatchCount;
        }

        protected override async ValueTask ReadSchemaAsync()
        {
            if (HasReadSchema)
            {
                return;
            }

            await ValidateFileAsync().ConfigureAwait(false);

            int footerLength = 0;
            await ArrayPool<byte>.Shared.RentReturnAsync(4, async (buffer) =>
            {
                BaseStream.Position = GetFooterLengthPosition();

                int bytesRead = await BaseStream.ReadFullBufferAsync(buffer).ConfigureAwait(false);
                EnsureFullRead(buffer, bytesRead);

                footerLength = ReadFooterLength(buffer);
            }).ConfigureAwait(false);

            await ArrayPool<byte>.Shared.RentReturnAsync(footerLength, async (buffer) =>
            {
                _footerStartPostion = (int)GetFooterLengthPosition() - footerLength;

                BaseStream.Position = _footerStartPostion;

                int bytesRead = await BaseStream.ReadFullBufferAsync(buffer).ConfigureAwait(false);
                EnsureFullRead(buffer, bytesRead);

                ReadSchema(buffer);
            }).ConfigureAwait(false);
        }

        protected override void ReadSchema()
        {
            if (HasReadSchema)
            {
                return;
            }

            ValidateFile();

            int footerLength = 0;
            ArrayPool<byte>.Shared.RentReturn(4, (buffer) =>
            {
                BaseStream.Position = GetFooterLengthPosition();

                int bytesRead = BaseStream.ReadFullBuffer(buffer);
                EnsureFullRead(buffer, bytesRead);

                footerLength = ReadFooterLength(buffer);
            });

            ArrayPool<byte>.Shared.RentReturn(footerLength, (buffer) =>
            {
                _footerStartPostion = (int)GetFooterLengthPosition() - footerLength;

                BaseStream.Position = _footerStartPostion;

                int bytesRead = BaseStream.ReadFullBuffer(buffer);
                EnsureFullRead(buffer, bytesRead);

                ReadSchema(buffer);
            });
        }

        private long GetFooterLengthPosition()
        {
            return BaseStream.Length - ArrowFileConstants.Magic.Length - 4;
        }

        private static int ReadFooterLength(Memory<byte> buffer)
        {
            int footerLength = BitUtility.ReadInt32(buffer);

            if (footerLength <= 0)
                throw new InvalidDataException(
                    $"Footer length has invalid size <{footerLength}>");

            return footerLength;
        }

        private void ReadSchema(Memory<byte> buffer)
        {
            // Deserialize the footer from the footer flatbuffer
            _footer = new ArrowFooter(Flatbuf.Footer.GetRootAsFooter(CreateByteBuffer(buffer)));

            Schema = _footer.Schema;
        }

        public async ValueTask<RecordBatch> ReadRecordBatchAsync(int index, CancellationToken cancellationToken)
        {
            await ReadSchemaAsync().ConfigureAwait(false);

            if (index >= _footer.RecordBatchCount)
            {
                throw new ArgumentOutOfRangeException(nameof(index));
            }

            var block = _footer.GetRecordBatchBlock(index);

            BaseStream.Position = block.Offset;

            return await ReadRecordBatchAsync(cancellationToken).ConfigureAwait(false);
        }

        public RecordBatch ReadRecordBatch(int index)
        {
            ReadSchema();

            if (index >= _footer.RecordBatchCount)
            {
                throw new ArgumentOutOfRangeException(nameof(index));
            }

            var block = _footer.GetRecordBatchBlock(index);

            BaseStream.Position = block.Offset;

            return ReadRecordBatch();
        }

        public override async ValueTask<RecordBatch> ReadNextRecordBatchAsync(CancellationToken cancellationToken)
        {
            await ReadSchemaAsync().ConfigureAwait(false);

            if (_recordBatchIndex >= _footer.RecordBatchCount)
            {
                return null;
            }

            var result = await ReadRecordBatchAsync(_recordBatchIndex, cancellationToken).ConfigureAwait(false);
            _recordBatchIndex++;

            return result;
        }

        public override RecordBatch ReadNextRecordBatch()
        {
            ReadSchema();

            if (_recordBatchIndex >= _footer.RecordBatchCount)
            {
                return null;
            }

            RecordBatch result = ReadRecordBatch(_recordBatchIndex);
            _recordBatchIndex++;

            return result;
        }

        /// <summary>
        /// Check if file format is valid. If it's valid don't run the validation again.
        /// </summary>
        private async ValueTask ValidateFileAsync()
        {
            if (IsFileValid)
            {
                return;
            }

            await ValidateMagicAsync().ConfigureAwait(false);

            IsFileValid = true;
        }

        /// <summary>
        /// Check if file format is valid. If it's valid don't run the validation again.
        /// </summary>
        private void ValidateFile()
        {
            if (IsFileValid)
            {
                return;
            }

            ValidateMagic();

            IsFileValid = true;
        }

        private async ValueTask ValidateMagicAsync()
        {
            var startingPosition = BaseStream.Position;
            var magicLength = ArrowFileConstants.Magic.Length;

            try
            {
                await ArrayPool<byte>.Shared.RentReturnAsync(magicLength, async (buffer) =>
                {
                    // Seek to the beginning of the stream
                    BaseStream.Position = 0;

                    // Read beginning of stream
                    await BaseStream.ReadAsync(buffer).ConfigureAwait(false);

                    VerifyMagic(buffer);

                    // Move stream position to magic-length bytes away from the end of the stream
                    BaseStream.Position = BaseStream.Length - magicLength;

                    // Read the end of the stream
                    await BaseStream.ReadAsync(buffer).ConfigureAwait(false);

                    VerifyMagic(buffer);
                }).ConfigureAwait(false);
            }
            finally
            {
                BaseStream.Position = startingPosition;
            }
        }

        private void ValidateMagic()
        {
            var startingPosition = BaseStream.Position;
            var magicLength = ArrowFileConstants.Magic.Length;

            try
            {
                ArrayPool<byte>.Shared.RentReturn(magicLength, buffer =>
                {
                    // Seek to the beginning of the stream
                    BaseStream.Position = 0;

                    // Read beginning of stream
                    BaseStream.Read(buffer);

                    VerifyMagic(buffer);

                    // Move stream position to magic-length bytes away from the end of the stream
                    BaseStream.Position = BaseStream.Length - magicLength;

                    // Read the end of the stream
                    BaseStream.Read(buffer);

                    VerifyMagic(buffer);
                });
            }
            finally
            {
                BaseStream.Position = startingPosition;
            }
        }

        private void VerifyMagic(Memory<byte> buffer)
        {
            if (!ArrowFileConstants.Magic.AsSpan().SequenceEqual(buffer.Span))
            {
                throw new InvalidDataException(
                    $"Invalid magic at offset <{BaseStream.Position}>");
            }
        }
    }
}
