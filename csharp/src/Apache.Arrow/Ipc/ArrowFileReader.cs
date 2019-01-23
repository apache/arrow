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

using FlatBuffers;
using System;
using System.Buffers.Binary;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Apache.Arrow.Ipc
{
    public class ArrowFileReader : ArrowStreamReader
    {

        public bool IsFileValid { get; protected set; }

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

        public async Task<int> RecordBatchCountAsync()
        {
            if (!HasReadSchema)
            {
                await ReadSchemaAsync();
            }

            return _footer.RecordBatchCount;
        }

        public ArrowFileReader(Stream stream) : base(stream)
        {
            if (!stream.CanSeek)
            {
                throw new ArgumentException("Stream must be seekable.", nameof(stream));
            }

            if (!stream.CanRead)
            {
                throw new ArgumentException("Stream must be readable.");
            }

            IsFileValid = false;
        }

        public static ArrowFileReader FromFile(string filename)
        {
            var stream = new FileStream(filename, FileMode.Open);
            return new ArrowFileReader(stream);
        }

        protected override async Task<Schema> ReadSchemaAsync()
        {
            if (HasReadSchema)
            {
                return Schema;
            }

            await ValidateFileAsync();

            var bytesRead = 0;
            var footerLength = 0;

            await Buffers.RentReturnAsync(4, async (buffer) =>
            {
                BaseStream.Position = BaseStream.Length - ArrowFileConstants.Magic.Length - 4;

                bytesRead = await BaseStream.ReadAsync(buffer, 0, 4);
                footerLength = BinaryPrimitives.ReadInt32LittleEndian(buffer);

                if (bytesRead != 4) throw new InvalidDataException(
                    $"Failed to read footer length. Read <{bytesRead}>, expected 4.");

                if (footerLength <= 0) throw new InvalidDataException(
                    $"Footer length has invalid size <{footerLength}>");
            });

            await Buffers.RentReturnAsync(footerLength, async (buffer) =>
            {
                _footerStartPostion = (int)BaseStream.Length - footerLength - ArrowFileConstants.Magic.Length - 4;

                BaseStream.Position = _footerStartPostion;

                bytesRead = await BaseStream.ReadAsync(buffer, 0, footerLength);

                if (bytesRead != footerLength)
                {
                    throw new InvalidDataException(
                        $"Failed to read footer. Read <{bytesRead}> bytes, expected <{footerLength}>.");
                }

                // Deserialize the footer from the footer flatbuffer

                _footer = new ArrowFooter(Flatbuf.Footer.GetRootAsFooter(new ByteBuffer(buffer)));

                Schema = _footer.Schema;
            });

            return Schema;
        }

        public async Task<RecordBatch> ReadRecordBatchAsync(int index, CancellationToken cancellationToken = default)
        {
            await ReadSchemaAsync();

            if (index >= _footer.RecordBatchCount)
            {
                throw new ArgumentOutOfRangeException(nameof(index));
            }

            var block = _footer.GetRecordBatchBlock(index);

            BaseStream.Position = block.Offset;

            return await ReadRecordBatchAsync(cancellationToken);
        }

        public override async Task<RecordBatch> ReadNextRecordBatchAsync(CancellationToken cancellationToken = default)
        {
            await ReadSchemaAsync();

            if (_recordBatchIndex >= _footer.RecordBatchCount)
            {
                return null;
            }

            var result = await ReadRecordBatchAsync(_recordBatchIndex, cancellationToken);
            _recordBatchIndex++;

            return result;
        }

        /// <summary>
        /// Check if file format is valid. If it's valid don't run the validation again.
        /// </summary>
        private async Task ValidateFileAsync()
        {
            if (IsFileValid)
            {
                return;
            }

            await ValidateMagicAsync();

            IsFileValid = true;
        }

        private async Task ValidateMagicAsync()
        {
            var startingPosition = BaseStream.Position;
            var magicLength = ArrowFileConstants.Magic.Length;

            try
            {
                await Buffers.RentReturnAsync(magicLength, async (buffer) =>
                {
                    // Seek to the beginning of the stream

                    BaseStream.Position = 0;

                    // Read beginning of stream

                    await BaseStream.ReadAsync(buffer, 0, magicLength);

                    if (!ArrowFileConstants.Magic.SequenceEqual(buffer.Take(magicLength)))
                    {
                        throw new InvalidDataException(
                            $"Invalid magic at offset <{BaseStream.Position}>");
                    }

                    // Move stream position to magic-length bytes away from the end of the stream

                    BaseStream.Position = BaseStream.Length - magicLength;

                    // Read the end of the stream

                    await BaseStream.ReadAsync(buffer, 0, magicLength);

                    if (!ArrowFileConstants.Magic.SequenceEqual(buffer.Take(magicLength)))
                    {
                        throw new InvalidDataException(
                            $"Invalid magic at offset <{BaseStream.Position}>");
                    }
                });
            }
            finally
            {
                BaseStream.Position = startingPosition;
            }
        }
    }
}
