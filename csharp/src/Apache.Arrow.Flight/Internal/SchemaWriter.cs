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

using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Flatbuf;
using Apache.Arrow.Flight.Internal;
using Apache.Arrow.Ipc;
using Google.Protobuf;

namespace Apache.Arrow.Flight.Internal
{
    /// <summary>
    /// This class handles writing schemas
    /// </summary>
    internal class SchemaWriter : ArrowStreamWriter
    {
        internal SchemaWriter(Stream baseStream, Schema schema) : base(baseStream, schema)
        {
        }

        public void WriteSchema(Schema schema, CancellationToken cancellationToken)
        {
            var offset = base.SerializeSchema(schema);
            WriteMessage(MessageHeader.Schema, offset, 0);
        }

        public static ByteString SerializeSchema(Schema schema, CancellationToken cancellationToken = default(CancellationToken))
        {
            using(var memoryStream = new MemoryStream())
            {
                var writer = new SchemaWriter(memoryStream, schema);
                writer.WriteSchema(schema, cancellationToken);

                memoryStream.Position = 0;
                return ByteString.FromStream(memoryStream);
            }
        }
    }
}

public static class SchemaExtension
{
    // Translate an Apache.Arrow.Schema to FlatBuffer Schema to ByteString
    public static ByteString ToByteString(this Apache.Arrow.Schema schema)
    {
        return SchemaWriter.SerializeSchema(schema);
    }
}
