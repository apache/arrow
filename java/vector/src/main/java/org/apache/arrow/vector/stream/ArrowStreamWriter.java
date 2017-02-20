/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.arrow.vector.stream;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.file.ArrowBlock;
import org.apache.arrow.vector.file.ArrowWriter;
import org.apache.arrow.vector.file.WriteChannel;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.WritableByteChannel;
import java.util.List;

public class ArrowStreamWriter extends ArrowWriter {

    public ArrowStreamWriter(Schema schema, OutputStream out, BufferAllocator allocator) {
        super(schema, out, allocator);
    }

    public ArrowStreamWriter(Schema schema, WritableByteChannel out, BufferAllocator allocator) {
        super(schema, out, allocator);
    }

    public ArrowStreamWriter(List<Field> fields, List<FieldVector> vectors, OutputStream out) {
       super(fields, vectors, out);
    }

    public ArrowStreamWriter(List<Field> fields, List<FieldVector> vectors, WritableByteChannel out) {
       super(fields, vectors, out, false);
    }

    @Override
    protected void startInternal(WriteChannel out) throws IOException {}

    @Override
    protected void endInternal(WriteChannel out,
                               List<ArrowBlock> dictionaries,
                               List<ArrowBlock> records) throws IOException {
       out.writeIntLittleEndian(0);
    }
}

