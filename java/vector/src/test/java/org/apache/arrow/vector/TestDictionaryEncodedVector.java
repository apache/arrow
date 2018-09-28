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

package org.apache.arrow.vector;

import static java.nio.channels.Channels.newChannel;
import static java.util.Arrays.asList;
import static org.apache.arrow.vector.TestUtils.newVarCharVector;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Vector;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestDictionaryEncodedVector {

  private BufferAllocator allocator;

  @Before
  public void init() {
    allocator = new RootAllocator(Long.MAX_VALUE);
  }

  @After
  public void terminate() throws Exception {
    allocator.close();
  }

  @Test
  public void testDicionaryEncodedVector() throws IOException {

    // Create the dictionary
    VarCharVector dictVector = (VarCharVector)
        FieldType.nullable(new ArrowType.Utf8())
            .createNewSingleVector("enum1_dict", allocator, null);
    dictVector.allocateNew();
    dictVector.set(0, "foo".getBytes(StandardCharsets.UTF_8));
    dictVector.set(1, "bar".getBytes(StandardCharsets.UTF_8));
    dictVector.setValueCount(2);

    DictionaryEncoding enum1Encoding = new DictionaryEncoding(
        0,
        true,
        new ArrowType.Int(8, true)
    );

    DictionaryProvider dictionaryProvider = new DictionaryProvider.MapDictionaryProvider(
        new Dictionary(dictVector, enum1Encoding)
    );

    // Create the dictionary encoded vector
    Schema schema = new Schema(
        asList(
            new Field(
                "enum1",
                true,
                // this is the type of the decoded value
                new ArrowType.Utf8(),
                enum1Encoding,
                Collections.<Field>emptyList())
    ));

    // I expected this to work but it doesn't
    // VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
    // TinyIntVector vector = (TinyIntVector) root.getVector("enum1");

    // This is the encoded vector
    TinyIntVector vector = new TinyIntVector("enum1", allocator);
    vector.allocateNew();
    vector.set(0, 1);
    vector.set(1, 0);
    vector.set(2, 1);
    vector.setValueCount(3);
    VectorSchemaRoot root = new VectorSchemaRoot(schema, asList(vector), 3);


    // Test round trip
    // ByteArrayOutputStream out = new ByteArrayOutputStream();
    FileOutputStream out = new FileOutputStream("/tmp/dictionary_encoded.arrow");
    ArrowStreamWriter writer = new ArrowStreamWriter(root, dictionaryProvider, newChannel(out));

    writer.start();
    writer.writeBatch();
    writer.close();

    dictVector.close();
    root.close();

    // byte[] data = out.toByteArray();
    out.close();

    // Read
    //ByteArrayInputStream in = new ByteArrayInputStream(data);

    FileInputStream in = new FileInputStream("/tmp/dictionary_encoded.arrow");
    ArrowStreamReader reader = new ArrowStreamReader(in, allocator);
    root = reader.getVectorSchemaRoot();
    reader.loadNextBatch();
    System.out.println(root.contentToTSVString());

    reader.close();
    root.close();
  }
}
