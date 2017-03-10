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
package org.apache.arrow.tools;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.google.common.collect.ImmutableList;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.NullableIntVector;
import org.apache.arrow.vector.NullableTinyIntVector;
import org.apache.arrow.vector.NullableVarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.dictionary.DictionaryProvider.MapDictionaryProvider;
import org.apache.arrow.vector.stream.ArrowStreamReader;
import org.apache.arrow.vector.stream.ArrowStreamWriter;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.ArrowType.Int;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class EchoServerTest {

  private static EchoServer server;
  private static int serverPort;
  private static Thread serverThread;

  @BeforeClass
  public static void startEchoServer() throws IOException {
    server = new EchoServer(0);
    serverPort = server.port();
    serverThread = new Thread() {
      @Override
      public void run() {
        try {
          server.run();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    };
    serverThread.start();
  }

  @AfterClass
  public static void stopEchoServer() throws IOException, InterruptedException {
    server.close();
    serverThread.join();
  }

  private void testEchoServer(int serverPort,
                              Field field,
                              NullableTinyIntVector vector,
                              int batches)
      throws UnknownHostException, IOException {
    BufferAllocator alloc = new RootAllocator(Long.MAX_VALUE);
    VectorSchemaRoot root = new VectorSchemaRoot(asList(field), asList((FieldVector) vector), 0);
    try (Socket socket = new Socket("localhost", serverPort);
        ArrowStreamWriter writer = new ArrowStreamWriter(root, null, socket.getOutputStream());
        ArrowStreamReader reader = new ArrowStreamReader(socket.getInputStream(), alloc)) {
      writer.start();
      for (int i = 0; i < batches; i++) {
        vector.allocateNew(16);
        for (int j = 0; j < 8; j++) {
          vector.getMutator().set(j, j + i);
          vector.getMutator().set(j + 8, 0, (byte) (j + i));
        }
        vector.getMutator().setValueCount(16);
        root.setRowCount(16);
        writer.writeBatch();
      }
      writer.end();

      assertEquals(new Schema(asList(field)), reader.getVectorSchemaRoot().getSchema());

      NullableTinyIntVector readVector = (NullableTinyIntVector) reader.getVectorSchemaRoot().getFieldVectors().get(0);
      for (int i = 0; i < batches; i++) {
        reader.loadNextBatch();
        assertEquals(16, reader.getVectorSchemaRoot().getRowCount());
        assertEquals(16, readVector.getAccessor().getValueCount());
        for (int j = 0; j < 8; j++) {
          assertEquals(j + i, readVector.getAccessor().get(j));
          assertTrue(readVector.getAccessor().isNull(j + 8));
        }
      }
      reader.loadNextBatch();
      assertEquals(0, reader.getVectorSchemaRoot().getRowCount());
      assertEquals(reader.bytesRead(), writer.bytesWritten());
    }
  }

  @Test
  public void basicTest() throws InterruptedException, IOException {
    BufferAllocator alloc = new RootAllocator(Long.MAX_VALUE);

    Field field = new Field("testField", true, new ArrowType.Int(8, true), Collections.<Field>emptyList());
    NullableTinyIntVector vector = new NullableTinyIntVector("testField", alloc, null);
    Schema schema = new Schema(asList(field));

    // Try an empty stream, just the header.
    testEchoServer(serverPort, field, vector, 0);

    // Try with one batch.
    testEchoServer(serverPort, field, vector, 1);

    // Try with a few
    testEchoServer(serverPort, field, vector, 10);
  }

  @Test
  public void testFlatDictionary() throws IOException {
    DictionaryEncoding writeEncoding = new DictionaryEncoding(1L, false, null);
    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
         NullableIntVector writeVector = new NullableIntVector("varchar", allocator, writeEncoding);
         NullableVarCharVector writeDictionaryVector = new NullableVarCharVector("dict", allocator, null)) {
      writeVector.allocateNewSafe();
      NullableIntVector.Mutator mutator = writeVector.getMutator();
      mutator.set(0, 0);
      mutator.set(1, 1);
      mutator.set(3, 2);
      mutator.set(4, 1);
      mutator.set(5, 2);
      mutator.setValueCount(6);

      writeDictionaryVector.allocateNewSafe();
      NullableVarCharVector.Mutator dictionaryMutator = writeDictionaryVector.getMutator();
      dictionaryMutator.set(0, "foo".getBytes(StandardCharsets.UTF_8));
      dictionaryMutator.set(1, "bar".getBytes(StandardCharsets.UTF_8));
      dictionaryMutator.set(2, "baz".getBytes(StandardCharsets.UTF_8));
      dictionaryMutator.setValueCount(3);

      List<Field> fields = ImmutableList.of(writeVector.getField());
      List<FieldVector> vectors = ImmutableList.of((FieldVector) writeVector);
      VectorSchemaRoot root = new VectorSchemaRoot(fields, vectors, 6);

      DictionaryProvider writeProvider = new MapDictionaryProvider(new Dictionary(writeDictionaryVector, writeEncoding));

      try (Socket socket = new Socket("localhost", serverPort);
           ArrowStreamWriter writer = new ArrowStreamWriter(root, writeProvider, socket.getOutputStream());
           ArrowStreamReader reader = new ArrowStreamReader(socket.getInputStream(), allocator)) {
        writer.start();
        writer.writeBatch();
        writer.end();

        reader.loadNextBatch();
        VectorSchemaRoot readerRoot = reader.getVectorSchemaRoot();
        Assert.assertEquals(6, readerRoot.getRowCount());

        FieldVector readVector = readerRoot.getFieldVectors().get(0);
        Assert.assertNotNull(readVector);

        DictionaryEncoding readEncoding = readVector.getField().getDictionary();
        Assert.assertNotNull(readEncoding);
        Assert.assertEquals(1L, readEncoding.getId());

        FieldVector.Accessor accessor = readVector.getAccessor();
        Assert.assertEquals(6, accessor.getValueCount());
        Assert.assertEquals(0, accessor.getObject(0));
        Assert.assertEquals(1, accessor.getObject(1));
        Assert.assertEquals(null, accessor.getObject(2));
        Assert.assertEquals(2, accessor.getObject(3));
        Assert.assertEquals(1, accessor.getObject(4));
        Assert.assertEquals(2, accessor.getObject(5));

        Dictionary dictionary = reader.lookup(1L);
        Assert.assertNotNull(dictionary);
        NullableVarCharVector.Accessor dictionaryAccessor = ((NullableVarCharVector) dictionary.getVector()).getAccessor();
        Assert.assertEquals(3, dictionaryAccessor.getValueCount());
        Assert.assertEquals(new Text("foo"), dictionaryAccessor.getObject(0));
        Assert.assertEquals(new Text("bar"), dictionaryAccessor.getObject(1));
        Assert.assertEquals(new Text("baz"), dictionaryAccessor.getObject(2));
      }
    }
  }

  @Test
  public void testNestedDictionary() throws IOException {
    DictionaryEncoding writeEncoding = new DictionaryEncoding(2L, false, null);
    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
         NullableVarCharVector writeDictionaryVector = new NullableVarCharVector("dictionary", allocator, null);
         ListVector writeVector = new ListVector("list", allocator, null, null)) {

      // data being written:
      // [['foo', 'bar'], ['foo'], ['bar']] -> [[0, 1], [0], [1]]

      writeDictionaryVector.allocateNew();
      writeDictionaryVector.getMutator().set(0, "foo".getBytes(StandardCharsets.UTF_8));
      writeDictionaryVector.getMutator().set(1, "bar".getBytes(StandardCharsets.UTF_8));
      writeDictionaryVector.getMutator().setValueCount(2);

      writeVector.addOrGetVector(MinorType.INT, writeEncoding);
      writeVector.allocateNew();
      UnionListWriter listWriter = new UnionListWriter(writeVector);
      listWriter.startList();
      listWriter.writeInt(0);
      listWriter.writeInt(1);
      listWriter.endList();
      listWriter.startList();
      listWriter.writeInt(0);
      listWriter.endList();
      listWriter.startList();
      listWriter.writeInt(1);
      listWriter.endList();
      listWriter.setValueCount(3);

      List<Field> fields = ImmutableList.of(writeVector.getField());
      List<FieldVector> vectors = ImmutableList.of((FieldVector) writeVector);
      VectorSchemaRoot root = new VectorSchemaRoot(fields, vectors, 3);

      DictionaryProvider writeProvider = new MapDictionaryProvider(new Dictionary(writeDictionaryVector, writeEncoding));

      try (Socket socket = new Socket("localhost", serverPort);
           ArrowStreamWriter writer = new ArrowStreamWriter(root, writeProvider, socket.getOutputStream());
           ArrowStreamReader reader = new ArrowStreamReader(socket.getInputStream(), allocator)) {
        writer.start();
        writer.writeBatch();
        writer.end();

        reader.loadNextBatch();
        VectorSchemaRoot readerRoot = reader.getVectorSchemaRoot();
        Assert.assertEquals(3, readerRoot.getRowCount());

        ListVector readVector = (ListVector) readerRoot.getFieldVectors().get(0);
        Assert.assertNotNull(readVector);

        Assert.assertNull(readVector.getField().getDictionary());
        DictionaryEncoding readEncoding = readVector.getField().getChildren().get(0).getDictionary();
        Assert.assertNotNull(readEncoding);
        Assert.assertEquals(2L, readEncoding.getId());

        Field nestedField = readVector.getField().getChildren().get(0);

        DictionaryEncoding encoding = nestedField.getDictionary();
        Assert.assertNotNull(encoding);
        Assert.assertEquals(2L, encoding.getId());
        Assert.assertEquals(new Int(32, true), encoding.getIndexType());

        ListVector.Accessor accessor = readVector.getAccessor();
        Assert.assertEquals(3, accessor.getValueCount());
        Assert.assertEquals(Arrays.asList(0, 1), accessor.getObject(0));
        Assert.assertEquals(Arrays.asList(0), accessor.getObject(1));
        Assert.assertEquals(Arrays.asList(1), accessor.getObject(2));

        Dictionary readDictionary = reader.lookup(2L);
        Assert.assertNotNull(readDictionary);
        NullableVarCharVector.Accessor dictionaryAccessor = ((NullableVarCharVector) readDictionary.getVector()).getAccessor();
        Assert.assertEquals(2, dictionaryAccessor.getValueCount());
        Assert.assertEquals(new Text("foo"), dictionaryAccessor.getObject(0));
        Assert.assertEquals(new Text("bar"), dictionaryAccessor.getObject(1));
      }
    }
  }
}
