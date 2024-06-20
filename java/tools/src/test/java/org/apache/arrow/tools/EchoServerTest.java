/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.arrow.tools;

import static java.util.Arrays.asList;
import static org.apache.arrow.vector.types.Types.MinorType.TINYINT;
import static org.apache.arrow.vector.types.Types.MinorType.VARCHAR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.dictionary.DictionaryProvider.MapDictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.testing.ValueVectorDataPopulator;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.ArrowType.Int;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class EchoServerTest {

  private static EchoServer server;
  private static int serverPort;
  private static Thread serverThread;

  @BeforeAll
  public static void startEchoServer() throws IOException {
    server = new EchoServer(0);
    serverPort = server.port();
    serverThread =
        new Thread() {
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

  @AfterAll
  public static void stopEchoServer() throws IOException, InterruptedException {
    server.close();
    serverThread.join();
  }

  private void testEchoServer(int serverPort, Field field, TinyIntVector vector, int batches)
      throws UnknownHostException, IOException {
    VectorSchemaRoot root = new VectorSchemaRoot(asList(field), asList((FieldVector) vector), 0);
    try (BufferAllocator alloc = new RootAllocator(Long.MAX_VALUE);
        Socket socket = new Socket("localhost", serverPort);
        ArrowStreamWriter writer = new ArrowStreamWriter(root, null, socket.getOutputStream());
        ArrowStreamReader reader = new ArrowStreamReader(socket.getInputStream(), alloc)) {
      writer.start();
      for (int i = 0; i < batches; i++) {
        vector.allocateNew(16);
        for (int j = 0; j < 8; j++) {
          vector.set(j, j + i);
          vector.set(j + 8, 0, (byte) (j + i));
        }
        vector.setValueCount(16);
        root.setRowCount(16);
        writer.writeBatch();
      }
      writer.end();

      assertEquals(new Schema(asList(field)), reader.getVectorSchemaRoot().getSchema());

      TinyIntVector readVector =
          (TinyIntVector) reader.getVectorSchemaRoot().getFieldVectors().get(0);
      for (int i = 0; i < batches; i++) {
        assertTrue(reader.loadNextBatch());
        assertEquals(16, reader.getVectorSchemaRoot().getRowCount());
        assertEquals(16, readVector.getValueCount());
        for (int j = 0; j < 8; j++) {
          assertEquals(j + i, readVector.get(j));
          assertTrue(readVector.isNull(j + 8));
        }
      }
      assertFalse(reader.loadNextBatch());
      assertEquals(0, reader.getVectorSchemaRoot().getRowCount());
      assertEquals(reader.bytesRead(), writer.bytesWritten());
    }
  }

  @Test
  public void basicTest() throws InterruptedException, IOException {
    BufferAllocator alloc = new RootAllocator(Long.MAX_VALUE);

    Field field =
        new Field(
            "testField",
            new FieldType(true, new ArrowType.Int(8, true), null, null),
            Collections.<Field>emptyList());
    TinyIntVector vector =
        new TinyIntVector("testField", FieldType.nullable(TINYINT.getType()), alloc);

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
        IntVector writeVector =
            new IntVector(
                "varchar",
                new FieldType(true, MinorType.INT.getType(), writeEncoding, null),
                allocator);
        VarCharVector writeDictionaryVector =
            new VarCharVector("dict", FieldType.nullable(VARCHAR.getType()), allocator)) {

      ValueVectorDataPopulator.setVector(writeVector, 0, 1, null, 2, 1, 2);
      ValueVectorDataPopulator.setVector(
          writeDictionaryVector,
          "foo".getBytes(StandardCharsets.UTF_8),
          "bar".getBytes(StandardCharsets.UTF_8),
          "baz".getBytes(StandardCharsets.UTF_8));

      List<Field> fields = ImmutableList.of(writeVector.getField());
      List<FieldVector> vectors = ImmutableList.of((FieldVector) writeVector);
      VectorSchemaRoot root = new VectorSchemaRoot(fields, vectors, 6);

      DictionaryProvider writeProvider =
          new MapDictionaryProvider(new Dictionary(writeDictionaryVector, writeEncoding));

      try (Socket socket = new Socket("localhost", serverPort);
          ArrowStreamWriter writer =
              new ArrowStreamWriter(root, writeProvider, socket.getOutputStream());
          ArrowStreamReader reader = new ArrowStreamReader(socket.getInputStream(), allocator)) {
        writer.start();
        writer.writeBatch();
        writer.end();

        reader.loadNextBatch();
        VectorSchemaRoot readerRoot = reader.getVectorSchemaRoot();
        assertEquals(6, readerRoot.getRowCount());

        FieldVector readVector = readerRoot.getFieldVectors().get(0);
        assertNotNull(readVector);

        DictionaryEncoding readEncoding = readVector.getField().getDictionary();
        assertNotNull(readEncoding);
        assertEquals(1L, readEncoding.getId());

        assertEquals(6, readVector.getValueCount());
        assertEquals(0, readVector.getObject(0));
        assertEquals(1, readVector.getObject(1));
        assertEquals(null, readVector.getObject(2));
        assertEquals(2, readVector.getObject(3));
        assertEquals(1, readVector.getObject(4));
        assertEquals(2, readVector.getObject(5));

        Dictionary dictionary = reader.lookup(1L);
        assertNotNull(dictionary);
        VarCharVector dictionaryVector = ((VarCharVector) dictionary.getVector());
        assertEquals(3, dictionaryVector.getValueCount());
        assertEquals(new Text("foo"), dictionaryVector.getObject(0));
        assertEquals(new Text("bar"), dictionaryVector.getObject(1));
        assertEquals(new Text("baz"), dictionaryVector.getObject(2));
      }
    }
  }

  @Test
  public void testNestedDictionary() throws IOException {
    DictionaryEncoding writeEncoding = new DictionaryEncoding(2L, false, null);
    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        VarCharVector writeDictionaryVector =
            new VarCharVector("dictionary", FieldType.nullable(VARCHAR.getType()), allocator);
        ListVector writeVector = ListVector.empty("list", allocator)) {

      // data being written:
      // [['foo', 'bar'], ['foo'], ['bar']] -> [[0, 1], [0], [1]]

      writeDictionaryVector.allocateNew();
      writeDictionaryVector.set(0, "foo".getBytes(StandardCharsets.UTF_8));
      writeDictionaryVector.set(1, "bar".getBytes(StandardCharsets.UTF_8));
      writeDictionaryVector.setValueCount(2);

      writeVector.addOrGetVector(new FieldType(true, MinorType.INT.getType(), writeEncoding, null));
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

      DictionaryProvider writeProvider =
          new MapDictionaryProvider(new Dictionary(writeDictionaryVector, writeEncoding));

      try (Socket socket = new Socket("localhost", serverPort);
          ArrowStreamWriter writer =
              new ArrowStreamWriter(root, writeProvider, socket.getOutputStream());
          ArrowStreamReader reader = new ArrowStreamReader(socket.getInputStream(), allocator)) {
        writer.start();
        writer.writeBatch();
        writer.end();

        reader.loadNextBatch();
        VectorSchemaRoot readerRoot = reader.getVectorSchemaRoot();
        assertEquals(3, readerRoot.getRowCount());

        ListVector readVector = (ListVector) readerRoot.getFieldVectors().get(0);
        assertNotNull(readVector);

        assertNull(readVector.getField().getDictionary());
        DictionaryEncoding readEncoding =
            readVector.getField().getChildren().get(0).getDictionary();
        assertNotNull(readEncoding);
        assertEquals(2L, readEncoding.getId());

        Field nestedField = readVector.getField().getChildren().get(0);

        DictionaryEncoding encoding = nestedField.getDictionary();
        assertNotNull(encoding);
        assertEquals(2L, encoding.getId());
        assertEquals(new Int(32, true), encoding.getIndexType());

        assertEquals(3, readVector.getValueCount());
        assertEquals(Arrays.asList(0, 1), readVector.getObject(0));
        assertEquals(Arrays.asList(0), readVector.getObject(1));
        assertEquals(Arrays.asList(1), readVector.getObject(2));

        Dictionary readDictionary = reader.lookup(2L);
        assertNotNull(readDictionary);
        VarCharVector dictionaryVector = ((VarCharVector) readDictionary.getVector());
        assertEquals(2, dictionaryVector.getValueCount());
        assertEquals(new Text("foo"), dictionaryVector.getObject(0));
        assertEquals(new Text("bar"), dictionaryVector.getObject(1));
      }
    }
  }
}
