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

package org.apache.arrow.flight;

import static org.apache.arrow.flight.FlightTestUtil.LOCALHOST;
import static org.apache.arrow.flight.Location.forGrpcInsecure;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;

import org.apache.arrow.flight.FlightClient.ClientStreamListener;
import org.apache.arrow.flight.TestBasicOperation.Producer;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryEncoder;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class TestFlightClient {
  /**
   * ARROW-5063: make sure two clients to the same location can be closed independently.
   */
  @Test
  public void independentShutdown() throws Exception {
    try (final BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE);
         final FlightServer server = FlightServer.builder(allocator, forGrpcInsecure(LOCALHOST, 0),
             new Producer(allocator)).build().start()) {
      final Schema schema = new Schema(Collections.singletonList(Field.nullable("a", new ArrowType.Int(32, true))));
      try (final FlightClient client1 = FlightClient.builder(allocator, server.getLocation()).build();
           final VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
        // Use startPut as this ensures the RPC won't finish until we want it to
        final ClientStreamListener listener = client1.startPut(FlightDescriptor.path("test"), root,
            new AsyncPutListener());
        try (final FlightClient client2 = FlightClient.builder(allocator, server.getLocation()).build()) {
          client2.listActions().forEach(actionType -> Assertions.assertNotNull(actionType.getType()));
        }
        listener.completed();
        listener.getResult();
      }
    }
  }

  /**
   * ARROW-5978: make sure that we can properly close a client/stream after requesting dictionaries.
   */
  @Disabled // Unfortunately this test is flaky in CI.
  @Test
  public void freeDictionaries() throws Exception {
    final Schema expectedSchema = new Schema(Collections
        .singletonList(new Field("encoded",
            new FieldType(true, new ArrowType.Int(32, true), new DictionaryEncoding(1L, false, null)), null)));
    try (final BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE);
         final BufferAllocator serverAllocator = allocator.newChildAllocator("flight-server", 0, Integer.MAX_VALUE);
         final FlightServer server = FlightServer.builder(serverAllocator, forGrpcInsecure(LOCALHOST, 0),
             new DictionaryProducer(serverAllocator)).build().start()) {
      try (final FlightClient client = FlightClient.builder(allocator, server.getLocation()).build()) {
        try (final FlightStream stream = client.getStream(new Ticket(new byte[0]))) {
          Assertions.assertTrue(stream.next());
          Assertions.assertNotNull(stream.getDictionaryProvider().lookup(1));
          final VectorSchemaRoot root = stream.getRoot();
          Assertions.assertEquals(expectedSchema, root.getSchema());
          Assertions.assertEquals(6, root.getVector("encoded").getValueCount());
          try (final ValueVector decoded = DictionaryEncoder
              .decode(root.getVector("encoded"), stream.getDictionaryProvider().lookup(1))) {
            Assertions.assertFalse(decoded.isNull(1));
            Assertions.assertTrue(decoded instanceof VarCharVector);
            Assertions.assertArrayEquals("one".getBytes(StandardCharsets.UTF_8), ((VarCharVector) decoded).get(1));
          }
          Assertions.assertFalse(stream.next());
        }
        // Closing stream fails if it doesn't free dictionaries; closing dictionaries fails (refcount goes negative)
        // if reference isn't retained in ArrowMessage
      }
    }
  }

  /**
   * ARROW-5978: make sure that dictionary ownership can't be claimed twice.
   */
  @Disabled // Unfortunately this test is flaky in CI.
  @Test
  public void ownDictionaries() throws Exception {
    try (final BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE);
         final BufferAllocator serverAllocator = allocator.newChildAllocator("flight-server", 0, Integer.MAX_VALUE);
         final FlightServer server = FlightServer.builder(serverAllocator, forGrpcInsecure(LOCALHOST, 0),
             new DictionaryProducer(serverAllocator)).build().start()) {
      try (final FlightClient client = FlightClient.builder(allocator, server.getLocation()).build()) {
        try (final FlightStream stream = client.getStream(new Ticket(new byte[0]))) {
          Assertions.assertTrue(stream.next());
          Assertions.assertFalse(stream.next());
          final DictionaryProvider provider = stream.takeDictionaryOwnership();
          Assertions.assertThrows(IllegalStateException.class, stream::takeDictionaryOwnership);
          Assertions.assertThrows(IllegalStateException.class, stream::getDictionaryProvider);
          DictionaryUtils.closeDictionaries(stream.getSchema(), provider);
        }
      }
    }
  }

  /**
   * ARROW-5978: make sure that dictionaries can be used after closing the stream.
   */
  @Disabled // Unfortunately this test is flaky in CI.
  @Test
  public void useDictionariesAfterClose() throws Exception {
    try (final BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE);
         final BufferAllocator serverAllocator = allocator.newChildAllocator("flight-server", 0, Integer.MAX_VALUE);
         final FlightServer server = FlightServer.builder(serverAllocator, forGrpcInsecure(LOCALHOST, 0),
                 new DictionaryProducer(serverAllocator))
             .build().start()) {
      try (final FlightClient client = FlightClient.builder(allocator, server.getLocation()).build()) {
        final VectorSchemaRoot root;
        final DictionaryProvider provider;
        try (final FlightStream stream = client.getStream(new Ticket(new byte[0]))) {
          final VectorUnloader unloader = new VectorUnloader(stream.getRoot());
          root = VectorSchemaRoot.create(stream.getSchema(), allocator);
          final VectorLoader loader = new VectorLoader(root);
          while (stream.next()) {
            try (final ArrowRecordBatch arb = unloader.getRecordBatch()) {
              loader.load(arb);
            }
          }
          provider = stream.takeDictionaryOwnership();
        }
        try (final ValueVector decoded = DictionaryEncoder
            .decode(root.getVector("encoded"), provider.lookup(1))) {
          Assertions.assertFalse(decoded.isNull(1));
          Assertions.assertTrue(decoded instanceof VarCharVector);
          Assertions.assertArrayEquals("one".getBytes(StandardCharsets.UTF_8), ((VarCharVector) decoded).get(1));
        }
        root.close();
        DictionaryUtils.closeDictionaries(root.getSchema(), provider);
      }
    }
  }

  static class DictionaryProducer extends NoOpFlightProducer {

    private final BufferAllocator allocator;

    public DictionaryProducer(BufferAllocator allocator) {
      this.allocator = allocator;
    }

    @Override
    public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener) {
      final byte[] zero = "zero".getBytes(StandardCharsets.UTF_8);
      final byte[] one = "one".getBytes(StandardCharsets.UTF_8);
      final byte[] two = "two".getBytes(StandardCharsets.UTF_8);
      try (final VarCharVector dictionaryVector = newVarCharVector("dictionary", allocator)) {
        final DictionaryProvider.MapDictionaryProvider provider = new DictionaryProvider.MapDictionaryProvider();

        dictionaryVector.allocateNew(512, 3);
        dictionaryVector.setSafe(0, zero, 0, zero.length);
        dictionaryVector.setSafe(1, one, 0, one.length);
        dictionaryVector.setSafe(2, two, 0, two.length);
        dictionaryVector.setValueCount(3);

        final Dictionary dictionary = new Dictionary(dictionaryVector, new DictionaryEncoding(1L, false, null));
        provider.put(dictionary);

        final FieldVector encodedVector;
        try (final VarCharVector unencoded = newVarCharVector("encoded", allocator)) {
          unencoded.allocateNewSafe();
          unencoded.set(1, one);
          unencoded.set(2, two);
          unencoded.set(3, zero);
          unencoded.set(4, two);
          unencoded.setValueCount(6);
          encodedVector = (FieldVector) DictionaryEncoder.encode(unencoded, dictionary);
        }

        final List<Field> fields = Collections.singletonList(encodedVector.getField());
        final List<FieldVector> vectors = Collections.singletonList(encodedVector);

        try (final VectorSchemaRoot root = new VectorSchemaRoot(fields, vectors, encodedVector.getValueCount())) {
          listener.start(root, provider);
          listener.putNext();
          listener.completed();
        }
      }
    }

    private static VarCharVector newVarCharVector(String name, BufferAllocator allocator) {
      return (VarCharVector)
          FieldType.nullable(new ArrowType.Utf8()).createNewSingleVector(name, allocator, null);
    }
  }
}
