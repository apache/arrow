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

package org.apache.arrow.vector.types.pojo;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.UUID;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ExtensionTypeVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.types.pojo.ArrowType.ExtensionType;

import org.junit.Assert;
import org.junit.Test;

public class TestExtensionType {
  /**
   * Test that a custom UUID type can be round-tripped through a temporary file.
   */
  @Test
  public void roundtripUuid() throws IOException {
    ExtensionTypeRegistry.register(new UuidType());
    final Schema schema = new Schema(Collections.singletonList(Field.nullable("a", new UuidType())));
    try (final BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE);
        final VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
      UUID u1 = UUID.randomUUID();
      UUID u2 = UUID.randomUUID();
      UuidVector vector = (UuidVector) root.getVector("a");
      vector.setValueCount(2);
      vector.set(0, u1);
      vector.set(1, u2);
      root.setRowCount(2);

      final File file = File.createTempFile("uuidtest", ".arrow");
      try (final WritableByteChannel channel = FileChannel
          .open(Paths.get(file.getAbsolutePath()), StandardOpenOption.WRITE);
          final ArrowFileWriter writer = new ArrowFileWriter(root, null, channel)) {
        writer.start();
        writer.writeBatch();
        writer.end();
      }

      try (final SeekableByteChannel channel = Files.newByteChannel(Paths.get(file.getAbsolutePath()));
          final ArrowFileReader reader = new ArrowFileReader(channel, allocator)) {
        reader.loadNextBatch();
        final VectorSchemaRoot readerRoot = reader.getVectorSchemaRoot();
        Assert.assertEquals(root.getSchema(), readerRoot.getSchema());

        final Field field = readerRoot.getSchema().getFields().get(0);
        final UuidType expectedType = new UuidType();
        Assert.assertEquals(field.getMetadata().get(ExtensionType.EXTENSION_METADATA_KEY_NAME),
            expectedType.extensionName());
        Assert.assertEquals(field.getMetadata().get(ExtensionType.EXTENSION_METADATA_KEY_METADATA),
            expectedType.serialize());

        final ExtensionTypeVector deserialized = (ExtensionTypeVector) readerRoot.getFieldVectors().get(0);
        Assert.assertEquals(vector.getValueCount(), deserialized.getValueCount());
        for (int i = 0; i < vector.getValueCount(); i++) {
          Assert.assertEquals(vector.isNull(i), deserialized.isNull(i));
          if (!vector.isNull(i)) {
            Assert.assertEquals(vector.getObject(i), deserialized.getObject(i));
          }
        }
      }
    }
  }

  /**
   * Test that a custom UUID type can be read as its underlying type.
   */
  @Test
  public void readUnderlyingType() throws IOException {
    ExtensionTypeRegistry.register(new UuidType());
    final Schema schema = new Schema(Collections.singletonList(Field.nullable("a", new UuidType())));
    try (final BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE);
        final VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
      UUID u1 = UUID.randomUUID();
      UUID u2 = UUID.randomUUID();
      UuidVector vector = (UuidVector) root.getVector("a");
      vector.setValueCount(2);
      vector.set(0, u1);
      vector.set(1, u2);
      root.setRowCount(2);

      final File file = File.createTempFile("uuidtest", ".arrow");
      try (final WritableByteChannel channel = FileChannel
          .open(Paths.get(file.getAbsolutePath()), StandardOpenOption.WRITE);
          final ArrowFileWriter writer = new ArrowFileWriter(root, null, channel)) {
        writer.start();
        writer.writeBatch();
        writer.end();
      }

      ExtensionTypeRegistry.unregister(new UuidType());

      try (final SeekableByteChannel channel = Files.newByteChannel(Paths.get(file.getAbsolutePath()));
          final ArrowFileReader reader = new ArrowFileReader(channel, allocator)) {
        reader.loadNextBatch();
        final VectorSchemaRoot readerRoot = reader.getVectorSchemaRoot();
        Assert.assertEquals(1, readerRoot.getSchema().getFields().size());
        Assert.assertEquals("a", readerRoot.getSchema().getFields().get(0).getName());
        Assert.assertTrue(readerRoot.getSchema().getFields().get(0).getType() instanceof ArrowType.FixedSizeBinary);
        Assert.assertEquals(16,
            ((ArrowType.FixedSizeBinary) readerRoot.getSchema().getFields().get(0).getType()).getByteWidth());

        final Field field = readerRoot.getSchema().getFields().get(0);
        final UuidType expectedType = new UuidType();
        Assert.assertEquals(field.getMetadata().get(ExtensionType.EXTENSION_METADATA_KEY_NAME),
            expectedType.extensionName());
        Assert.assertEquals(field.getMetadata().get(ExtensionType.EXTENSION_METADATA_KEY_METADATA),
            expectedType.serialize());

        final FixedSizeBinaryVector deserialized = (FixedSizeBinaryVector) readerRoot.getFieldVectors().get(0);
        Assert.assertEquals(vector.getValueCount(), deserialized.getValueCount());
        for (int i = 0; i < vector.getValueCount(); i++) {
          Assert.assertEquals(vector.isNull(i), deserialized.isNull(i));
          if (!vector.isNull(i)) {
            final UUID uuid = vector.getObject(i);
            final ByteBuffer bb = ByteBuffer.allocate(16);
            bb.putLong(uuid.getMostSignificantBits());
            bb.putLong(uuid.getLeastSignificantBits());
            Assert.assertArrayEquals(bb.array(), deserialized.get(i));
          }
        }
      }
    }
  }

  static class UuidType extends ExtensionType {

    @Override
    public ArrowType storageType() {
      return new ArrowType.FixedSizeBinary(16);
    }

    @Override
    public String extensionName() {
      return "uuid";
    }

    @Override
    public boolean extensionEquals(ExtensionType other) {
      return other instanceof UuidType;
    }

    @Override
    public ArrowType deserialize(ArrowType storageType, String serializedData) {
      if (!storageType.equals(storageType())) {
        throw new UnsupportedOperationException("Cannot construct UuidType from underlying type " + storageType);
      }
      return new UuidType();
    }

    @Override
    public String serialize() {
      return "";
    }

    @Override
    public FieldVector getNewVector(String name, FieldType fieldType, BufferAllocator allocator) {
      return new UuidVector(name, allocator, new FixedSizeBinaryVector(name, allocator, 16));
    }

  }

  static class UuidVector extends ExtensionTypeVector<FixedSizeBinaryVector> {

    public UuidVector(String name, BufferAllocator allocator, FixedSizeBinaryVector underlyingVector) {
      super(name, allocator, underlyingVector);
    }

    @Override
    public UUID getObject(int index) {
      final ByteBuffer bb = ByteBuffer.wrap(getUnderlyingVector().getObject(index));
      return new UUID(bb.getLong(), bb.getLong());
    }

    public void set(int index, UUID uuid) {
      ByteBuffer bb = ByteBuffer.allocate(16);
      bb.putLong(uuid.getMostSignificantBits());
      bb.putLong(uuid.getLeastSignificantBits());
      getUnderlyingVector().set(index, bb.array());
    }
  }
}
