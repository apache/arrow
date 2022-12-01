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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

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
import org.apache.arrow.memory.util.hash.ArrowBufHasher;
import org.apache.arrow.vector.ExtensionTypeVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.compare.Range;
import org.apache.arrow.vector.compare.RangeEqualsVisitor;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType.ExtensionType;
import org.apache.arrow.vector.util.VectorBatchAppender;
import org.apache.arrow.vector.validate.ValidateVectorVisitor;
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

  @Test
  public void testNullCheck() {
    NullPointerException e = assertThrows(NullPointerException.class,
        () -> {
          try (final BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE);
               final ExtensionTypeVector vector = new UuidVector("uuid", allocator, null)) {
            vector.getField();
            vector.allocateNewSafe();
          }
        });
    assertTrue(e.getMessage().contains("underlyingVector can not be null."));
  }

  /**
   * Test that a custom Location type can be round-tripped through a temporary file.
   */
  @Test
  public void roundtripLocation() throws IOException {
    ExtensionTypeRegistry.register(new LocationType());
    final Schema schema = new Schema(Collections.singletonList(Field.nullable("location", new LocationType())));
    try (final BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE);
         final VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
      LocationVector vector = (LocationVector) root.getVector("location");
      vector.allocateNew();
      vector.set(0, 34.073814f, -118.240784f);
      vector.set(2, 37.768056f, -122.3875f);
      vector.set(3, 40.739716f, -73.840782f);
      vector.setValueCount(4);
      root.setRowCount(4);

      final File file = File.createTempFile("locationtest", ".arrow");
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
        final LocationType expectedType = new LocationType();
        Assert.assertEquals(field.getMetadata().get(ExtensionType.EXTENSION_METADATA_KEY_NAME),
                expectedType.extensionName());
        Assert.assertEquals(field.getMetadata().get(ExtensionType.EXTENSION_METADATA_KEY_METADATA),
                expectedType.serialize());

        final ExtensionTypeVector deserialized = (ExtensionTypeVector) readerRoot.getFieldVectors().get(0);
        Assert.assertTrue(deserialized instanceof LocationVector);
        Assert.assertEquals(deserialized.getName(), "location");
        StructVector deserStruct = (StructVector) deserialized.getUnderlyingVector();
        Assert.assertNotNull(deserStruct.getChild("Latitude"));
        Assert.assertNotNull(deserStruct.getChild("Longitude"));
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

  @Test
  public void testVectorCompare() {
    UuidType uuidType = new UuidType();
    ExtensionTypeRegistry.register(uuidType);
    try (final BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE);
         UuidVector a1 = (UuidVector) uuidType.getNewVector("a", FieldType.nullable(uuidType), allocator);
         UuidVector a2 = (UuidVector) uuidType.getNewVector("a", FieldType.nullable(uuidType), allocator);
         UuidVector bb = (UuidVector) uuidType.getNewVector("a", FieldType.nullable(uuidType), allocator)
         ) {
      UUID u1 = UUID.randomUUID();
      UUID u2 = UUID.randomUUID();

      // Test out type and vector validation visitors for an ExtensionTypeVector
      ValidateVectorVisitor validateVisitor = new ValidateVectorVisitor();
      validateVisitor.visit(a1, null);

      a1.setValueCount(2);
      a1.set(0, u1);
      a1.set(1, u2);

      a2.setValueCount(2);
      a2.set(0, u1);
      a2.set(1, u2);

      bb.setValueCount(2);
      bb.set(0, u2);
      bb.set(1, u1);

      Range range = new Range(0, 0, a1.getValueCount());
      RangeEqualsVisitor visitor = new RangeEqualsVisitor(a1, a2);
      assertTrue(visitor.rangeEquals(range));

      visitor = new RangeEqualsVisitor(a1, bb);
      assertFalse(visitor.rangeEquals(range));

      // Test out vector appender
      VectorBatchAppender.batchAppend(a1, a2, bb);
      assertEquals(a1.getValueCount(), 6);
      validateVisitor.visit(a1, null);
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

    @Override
    public int hashCode(int index) {
      return hashCode(index, null);
    }

    @Override
    public int hashCode(int index, ArrowBufHasher hasher) {
      return getUnderlyingVector().hashCode(index, hasher);
    }

    public void set(int index, UUID uuid) {
      ByteBuffer bb = ByteBuffer.allocate(16);
      bb.putLong(uuid.getMostSignificantBits());
      bb.putLong(uuid.getLeastSignificantBits());
      getUnderlyingVector().set(index, bb.array());
    }
  }

  static class LocationType extends ExtensionType {

    @Override
    public ArrowType storageType() {
      return Struct.INSTANCE;
    }

    @Override
    public String extensionName() {
      return "location";
    }

    @Override
    public boolean extensionEquals(ExtensionType other) {
      return other instanceof LocationType;
    }

    @Override
    public ArrowType deserialize(ArrowType storageType, String serializedData) {
      if (!storageType.equals(storageType())) {
        throw new UnsupportedOperationException("Cannot construct LocationType from underlying type " + storageType);
      }
      return new LocationType();
    }

    @Override
    public String serialize() {
      return "";
    }

    @Override
    public FieldVector getNewVector(String name, FieldType fieldType, BufferAllocator allocator) {
      return new LocationVector(name, allocator);
    }
  }

  public static class LocationVector extends ExtensionTypeVector<StructVector> {

    private static StructVector buildUnderlyingVector(String name, BufferAllocator allocator) {
      final StructVector underlyingVector =
              new StructVector(name, allocator, FieldType.nullable(ArrowType.Struct.INSTANCE), null);
      underlyingVector.addOrGet("Latitude",
              FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)), Float4Vector.class);
      underlyingVector.addOrGet("Longitude",
              FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)), Float4Vector.class);
      return underlyingVector;
    }

    public LocationVector(String name, BufferAllocator allocator) {
      super(name, allocator, buildUnderlyingVector(name, allocator));
    }

    @Override
    public int hashCode(int index) {
      return hashCode(index, null);
    }

    @Override
    public int hashCode(int index, ArrowBufHasher hasher) {
      return getUnderlyingVector().hashCode(index, hasher);
    }

    @Override
    public java.util.Map<String, ?> getObject(int index) {
      return getUnderlyingVector().getObject(index);
    }

    public void set(int index, float latitude, float longitude) {
      getUnderlyingVector().getChild("Latitude", Float4Vector.class).set(index, latitude);
      getUnderlyingVector().getChild("Longitude", Float4Vector.class).set(index, longitude);
      getUnderlyingVector().setIndexDefined(index);
    }
  }
}
