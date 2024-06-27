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
package org.apache.arrow.vector;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.stream.Stream;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.extension.InvalidExtensionMetadataException;
import org.apache.arrow.vector.extension.OpaqueType;
import org.apache.arrow.vector.extension.OpaqueVector;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

class TestOpaqueExtensionType {
  BufferAllocator allocator;

  @BeforeEach
  void beforeEach() {
    allocator = new RootAllocator();
  }

  @AfterEach
  void afterEach() {
    allocator.close();
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "{\"type_name\": \"\", \"vendor_name\": \"\"}",
        "{\"type_name\": \"\", \"vendor_name\": \"\", \"extra_field\": 42}",
        "{\"type_name\": \"array\", \"vendor_name\": \"postgresql\"}",
        "{\"type_name\": \"foo.bar\", \"vendor_name\": \"postgresql\"}",
      })
  void testDeserializeValid(String serialized) {
    ArrowType storageType = Types.MinorType.NULL.getType();
    OpaqueType type = new OpaqueType(storageType, "", "");

    assertDoesNotThrow(() -> type.deserialize(storageType, serialized));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "",
        "{\"type_name\": \"\"}",
        "{\"vendor_name\": \"\"}",
        "{\"type_name\": null, \"vendor_name\": \"\"}",
        "{\"type_name\": \"\", \"vendor_name\": null}",
        "{\"type_name\": 42, \"vendor_name\": \"\"}",
        "{\"type_name\": \"\", \"vendor_name\": 42}",
        "{\"type_name\": \"\", \"vendor_name\": \"\"",
      })
  void testDeserializeInvalid(String serialized) {
    ArrowType storageType = Types.MinorType.NULL.getType();
    OpaqueType type = new OpaqueType(storageType, "", "");

    assertThrows(
        InvalidExtensionMetadataException.class, () -> type.deserialize(storageType, serialized));
  }

  @ParameterizedTest
  @MethodSource("storageType")
  void testRoundTrip(ArrowType storageType) {
    OpaqueType type = new OpaqueType(storageType, "foo", "bar");
    assertEquals(storageType, type.storageType());
    assertEquals("foo", type.typeName());
    if (storageType.isComplex()) {
      assertThrows(
          UnsupportedOperationException.class,
          () -> type.getNewVector("name", FieldType.nullable(type), allocator));
    } else {
      assertDoesNotThrow(() -> type.getNewVector("name", FieldType.nullable(type), allocator))
          .close();
    }

    String serialized = assertDoesNotThrow(type::serialize);
    OpaqueType holder = new OpaqueType(Types.MinorType.NULL.getType(), "", "");
    OpaqueType deserialized = (OpaqueType) holder.deserialize(storageType, serialized);
    assertEquals(type, deserialized);
    assertNotEquals(holder, deserialized);
  }

  @ParameterizedTest
  @MethodSource("storageType")
  void testIpcRoundTrip(ArrowType storageType) {
    OpaqueType.ensureRegistered();

    OpaqueType type = new OpaqueType(storageType, "foo", "bar");
    Schema schema = new Schema(Collections.singletonList(Field.nullable("unknown", type)));
    byte[] serialized = schema.serializeAsMessage();
    Schema deseralized = Schema.deserializeMessage(ByteBuffer.wrap(serialized));
    assertEquals(schema, deseralized);
  }

  @Test
  void testVectorType() throws IOException {
    OpaqueType.ensureRegistered();

    ArrowType storageType = Types.MinorType.VARBINARY.getType();
    OpaqueType type = new OpaqueType(storageType, "foo", "bar");
    try (FieldVector vector = type.getNewVector("field", FieldType.nullable(type), allocator)) {
      OpaqueVector opaque = assertInstanceOf(OpaqueVector.class, vector);
      assertEquals("field", opaque.getField().getName());
      assertEquals(type, opaque.getField().getType());

      VarBinaryVector binary =
          assertInstanceOf(VarBinaryVector.class, opaque.getUnderlyingVector());
      binary.setSafe(0, new byte[] {0, 1, 2, 3});
      binary.setNull(1);
      opaque.setValueCount(2);

      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      try (VectorSchemaRoot root = new VectorSchemaRoot(Collections.singletonList(opaque));
          ArrowStreamWriter writer =
              new ArrowStreamWriter(root, new DictionaryProvider.MapDictionaryProvider(), baos)) {
        writer.start();
        writer.writeBatch();
      }

      try (ArrowStreamReader reader =
          new ArrowStreamReader(new ByteArrayInputStream(baos.toByteArray()), allocator)) {
        assertTrue(reader.loadNextBatch());
        VectorSchemaRoot root = reader.getVectorSchemaRoot();
        assertEquals(2, root.getRowCount());
        assertEquals(new Schema(Collections.singletonList(opaque.getField())), root.getSchema());

        OpaqueVector actual = assertInstanceOf(OpaqueVector.class, root.getVector("field"));
        assertFalse(actual.isNull(0));
        assertTrue(actual.isNull(1));
        assertArrayEquals(new byte[] {0, 1, 2, 3}, (byte[]) actual.getObject(0));
        assertNull(actual.getObject(1));
      }
    }
  }

  static Stream<ArrowType> storageType() {
    return Stream.of(
        Types.MinorType.NULL.getType(),
        Types.MinorType.BIGINT.getType(),
        Types.MinorType.BIT.getType(),
        Types.MinorType.VARBINARY.getType(),
        Types.MinorType.VARCHAR.getType(),
        Types.MinorType.LIST.getType(),
        new ArrowType.Decimal(12, 4, 128));
  }
}
