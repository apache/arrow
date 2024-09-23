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
package org.apache.arrow.vector.extension;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Collections;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.Decimal256Vector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.DurationVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.Float2Vector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.IntervalDayVector;
import org.apache.arrow.vector.IntervalMonthDayNanoVector;
import org.apache.arrow.vector.IntervalYearVector;
import org.apache.arrow.vector.LargeVarBinaryVector;
import org.apache.arrow.vector.LargeVarCharVector;
import org.apache.arrow.vector.NullVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.TimeSecVector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.TimeStampMilliTZVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.TimeStampNanoTZVector;
import org.apache.arrow.vector.TimeStampNanoVector;
import org.apache.arrow.vector.TimeStampSecTZVector;
import org.apache.arrow.vector.TimeStampSecVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.ViewVarBinaryVector;
import org.apache.arrow.vector.ViewVarCharVector;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.ExtensionTypeRegistry;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

/**
 * Opaque is a placeholder for a type from an external (usually non-Arrow) system that could not be
 * interpreted.
 */
public class OpaqueType extends ArrowType.ExtensionType {
  private static final AtomicBoolean registered = new AtomicBoolean(false);
  public static final String EXTENSION_NAME = "arrow.opaque";
  private final ArrowType storageType;
  private final String typeName;
  private final String vendorName;

  /** Register the extension type so it can be used globally. */
  public static void ensureRegistered() {
    if (!registered.getAndSet(true)) {
      // The values don't matter, we just need an instance
      ExtensionTypeRegistry.register(new OpaqueType(Types.MinorType.NULL.getType(), "", ""));
    }
  }

  /**
   * Create a new type instance.
   *
   * @param storageType The underlying Arrow type.
   * @param typeName The name of the unknown type.
   * @param vendorName The name of the originating system of the unknown type.
   */
  public OpaqueType(ArrowType storageType, String typeName, String vendorName) {
    this.storageType = Objects.requireNonNull(storageType, "storageType");
    this.typeName = Objects.requireNonNull(typeName, "typeName");
    this.vendorName = Objects.requireNonNull(vendorName, "vendorName");
  }

  @Override
  public ArrowType storageType() {
    return storageType;
  }

  public String typeName() {
    return typeName;
  }

  public String vendorName() {
    return vendorName;
  }

  @Override
  public String extensionName() {
    return EXTENSION_NAME;
  }

  @Override
  public boolean extensionEquals(ExtensionType other) {
    return other != null
        && EXTENSION_NAME.equals(other.extensionName())
        && other instanceof OpaqueType
        && storageType.equals(other.storageType())
        && typeName.equals(((OpaqueType) other).typeName())
        && vendorName.equals(((OpaqueType) other).vendorName());
  }

  @Override
  public String serialize() {
    ObjectMapper mapper = new ObjectMapper();
    ObjectNode object = mapper.createObjectNode();
    object.put("type_name", typeName);
    object.put("vendor_name", vendorName);
    try {
      return mapper.writeValueAsString(object);
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Could not serialize " + this, e);
    }
  }

  @Override
  public ArrowType deserialize(ArrowType storageType, String serializedData) {
    ObjectMapper mapper = new ObjectMapper();
    JsonNode object;
    try {
      object = mapper.readTree(serializedData);
    } catch (JsonProcessingException e) {
      throw new InvalidExtensionMetadataException("Extension metadata is invalid", e);
    }
    JsonNode typeName = object.get("type_name");
    JsonNode vendorName = object.get("vendor_name");
    if (typeName == null) {
      throw new InvalidExtensionMetadataException("typeName is missing");
    }
    if (vendorName == null) {
      throw new InvalidExtensionMetadataException("vendorName is missing");
    }
    if (!typeName.isTextual()) {
      throw new InvalidExtensionMetadataException("typeName should be string, was " + typeName);
    }
    if (!vendorName.isTextual()) {
      throw new InvalidExtensionMetadataException("vendorName should be string, was " + vendorName);
    }
    return new OpaqueType(storageType, typeName.asText(), vendorName.asText());
  }

  @Override
  public FieldVector getNewVector(String name, FieldType fieldType, BufferAllocator allocator) {
    // XXX: fieldType is supposed to be the extension type
    final Field field = new Field(name, fieldType, Collections.emptyList());
    final FieldVector underlyingVector =
        storageType.accept(new UnderlyingVectorTypeVisitor(name, allocator));
    return new OpaqueVector(field, allocator, underlyingVector);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), storageType, typeName, vendorName);
  }

  @Override
  public String toString() {
    return "OpaqueType("
        + storageType
        + ", typeName='"
        + typeName
        + '\''
        + ", vendorName='"
        + vendorName
        + '\''
        + ')';
  }

  private static class UnderlyingVectorTypeVisitor implements ArrowTypeVisitor<FieldVector> {
    private final String name;
    private final BufferAllocator allocator;

    UnderlyingVectorTypeVisitor(String name, BufferAllocator allocator) {
      this.name = name;
      this.allocator = allocator;
    }

    @Override
    public FieldVector visit(Null type) {
      return new NullVector(name);
    }

    private RuntimeException unsupported(ArrowType type) {
      throw new UnsupportedOperationException(
          "OpaqueType#getUnderlyingVector is not supported for storage type: " + type);
    }

    @Override
    public FieldVector visit(Struct type) {
      throw unsupported(type);
    }

    @Override
    public FieldVector visit(List type) {
      throw unsupported(type);
    }

    @Override
    public FieldVector visit(LargeList type) {
      throw unsupported(type);
    }

    @Override
    public FieldVector visit(FixedSizeList type) {
      throw unsupported(type);
    }

    @Override
    public FieldVector visit(Union type) {
      throw unsupported(type);
    }

    @Override
    public FieldVector visit(Map type) {
      throw unsupported(type);
    }

    @Override
    public FieldVector visit(Int type) {
      return new IntVector(name, allocator);
    }

    @Override
    public FieldVector visit(FloatingPoint type) {
      switch (type.getPrecision()) {
        case HALF:
          return new Float2Vector(name, allocator);
        case SINGLE:
          return new Float4Vector(name, allocator);
        case DOUBLE:
          return new Float8Vector(name, allocator);
        default:
          throw unsupported(type);
      }
    }

    @Override
    public FieldVector visit(Utf8 type) {
      return new VarCharVector(name, allocator);
    }

    @Override
    public FieldVector visit(Utf8View type) {
      return new ViewVarCharVector(name, allocator);
    }

    @Override
    public FieldVector visit(LargeUtf8 type) {
      return new LargeVarCharVector(name, allocator);
    }

    @Override
    public FieldVector visit(Binary type) {
      return new VarBinaryVector(name, allocator);
    }

    @Override
    public FieldVector visit(BinaryView type) {
      return new ViewVarBinaryVector(name, allocator);
    }

    @Override
    public FieldVector visit(LargeBinary type) {
      return new LargeVarBinaryVector(name, allocator);
    }

    @Override
    public FieldVector visit(FixedSizeBinary type) {
      return new FixedSizeBinaryVector(Field.nullable(name, type), allocator);
    }

    @Override
    public FieldVector visit(Bool type) {
      return new BitVector(name, allocator);
    }

    @Override
    public FieldVector visit(Decimal type) {
      if (type.getBitWidth() == 128) {
        return new DecimalVector(Field.nullable(name, type), allocator);
      } else if (type.getBitWidth() == 256) {
        return new Decimal256Vector(Field.nullable(name, type), allocator);
      }
      throw unsupported(type);
    }

    @Override
    public FieldVector visit(Date type) {
      switch (type.getUnit()) {
        case DAY:
          return new DateDayVector(name, allocator);
        case MILLISECOND:
          return new DateMilliVector(name, allocator);
        default:
          throw unsupported(type);
      }
    }

    @Override
    public FieldVector visit(Time type) {
      switch (type.getUnit()) {
        case SECOND:
          return new TimeSecVector(name, allocator);
        case MILLISECOND:
          return new TimeMilliVector(name, allocator);
        case MICROSECOND:
          return new TimeMicroVector(name, allocator);
        case NANOSECOND:
          return new TimeNanoVector(name, allocator);
        default:
          throw unsupported(type);
      }
    }

    @Override
    public FieldVector visit(Timestamp type) {
      if (type.getTimezone() == null || type.getTimezone().isEmpty()) {
        switch (type.getUnit()) {
          case SECOND:
            return new TimeStampSecVector(Field.nullable(name, type), allocator);
          case MILLISECOND:
            return new TimeStampMilliVector(Field.nullable(name, type), allocator);
          case MICROSECOND:
            return new TimeStampMicroVector(Field.nullable(name, type), allocator);
          case NANOSECOND:
            return new TimeStampNanoVector(Field.nullable(name, type), allocator);
          default:
            throw unsupported(type);
        }
      }
      switch (type.getUnit()) {
        case SECOND:
          return new TimeStampSecTZVector(Field.nullable(name, type), allocator);
        case MILLISECOND:
          return new TimeStampMilliTZVector(Field.nullable(name, type), allocator);
        case MICROSECOND:
          return new TimeStampMicroTZVector(Field.nullable(name, type), allocator);
        case NANOSECOND:
          return new TimeStampNanoTZVector(Field.nullable(name, type), allocator);
        default:
          throw unsupported(type);
      }
    }

    @Override
    public FieldVector visit(Interval type) {
      switch (type.getUnit()) {
        case YEAR_MONTH:
          return new IntervalYearVector(name, allocator);
        case DAY_TIME:
          return new IntervalDayVector(name, allocator);
        case MONTH_DAY_NANO:
          return new IntervalMonthDayNanoVector(name, allocator);
        default:
          throw unsupported(type);
      }
    }

    @Override
    public FieldVector visit(Duration type) {
      return new DurationVector(Field.nullable(name, type), allocator);
    }

    @Override
    public FieldVector visit(ListView type) {
      throw unsupported(type);
    }

    @Override
    public FieldVector visit(LargeListView type) {
      throw unsupported(type);
    }

    @Override
    public FieldVector visit(RunEndEncoded type) {
      throw unsupported(type);
    }
  }
}
