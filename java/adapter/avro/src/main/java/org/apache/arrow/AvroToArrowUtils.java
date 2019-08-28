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

package org.apache.arrow;

import static org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE;
import static org.apache.arrow.vector.types.FloatingPointPrecision.SINGLE;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.arrow.consumers.AvroArraysConsumer;
import org.apache.arrow.consumers.AvroBooleanConsumer;
import org.apache.arrow.consumers.AvroBytesConsumer;
import org.apache.arrow.consumers.AvroDoubleConsumer;
import org.apache.arrow.consumers.AvroFixedConsumer;
import org.apache.arrow.consumers.AvroFloatConsumer;
import org.apache.arrow.consumers.AvroIntConsumer;
import org.apache.arrow.consumers.AvroLongConsumer;
import org.apache.arrow.consumers.AvroMapConsumer;
import org.apache.arrow.consumers.AvroNullConsumer;
import org.apache.arrow.consumers.AvroStringConsumer;
import org.apache.arrow.consumers.AvroStructConsumer;
import org.apache.arrow.consumers.AvroUnionsConsumer;
import org.apache.arrow.consumers.CompositeAvroConsumer;
import org.apache.arrow.consumers.Consumer;
import org.apache.arrow.consumers.NullableTypeConsumer;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ZeroVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.UnionVector;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.UnionMode;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.io.Decoder;

/**
 * Class that does most of the work to convert Avro data into Arrow columnar format Vector objects.
 */
public class AvroToArrowUtils {

  private static final int INVALID_NULL_INDEX = -1;

  /**
   * Creates a {@link Consumer} from the {@link Schema}
   *
   <p>This method currently performs following type mapping for Avro data types to corresponding Arrow data types.
   *
   * <ul>
   *   <li>STRING --> ArrowType.Utf8</li>
   *   <li>INT --> ArrowType.Int(32, signed)</li>
   *   <li>LONG --> ArrowType.Int(64, signed)</li>
   *   <li>FLOAT --> ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)</li>
   *   <li>DOUBLE --> ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)</li>
   *   <li>BOOLEAN --> ArrowType.Bool</li>
   *   <li>BYTES --> ArrowType.Binary</li>
   *   <li>ARRAY --> ArrowType.List</li>
   *   <li>MAP --> ArrowType.Map</li>
   *   <li>FIXED --> ArrowType.FixedSizeBinary</li>
   * </ul>
   */

  private static Consumer createConsumer(Schema schema, String name, BufferAllocator allocator) {
    return createConsumer(schema, name, false, INVALID_NULL_INDEX, allocator, null);
  }

  private static Consumer createConsumer(Schema schema, String name, BufferAllocator allocator, FieldVector vector) {
    return createConsumer(schema, name, false, INVALID_NULL_INDEX, allocator, vector);
  }

  /**
   * Create a consumer with the given avro schema
   * @param schema avro schema
   * @param name arrow field name
   * @param v vector to keep in consumer, if v == null, will create a new vector via field.
   * @return consumer
   */
  private static Consumer createConsumer(
      Schema schema,
      String name,
      boolean nullable,
      int nullIndex,
      BufferAllocator allocator,
      FieldVector v) {

    Preconditions.checkNotNull(schema, "Avro schema object can't be null");
    Preconditions.checkNotNull(allocator, "allocator can't be null");

    Type type = schema.getType();

    final ArrowType arrowType;
    final FieldType fieldType;
    final FieldVector vector;
    final Consumer consumer;

    switch (type) {
      case UNION:
        consumer = createUnionConsumer(schema, name, allocator, v);
        break;
      case ARRAY:
        consumer = createArrayConsumer(schema, name, allocator, v);
        break;
      case MAP:
        consumer = createMapConsumer(schema, name, allocator, v);
        break;
        //TODO implement enum and nested record type
      case RECORD:
        throw new UnsupportedOperationException();
      case ENUM:
        throw new UnsupportedOperationException();
      case STRING:
        arrowType = new ArrowType.Utf8();
        fieldType =  new FieldType(nullable, arrowType, /*dictionary=*/null, getMetaData(schema));
        vector = createVector(v, fieldType, name, allocator);
        consumer =  new AvroStringConsumer((VarCharVector) vector);
        break;
      case FIXED:
        arrowType = new ArrowType.FixedSizeBinary(schema.getFixedSize());
        fieldType =  new FieldType(nullable, arrowType, /*dictionary=*/null, getMetaData(schema));
        vector = createVector(v, fieldType, name, allocator);
        consumer =  new AvroFixedConsumer((FixedSizeBinaryVector) vector, schema.getFixedSize());
        break;
      case INT:
        arrowType = new ArrowType.Int(32, /*signed=*/true);
        fieldType =  new FieldType(nullable, arrowType, /*dictionary=*/null, getMetaData(schema));
        vector = createVector(v, fieldType, name, allocator);
        consumer = new AvroIntConsumer((IntVector) vector);
        break;
      case BOOLEAN:
        arrowType = new ArrowType.Bool();
        fieldType =  new FieldType(nullable, arrowType, /*dictionary=*/null, getMetaData(schema));
        vector = createVector(v, fieldType, name, allocator);
        consumer = new AvroBooleanConsumer((BitVector) vector);
        break;
      case LONG:
        arrowType = new ArrowType.Int(64, /*signed=*/true);
        fieldType =  new FieldType(nullable, arrowType, /*dictionary=*/null, getMetaData(schema));
        vector = createVector(v, fieldType, name, allocator);
        consumer =  new AvroLongConsumer((BigIntVector) vector);
        break;
      case FLOAT:
        arrowType =  new ArrowType.FloatingPoint(SINGLE);
        fieldType =  new FieldType(nullable, arrowType, /*dictionary=*/null, getMetaData(schema));
        vector = createVector(v, fieldType, name, allocator);
        consumer = new AvroFloatConsumer((Float4Vector) vector);
        break;
      case DOUBLE:
        arrowType = new ArrowType.FloatingPoint(DOUBLE);
        fieldType =  new FieldType(nullable, arrowType, /*dictionary=*/null, getMetaData(schema));
        vector = createVector(v, fieldType, name, allocator);
        consumer = new AvroDoubleConsumer((Float8Vector) vector);
        break;
      case BYTES:
        arrowType = new ArrowType.Binary();
        fieldType =  new FieldType(nullable, arrowType, /*dictionary=*/null, getMetaData(schema));
        vector = createVector(v, fieldType, name, allocator);
        consumer = new AvroBytesConsumer((VarBinaryVector) vector);
        break;
      case NULL:
        arrowType = new ArrowType.Null();
        fieldType =  new FieldType(nullable, arrowType, /*dictionary=*/null, getMetaData(schema));
        vector = createVector(v, fieldType, name, allocator);
        consumer = new AvroNullConsumer((ZeroVector) vector);
        break;
      default:
        // no-op, shouldn't get here
        throw new RuntimeException("Can't convert avro type %s to arrow type." + type.getName());
    }

    if (nullable) {
      return new NullableTypeConsumer(consumer, nullIndex);
    }
    return consumer;
  }

  private static FieldVector createVector(FieldVector v, FieldType fieldType, String name, BufferAllocator allocator) {
    return v != null ? v : fieldType.createNewSingleVector(name, allocator, null);
  }

  private static String getDefaultFieldName(ArrowType type) {
    Types.MinorType minorType = Types.getMinorTypeForArrowType(type);
    return minorType.name().toLowerCase();
  }

  private static Field avroSchemaToField(Schema schema, String name) {
    final Type type = schema.getType();
    final ArrowType arrowType;

    switch (type) {
      case UNION:
        List<Field> children = new ArrayList<>();
        for (int i = 0; i < schema.getTypes().size(); i++) {
          Schema childSchema = schema.getTypes().get(i);
          // Union child vector should use default name
          children.add(avroSchemaToField(childSchema, null));
        }
        arrowType = new ArrowType.Union(UnionMode.Sparse, null);
        if (name == null) {
          name = getDefaultFieldName(arrowType);
        }
        return new Field(name, FieldType.nullable(arrowType), children);
      case ARRAY:
        Schema elementSchema = schema.getElementType();
        arrowType = new ArrowType.List();
        if (name == null) {
          name = getDefaultFieldName(arrowType);
        }
        return new Field(name, FieldType.nullable(arrowType),
            Collections.singletonList(avroSchemaToField(elementSchema, elementSchema.getName())));
      case MAP:
        // MapVector internal struct field and key field should be non-nullable
        FieldType keyFieldType = new FieldType(/*nullable=*/false, new ArrowType.Utf8(), /*dictionary=*/null);
        Field keyField = new Field("key", keyFieldType, /*children=*/null);
        Field valueField = avroSchemaToField(schema.getValueType(), "value");

        FieldType structFieldType = new FieldType(false, new ArrowType.Struct(), /*dictionary=*/null);
        Field structField = new Field("internal", structFieldType, Arrays.asList(keyField, valueField));
        arrowType = new ArrowType.Map(/*keySorted=*/false);
        if (name == null) {
          name = getDefaultFieldName(arrowType);
        }
        return new Field(name, FieldType.nullable(arrowType), Collections.singletonList(structField));
      case STRING:
        arrowType = new ArrowType.Utf8();
        break;
      case FIXED:
        arrowType = new ArrowType.FixedSizeBinary(schema.getFixedSize());
        break;
      case INT:
        arrowType = new ArrowType.Int(32, /*signed=*/true);
        break;
      case BOOLEAN:
        arrowType = new ArrowType.Bool();
        break;
      case LONG:
        arrowType = new ArrowType.Int(64, /*signed=*/true);
        break;
      case FLOAT:
        arrowType = new ArrowType.FloatingPoint(SINGLE);
        break;
      case DOUBLE:
        arrowType = new ArrowType.FloatingPoint(DOUBLE);
        break;
      case BYTES:
        arrowType = new ArrowType.Binary();
        break;
      case NULL:
        arrowType = new ArrowType.Null();
        break;
      default:
        // no-op, shouldn't get here
        throw new UnsupportedOperationException();
    }

    if (name == null) {
      name = getDefaultFieldName(arrowType);
    }
    return Field.nullable(name, arrowType);
  }

  private static Consumer createArrayConsumer(Schema schema, String name, BufferAllocator allocator, FieldVector v) {

    ListVector listVector;
    if (v == null) {
      final Field field = avroSchemaToField(schema, name);
      listVector = (ListVector) field.createVector(allocator);
    } else {
      listVector = (ListVector) v;
    }

    FieldVector dataVector = listVector.getDataVector();

    // create delegate
    Schema childSchema = schema.getElementType();
    Consumer delegate = createConsumer(childSchema, childSchema.getName(), allocator, dataVector);

    return new AvroArraysConsumer(listVector, delegate);
  }

  private static Consumer createMapConsumer(Schema schema, String name, BufferAllocator allocator, FieldVector v) {

    MapVector mapVector;
    if (v == null) {
      final Field field = avroSchemaToField(schema, name);
      mapVector = (MapVector) field.createVector(allocator);
    } else {
      mapVector = (MapVector) v;
    }

    // create delegate struct consumer
    StructVector structVector = (StructVector) mapVector.getDataVector();

    // keys in avro map are always assumed to be strings.
    Consumer keyConsumer = new AvroStringConsumer(
        (VarCharVector) structVector.getChildrenFromFields().get(0));
    Consumer valueConsumer = createConsumer(schema.getValueType(), schema.getValueType().getName(),
        allocator, structVector.getChildrenFromFields().get(1));

    AvroStructConsumer internalConsumer =
        new AvroStructConsumer(structVector, new Consumer[] {keyConsumer, valueConsumer});

    return new AvroMapConsumer(mapVector, internalConsumer);
  }

  private static Consumer createUnionConsumer(Schema schema, String name, BufferAllocator allocator, FieldVector v) {
    int size = schema.getTypes().size();
    long nullCount = schema.getTypes().stream().filter(s -> s.getType() == Type.NULL).count();

    // union only has one type, convert to primitive type
    if (size == 1) {
      Schema subSchema = schema.getTypes().get(0);
      return createConsumer(subSchema, name, allocator, v);

      // size == 2 and has null type, convert to nullable primitive type
    } else if (size == 2 && nullCount == 1) {
      Schema nullSchema = schema.getTypes().stream().filter(s -> s.getType() == Type.NULL).findFirst().get();
      int nullIndex = schema.getTypes().indexOf(nullSchema);
      Schema subSchema = schema.getTypes().stream().filter(s -> s.getType() != Type.NULL).findFirst().get();
      Preconditions.checkNotNull(subSchema, "schema should not be null.");
      return createConsumer(subSchema, name, true, nullIndex, allocator, v);

      // real union type
    } else {

      UnionVector unionVector;
      if (v == null) {
        final Field field = avroSchemaToField(schema, name);
        unionVector = (UnionVector) field.createVector(allocator);
      } else {
        unionVector = (UnionVector) v;
      }

      List<FieldVector> childVectors = unionVector.getChildrenFromFields();

      Consumer[] delegates = new Consumer[size];
      Types.MinorType[] types = new Types.MinorType[size];

      for (int i = 0; i < size; i++) {
        FieldVector child = childVectors.get(i);
        Schema subSchema = schema.getTypes().get(i);
        Consumer delegate = createConsumer(subSchema, subSchema.getName(), allocator, child);
        delegates[i] = delegate;
        types[i] = child.getMinorType();
      }
      return new AvroUnionsConsumer(unionVector, delegates, types);
    }
  }

  private static Map<String, String> getMetaData(Schema schema) {
    Map<String, String> metadata = new HashMap<>();
    schema.getObjectProps().forEach((k,v) -> metadata.put(k, v.toString()));
    return metadata;
  }

  /**
   * Read data from {@link Decoder} and generate a {@link VectorSchemaRoot}.
   * @param schema avro schema
   * @param decoder avro decoder to read data from
   */
  public static VectorSchemaRoot avroToArrowVectors(Schema schema, Decoder decoder, BufferAllocator allocator)
      throws IOException {

    List<FieldVector> vectors = new ArrayList<>();
    List<Consumer> consumers = new ArrayList<>();

    Schema.Type type = schema.getType();
    if (type == Type.RECORD) {
      for (Schema.Field field : schema.getFields()) {
        Consumer consumer = createConsumer(field.schema(), field.name(), allocator);
        consumers.add(consumer);
        vectors.add(consumer.getVector());
      }
    } else if (type == Type.ENUM) {
      throw new UnsupportedOperationException();
    } else {
      Consumer consumer = createConsumer(schema, "", allocator);
      consumers.add(consumer);
      vectors.add(consumer.getVector());
    }

    Preconditions.checkArgument(vectors.size() == consumers.size(),
        "vectors size not equals consumers size");

    List<Field> fields = vectors.stream().map(t -> t.getField()).collect(Collectors.toList());

    VectorSchemaRoot root = new VectorSchemaRoot(fields, vectors, 0);

    CompositeAvroConsumer compositeConsumer = new CompositeAvroConsumer(consumers);

    int valueCount = 0;
    try {
      while (true) {
        compositeConsumer.consume(decoder, root);
        valueCount++;
      }
    } catch (EOFException eof) {
      // reach the end of encoder stream.
      root.setRowCount(valueCount);
    } catch (Exception e) {
      compositeConsumer.close();
      throw new RuntimeException("Error occurs while consume process.", e);
    }

    return root;
  }
}
