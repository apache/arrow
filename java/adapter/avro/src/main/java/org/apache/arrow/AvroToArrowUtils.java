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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.arrow.consumers.AvroBooleanConsumer;
import org.apache.arrow.consumers.AvroBytesConsumer;
import org.apache.arrow.consumers.AvroDoubleConsumer;
import org.apache.arrow.consumers.AvroFloatConsumer;
import org.apache.arrow.consumers.AvroIntConsumer;
import org.apache.arrow.consumers.AvroLongConsumer;
import org.apache.arrow.consumers.AvroNullConsumer;
import org.apache.arrow.consumers.AvroStringConsumer;
import org.apache.arrow.consumers.AvroUnionsConsumer;
import org.apache.arrow.consumers.Consumer;
import org.apache.arrow.consumers.NullableTypeConsumer;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ZeroVector;
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
   * </ul>
   */
  private static Consumer createConsumer(Schema schema, String name, BufferAllocator allocator) {
    return createConsumer(schema, name, false, INVALID_NULL_INDEX, allocator);
  }

  private static Consumer createConsumer(
      Schema schema,
      String name,
      boolean nullable,
      int nullIndex,
      BufferAllocator allocator) {
    Preconditions.checkNotNull(schema, "Avro schema object can't be null");

    Type type = schema.getType();

    final ArrowType arrowType;
    final FieldType fieldType;
    final FieldVector vector;
    final Consumer consumer;

    switch (type) {
      case UNION:
        return createUnionConsumer(schema, name, allocator);
      case STRING:
        arrowType = new ArrowType.Utf8();
        fieldType =  new FieldType(nullable, arrowType, /*dictionary=*/null, getMetaData(schema));
        vector = fieldType.createNewSingleVector(name, allocator, null);
        consumer =  new AvroStringConsumer((VarCharVector) vector);
        break;
      case INT:
        arrowType = new ArrowType.Int(32, /*signed=*/true);
        fieldType =  new FieldType(nullable, arrowType, /*dictionary=*/null, getMetaData(schema));
        vector = fieldType.createNewSingleVector(name, allocator, null);
        consumer = new AvroIntConsumer((IntVector) vector);
        break;
      case BOOLEAN:
        arrowType = new ArrowType.Bool();
        fieldType =  new FieldType(nullable, arrowType, /*dictionary=*/null, getMetaData(schema));
        vector = fieldType.createNewSingleVector(name, allocator, null);
        consumer = new AvroBooleanConsumer((BitVector) vector);
        break;
      case LONG:
        arrowType = new ArrowType.Int(64, /*signed=*/true);
        fieldType =  new FieldType(nullable, arrowType, /*dictionary=*/null, getMetaData(schema));
        vector = fieldType.createNewSingleVector(name, allocator, null);
        consumer =  new AvroLongConsumer((BigIntVector) vector);
        break;
      case FLOAT:
        arrowType =  new ArrowType.FloatingPoint(SINGLE);
        fieldType =  new FieldType(nullable, arrowType, /*dictionary=*/null, getMetaData(schema));
        vector = fieldType.createNewSingleVector(name, allocator, null);
        consumer = new AvroFloatConsumer((Float4Vector) vector);
        break;
      case DOUBLE:
        arrowType = new ArrowType.FloatingPoint(DOUBLE);
        fieldType =  new FieldType(nullable, arrowType, /*dictionary=*/null, getMetaData(schema));
        vector = fieldType.createNewSingleVector(name, allocator, null);
        consumer = new AvroDoubleConsumer((Float8Vector) vector);
        break;
      case BYTES:
        arrowType = new ArrowType.Binary();
        fieldType =  new FieldType(nullable, arrowType, /*dictionary=*/null, getMetaData(schema));
        vector = fieldType.createNewSingleVector(name, allocator, null);
        consumer = new AvroBytesConsumer((VarBinaryVector) vector);
        break;
      case NULL:
        arrowType = new ArrowType.Null();
        fieldType =  new FieldType(nullable, arrowType, /*dictionary=*/null, getMetaData(schema));
        vector = fieldType.createNewSingleVector(name, allocator, null);
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

  private static Consumer createUnionConsumer(Schema schema, String name, BufferAllocator allocator) {
    int size = schema.getTypes().size();
    long nullCount = schema.getTypes().stream().filter(s -> s.getType() == Type.NULL).count();

    // union only has one type, convert to primitive type
    if (size == 1) {
      Schema subSchema = schema.getTypes().get(0);
      return createConsumer(subSchema, name, allocator);

      // size == 2 and has null type, convert to nullable primitive type
    } else if (size == 2 && nullCount == 1) {
      Schema nullSchema = schema.getTypes().stream().filter(s -> s.getType() == Type.NULL).findFirst().get();
      int nullIndex = schema.getTypes().indexOf(nullSchema);
      Schema subSchema = schema.getTypes().stream().filter(s -> s.getType() != Type.NULL).findFirst().get();
      Preconditions.checkNotNull(subSchema, "schema should not be null.");
      return createConsumer(subSchema, name, true, nullIndex, allocator);

      // real union type
    } else {

      final FieldType fieldType =  new FieldType(/*nullable=*/true,
          new ArrowType.Union(UnionMode.Sparse, null), /*dictionary=*/null, getMetaData(schema));
      UnionVector unionVector =
          (UnionVector) fieldType.createNewSingleVector(name, allocator, null);

      Consumer[] delegates = new Consumer[size];
      Types.MinorType[] types = new Types.MinorType[size];

      for (int i = 0; i < size; i++) {
        Schema subSchema = schema.getTypes().get(i);
        Consumer delegate = createConsumer(subSchema, subSchema.getName(), allocator);
        unionVector.directAddVector(delegate.getVector());
        delegates[i] = delegate;
        types[i] = delegate.getVector().getMinorType();
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
    } else if (type == Type.MAP) {
      throw new UnsupportedOperationException();
    } else if (type == Type.ARRAY) {
      throw new UnsupportedOperationException();
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

    int valueCount = 0;
    while (true) {
      try {
        for (Consumer consumer : consumers) {
          consumer.consume(decoder);
        }
        valueCount++;
        //reach end will throw EOFException.
      } catch (EOFException eofException) {
        root.setRowCount(valueCount);
        break;
      }
    }
    return root;
  }
}
