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

import org.apache.arrow.consumers.AvroBooleanConsumer;
import org.apache.arrow.consumers.AvroBytesConsumer;
import org.apache.arrow.consumers.AvroDoubleConsumer;
import org.apache.arrow.consumers.AvroFloatConsumer;
import org.apache.arrow.consumers.AvroIntConsumer;
import org.apache.arrow.consumers.AvroLongConsumer;
import org.apache.arrow.consumers.AvroStringConsumer;
import org.apache.arrow.consumers.Consumer;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.BaseFixedWidthVector;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
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

  private static final int DEFAULT_BUFFER_SIZE = 256;

  /**
   * Creates an {@link org.apache.arrow.vector.types.pojo.ArrowType} from the {@link Schema.Field}
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
  private static ArrowType getArrowType(Type type) {

    Preconditions.checkNotNull(type, "Avro type object can't be null");

    switch (type) {
      case STRING:
        return new ArrowType.Utf8();
      case INT:
        return new ArrowType.Int(32, /*signed=*/true);
      case BOOLEAN:
        return new ArrowType.Bool();
      case LONG:
        return new ArrowType.Int(64, /*signed=*/true);
      case FLOAT:
        return new ArrowType.FloatingPoint(SINGLE);
      case DOUBLE:
        return new ArrowType.FloatingPoint(DOUBLE);
      case BYTES:
        return new ArrowType.Binary();
      default:
        // no-op, shouldn't get here
        throw new RuntimeException("Can't convert avro type %s to arrow type." + type.getName());
    }
  }

  /**
   * Create Arrow {@link org.apache.arrow.vector.types.pojo.Schema} object for the given Avro {@link Schema}.
   */
  public static org.apache.arrow.vector.types.pojo.Schema avroToArrowSchema(Schema schema) {

    Preconditions.checkNotNull(schema, "Avro Schema object can't be null");
    List<Field> arrowFields = new ArrayList<>();

    Schema.Type type = schema.getType();
    final Map<String, String> metadata = new HashMap<>();
    schema.getObjectProps().forEach((k,v) -> metadata.put(k, v.toString()));

    if (type == Type.RECORD) {
      throw new UnsupportedOperationException();
    } else if (type == Type.MAP) {
      throw new UnsupportedOperationException();
    } else if (type == Type.UNION) {
      throw new UnsupportedOperationException();
    } else if (type == Type.ARRAY) {
      throw new UnsupportedOperationException();
    } else if (type == Type.ENUM) {
      throw new UnsupportedOperationException();
    } else if (type == Type.NULL) {
      throw new UnsupportedOperationException();
    } else {
      final FieldType fieldType = new FieldType(true, getArrowType(type), null, null);
      arrowFields.add(new Field("", fieldType, null));
    }

    return new org.apache.arrow.vector.types.pojo.Schema(arrowFields, /*metadata=*/ metadata);
  }

  /**
   * Create consumers to consume avro values from decoder, will reduce boxing/unboxing operations.
   */
  public static Consumer[] createAvroConsumers(VectorSchemaRoot root) {

    Consumer[] consumers = new Consumer[root.getFieldVectors().size()];
    for (int i = 0; i < root.getFieldVectors().size(); i++) {
      FieldVector vector = root.getFieldVectors().get(i);
      Consumer consumer;
      switch (vector.getMinorType()) {
        case INT:
          consumer = new AvroIntConsumer((IntVector) vector);
          break;
        case VARBINARY:
          consumer = new AvroBytesConsumer((VarBinaryVector) vector);
          break;
        case VARCHAR:
          consumer = new AvroStringConsumer((VarCharVector) vector);
          break;
        case BIGINT:
          consumer = new AvroLongConsumer((BigIntVector) vector);
          break;
        case FLOAT4:
          consumer = new AvroFloatConsumer((Float4Vector) vector);
          break;
        case FLOAT8:
          consumer = new AvroDoubleConsumer((Float8Vector) vector);
          break;
        case BIT:
          consumer = new AvroBooleanConsumer((BitVector) vector);
          break;
        default:
          throw new RuntimeException("could not get consumer from type:" + vector.getMinorType());
      }
      consumers[i] = consumer;
    }
    return consumers;
  }

  /**
   * Iterate the given Avro {@link Decoder} object to fetch the data and transpose it to populate
   * the given Arrow Vector objects.
   * @param decoder avro decoder to read data.
   * @param root Arrow {@link VectorSchemaRoot} object to populate
   */
  public static void avroToArrowVectors(Decoder decoder, VectorSchemaRoot root) throws IOException {

    Preconditions.checkNotNull(decoder, "Avro decoder object can't be null");
    Preconditions.checkNotNull(root, "VectorSchemaRoot object can't be null");

    allocateVectors(root, DEFAULT_BUFFER_SIZE);
    Consumer[] consumers = createAvroConsumers(root);
    while (true) {
      try {
        for (Consumer consumer : consumers) {
          consumer.consume(decoder);
        }
        //reach end will throw EOFException.
      } catch (EOFException eofException) {
        break;
      }
    }
  }

  private static void allocateVectors(VectorSchemaRoot root, int size) {
    List<FieldVector> vectors = root.getFieldVectors();
    for (FieldVector fieldVector : vectors) {
      if (fieldVector instanceof BaseFixedWidthVector) {
        ((BaseFixedWidthVector) fieldVector).allocateNew(size);
      } else {
        fieldVector.allocateNew();
      }
      fieldVector.setInitialCapacity(size);
    }
  }
}
