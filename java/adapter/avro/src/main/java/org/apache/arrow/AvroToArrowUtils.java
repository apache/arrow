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
import org.apache.arrow.consumers.AvroUnionsConsumer;
import org.apache.arrow.consumers.Consumer;
import org.apache.arrow.consumers.NullableTypeConsumer;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.BaseFixedWidthVector;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
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

  private static final int DEFAULT_BUFFER_SIZE = 256;
  public static final String NULL_INDEX = "nullIndex";
  public static final String Field_INDEX = "index";

  /**
   * Creates a {@link Field} from the {@link Schema}
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
  private static Field getArrowField(Schema schema, String name, boolean nullable) {
    Preconditions.checkNotNull(schema, "Avro schema object can't be null");

    Type type = schema.getType();
    ArrowType arrowType;

    switch (type) {
      case UNION:
        return getUnionField(schema, name);
      case STRING:
        arrowType = new ArrowType.Utf8();
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
        arrowType =  new ArrowType.FloatingPoint(SINGLE);
        break;
      case DOUBLE:
        arrowType = new ArrowType.FloatingPoint(DOUBLE);
        break;
      case BYTES:
        arrowType = new ArrowType.Binary();
        break;
      default:
        // no-op, shouldn't get here
        throw new RuntimeException("Can't convert avro type %s to arrow type." + type.getName());
    }

    final FieldType fieldType =  new FieldType(nullable, arrowType, /*dictionary=*/null,
        getMetaData(schema));
    return new Field(name, fieldType, null);
  }

  private static Field getUnionField(Schema schema, String name) {
    int size = schema.getTypes().size();
    long nullCount = schema.getTypes().stream().filter(s -> s.getType() == Type.NULL).count();

    // union only has one type, convert to primitive type
    if (size == 1) {
      Schema subSchema = schema.getTypes().get(0);
      return getArrowField(subSchema, name,true);

      // size == 2 and has null type, convert to nullable primitive type
    } else if (size == 2 && nullCount == 1) {
      Schema nullSchema = schema.getTypes().stream().filter(s -> s.getType() == Type.NULL).findFirst().get();
      String nullIndex = String.valueOf(schema.getTypes().indexOf(nullSchema));
      Schema subSchema = schema.getTypes().stream().filter(s -> s.getType() != Type.NULL).findFirst().get();
      Preconditions.checkNotNull(subSchema);
      subSchema.addProp(NULL_INDEX, nullIndex);
      return getArrowField(subSchema, name,true);

      // real union type
    } else {
      List<Field> children = new ArrayList<>();

      for (int i = 0; i < size; i++) {
        Schema subSchema = schema.getTypes().get(i);
        if (subSchema.getType() != Type.NULL) {
          subSchema.addProp(Field_INDEX, i);
          Field arrowSubField = getArrowField(subSchema, createFieldName(subSchema), /*nullable=*/false);
          children.add(arrowSubField);
        }
      }
      // arrow Union type always nullable, and settings for it's FieldType seems not work
      final FieldType fieldType =  new FieldType(/*nullable=*/true,
          new ArrowType.Union(UnionMode.Dense, null), /*dictionary=*/null, getMetaData(schema));
      return new Field(name, fieldType, children);
    }
  }

  // keep field name with MinorType name, so UnionVector.getVector(i) will return existing vectors.
  private static String createFieldName(Schema schema) {
    String minorTypeName;
    switch (schema.getType()) {
      case STRING:
        minorTypeName = Types.MinorType.VARCHAR.toString();
        break;
      case INT:
        minorTypeName = Types.MinorType.INT.toString();
        break;
      case BOOLEAN:
        minorTypeName = Types.MinorType.BIT.toString();
        break;
      case FLOAT:
        minorTypeName = Types.MinorType.FLOAT4.toString();
        break;
      case DOUBLE:
        minorTypeName = Types.MinorType.FLOAT8.toString();
        break;
      case LONG:
        minorTypeName = Types.MinorType.BIGINT.toString();
        break;
      case BYTES:
        minorTypeName = Types.MinorType.VARBINARY.toString();
        break;
      default:
        throw new UnsupportedOperationException();
    }

    return minorTypeName.toLowerCase();
  }

  private static Map<String, String> getMetaData(Schema schema) {
    Map<String, String> metadata = new HashMap<>();
    schema.getObjectProps().forEach((k,v) -> metadata.put(k, v.toString()));
    return metadata;
  }

  /**
   * Create Arrow {@link org.apache.arrow.vector.types.pojo.Schema} object for the given Avro {@link Schema}.
   */
  public static org.apache.arrow.vector.types.pojo.Schema avroToArrowSchema(Schema schema) {

    Preconditions.checkNotNull(schema, "Avro Schema object can't be null");
    List<Field> arrowFields = new ArrayList<>();

    Schema.Type type = schema.getType();
    final Map<String, String> metadata = getMetaData(schema);

    if (type == Type.RECORD) {
      for (Schema.Field field : schema.getFields()) {
        arrowFields.add(getArrowField(field.schema(), field.name(),false));
      }
    } else if (type == Type.MAP) {
      throw new UnsupportedOperationException();
    } else if (type == Type.ARRAY) {
      throw new UnsupportedOperationException();
    } else if (type == Type.ENUM) {
      throw new UnsupportedOperationException();
    } else {
      arrowFields.add(getArrowField(schema, "", false));
    }

    return new org.apache.arrow.vector.types.pojo.Schema(arrowFields, /*metadata=*/ metadata);
  }

  private static Consumer createConsumer(ValueVector vector) {
    ArrowType arrowType = vector.getField().getType();
    if (!arrowType.isComplex()) {
      return createPrimitiveConsumer(vector);
    } else if (arrowType.getTypeID() == ArrowType.Union.TYPE_TYPE) {
      return createUnionConsumer(vector);
    } else {
      throw new UnsupportedOperationException();
    }
  }

  private static Consumer createUnionConsumer(ValueVector vector) {
    UnionVector unionVector = (UnionVector) vector;

    Map<Integer, Consumer> delegates = new HashMap<>();
    Map<Integer, Types.MinorType> types = new HashMap<>();

    List<FieldVector> internals = unionVector.getChildrenFromFields();
    for (int i = 0; i < internals.size(); i++) {
      ValueVector v = internals.get(i);
      int index = Integer.parseInt(v.getField().getMetadata().get(Field_INDEX));
      Consumer consumer = createConsumer(v);
      delegates.put(index, consumer);
      types.put(index, v.getMinorType());
    }

    return new AvroUnionsConsumer(unionVector, delegates, types);
  }

  /**
   * Create primitive consumer to read data from decoder, will reduce boxing/unboxing operations.
   */
  private static Consumer createPrimitiveConsumer(ValueVector vector) {

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

    if (vector.getField().isNullable()) {
      int nullIndex = getNullFieldIndex(vector.getField());
      return new NullableTypeConsumer(consumer, nullIndex);
    }

    return consumer;
  }

  /**
   * Get avro null field index from vector field metadata.
   */
  private static int getNullFieldIndex(Field field) {
    Map<String, String> metadata = field.getMetadata();
    Preconditions.checkNotNull(metadata, "metadata should not be null when vector is nullable");
    String index = metadata.get(AvroToArrowUtils.NULL_INDEX);
    Preconditions.checkNotNull(index, "nullIndex should not be null when vector is nullable");
    return Integer.parseInt(index);
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

    // create consumers
    Consumer[] consumers = new Consumer[root.getFieldVectors().size()];
    for (int i = 0; i < root.getFieldVectors().size(); i++) {
      FieldVector vector = root.getFieldVectors().get(i);
      consumers[i] = createConsumer(vector);
    }

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
