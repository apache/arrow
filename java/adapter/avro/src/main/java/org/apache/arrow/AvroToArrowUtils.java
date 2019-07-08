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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

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
import org.apache.arrow.vector.complex.impl.VarBinaryWriterImpl;
import org.apache.arrow.vector.holders.BigIntHolder;
import org.apache.arrow.vector.holders.BitHolder;
import org.apache.arrow.vector.holders.Float4Holder;
import org.apache.arrow.vector.holders.Float8Holder;
import org.apache.arrow.vector.holders.IntHolder;
import org.apache.arrow.vector.holders.VarBinaryHolder;
import org.apache.arrow.vector.holders.VarCharHolder;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;

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
  private static ArrowType getArrowTypeForAvroField(Schema.Field field) {

    Preconditions.checkNotNull(field, "Avro Field object can't be null");

    Type avroType = field.schema().getType();
    final ArrowType arrowType;

    switch (avroType) {
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
        arrowType = new ArrowType.FloatingPoint(SINGLE);
        break;
      case DOUBLE:
        arrowType = new ArrowType.FloatingPoint(DOUBLE);
        break;
      case BYTES:
        arrowType = new ArrowType.Binary();
        break;
      default:
        // no-op, shouldn't get here
        throw new RuntimeException("Can't convert avro type %s to arrow type." + avroType.getName());
    }

    return arrowType;
  }

  /**
   * Create Arrow {@link org.apache.arrow.vector.types.pojo.Schema} object for the given Avro {@link Schema}.
   */
  public static org.apache.arrow.vector.types.pojo.Schema avroToArrowSchema(Schema schema) {
    Preconditions.checkNotNull(schema, "Avro Schema object can't be null");

    List<Field> arrowFields = new ArrayList<>();
    for (Schema.Field field : schema.getFields()) {
      final ArrowType arrowType = getArrowTypeForAvroField(field);
      if (arrowType != null) {
        final FieldType fieldType = new FieldType(true, arrowType, /* dictionary encoding */ null, null);
        List<Field> children = null;
        //TODO handle complex type children fields
        arrowFields.add(new Field(field.name(), fieldType, children));
      }
    }
    return new org.apache.arrow.vector.types.pojo.Schema(arrowFields, null);
  }

  /**
   * Iterate the given Avro {@link DataFileReader} object to fetch the data and transpose it to populate
   * the given Arrow Vector objects.
   * @param reader avro reader to read data.
   * @param root Arrow {@link VectorSchemaRoot} object to populate
   */
  public static void avroToArrowVectors(DataFileReader<GenericRecord> reader, VectorSchemaRoot root) {

    Preconditions.checkNotNull(reader, "Avro DataFileReader object can't be null");
    Preconditions.checkNotNull(root, "VectorSchemaRoot object can't be null");

    allocateVectors(root, DEFAULT_BUFFER_SIZE);

    int rowCount = 0;
    while (reader.hasNext()) {
      GenericRecord record = reader.next();
      for (Schema.Field field : reader.getSchema().getFields()) {
        Object value = record.get(field.name());
        avroToFieldVector(
            value,
            field.schema().getType(),
            rowCount,
            root.getVector(field.name()));
      }
      rowCount++;
    }
    root.setRowCount(rowCount);
  }

  /**
   * Put the value into the vector at specific position.
   */
  public static void avroToFieldVector(Object value, Type avroType, int rowCount, FieldVector vector) {
    switch (avroType) {
      case STRING:
        updateVector((VarCharVector) vector, value, rowCount);
        break;
      case INT:
        updateVector((IntVector) vector, value, rowCount);
        break;
      case BOOLEAN:
        updateVector((BitVector)vector, value, rowCount);
        break;
      case LONG:
        updateVector((BigIntVector) vector, value, rowCount);
        break;
      case FLOAT:
        updateVector((Float4Vector) vector, value, rowCount);
        break;
      case DOUBLE:
        updateVector((Float8Vector) vector, value, rowCount);
        break;
      case BYTES:
        updateVector((VarBinaryVector) vector, value, rowCount);
        break;
      default:
        // no-op, shouldn't get here
        throw new RuntimeException("Can't convert avro type %s to arrow type." + avroType.getName());
    }
  }

  private static void updateVector(VarBinaryVector varBinaryVector, Object value, int rowCount) {
    VarBinaryHolder holder = new VarBinaryHolder();
    Preconditions.checkNotNull(value, "value should not be null");
    VarBinaryWriterImpl writer = new VarBinaryWriterImpl(varBinaryVector);

    byte[] bytes = ((ByteBuffer) value).array();
    holder.buffer = varBinaryVector.getAllocator().buffer(bytes.length);
    holder.buffer.setBytes(0, bytes, 0, bytes.length);
    holder.start = 0;
    holder.end = bytes.length;

    writer.setPosition(rowCount);
    writer.write(holder);
  }

  private static void updateVector(Float4Vector float4Vector, Object value, int rowCount) {
    Float4Holder holder = new Float4Holder();
    Preconditions.checkNotNull(value, "value should not be null");
    holder.value = (float) value;
    float4Vector.setSafe(rowCount, holder);
    float4Vector.setValueCount(rowCount + 1);
  }

  private static void updateVector(Float8Vector float8Vector, Object value, int rowCount) {
    Float8Holder holder = new Float8Holder();
    Preconditions.checkNotNull(value, "value should not be null");
    holder.value = (double) value;
    float8Vector.setSafe(rowCount, holder);
    float8Vector.setValueCount(rowCount + 1);
  }

  private static void updateVector(BigIntVector bigIntVector, Object value, int rowCount) {
    BigIntHolder holder = new BigIntHolder();
    Preconditions.checkNotNull(value, "value should not be null");
    holder.value = (long) value;
    bigIntVector.setSafe(rowCount, holder);
    bigIntVector.setValueCount(rowCount + 1);
  }

  private static void updateVector(BitVector bitVector, Object value, int rowCount) {
    BitHolder holder = new BitHolder();
    Preconditions.checkNotNull(value, "value should not be null");
    holder.value = (boolean) value ? 1 : 0;
    bitVector.setSafe(rowCount, holder);
    bitVector.setValueCount(rowCount + 1);
  }

  private static void updateVector(IntVector intVector, Object value, int rowCount) {
    IntHolder holder = new IntHolder();
    Preconditions.checkNotNull(value, "value should not be null");
    holder.value = (int) value;
    intVector.setSafe(rowCount, holder);
    intVector.setValueCount(rowCount + 1);
  }

  private static void updateVector(VarCharVector varcharVector, Object value, int rowCount) {
    VarCharHolder holder = new VarCharHolder();
    Preconditions.checkNotNull(value, "value should not be null");
    varcharVector.setIndexDefined(rowCount);
    byte[] bytes = ((Utf8)value).getBytes();
    holder.buffer = varcharVector.getAllocator().buffer(bytes.length);
    holder.buffer.setBytes(0, bytes, 0, bytes.length);
    holder.start = 0;
    holder.end = bytes.length;
    varcharVector.setSafe(rowCount, holder);
    varcharVector.setValueCount(rowCount + 1);
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
