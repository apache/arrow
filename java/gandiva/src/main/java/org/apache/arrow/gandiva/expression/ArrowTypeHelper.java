/*
 * Copyright (C) 2017-2018 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.arrow.gandiva.expression;

import org.apache.arrow.flatbuf.Type;
import org.apache.arrow.gandiva.exceptions.GandivaException;
import org.apache.arrow.gandiva.exceptions.UnsupportedTypeException;
import org.apache.arrow.gandiva.ipc.GandivaTypes;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

public class ArrowTypeHelper {
  static final int WIDTH_8 = 8;
  static final int WIDTH_16 = 16;
  static final int WIDTH_32 = 32;
  static final int WIDTH_64 = 64;

  private static void initArrowTypeInt(ArrowType.Int intType,
                                       GandivaTypes.ExtGandivaType.Builder builder)
          throws GandivaException {
    int width = intType.getBitWidth();

    if (intType.getIsSigned()) {
      switch (width) {
        case WIDTH_8: {
          builder.setType(GandivaTypes.GandivaType.INT8);
          return;
        }
        case WIDTH_16: {
          builder.setType(GandivaTypes.GandivaType.INT16);
          return;
        }
        case WIDTH_32: {
          builder.setType(GandivaTypes.GandivaType.INT32);
          return;
        }
        case WIDTH_64: {
          builder.setType(GandivaTypes.GandivaType.INT64);
          return;
        }
        default: {
          throw new UnsupportedTypeException("Unsupported width for integer type");
        }
      }
    }

    // unsigned int
    switch (width) {
      case WIDTH_8: {
        builder.setType(GandivaTypes.GandivaType.UINT8);
        return;
      }
      case WIDTH_16: {
        builder.setType(GandivaTypes.GandivaType.UINT16);
        return;
      }
      case WIDTH_32: {
        builder.setType(GandivaTypes.GandivaType.UINT32);
        return;
      }
      case WIDTH_64: {
        builder.setType(GandivaTypes.GandivaType.UINT64);
        return;
      }
      default: {
        throw new UnsupportedTypeException("Unsupported width for integer type");
      }
    }
  }

  private static void initArrowTypeFloat(ArrowType.FloatingPoint floatType,
                                         GandivaTypes.ExtGandivaType.Builder builder)
          throws GandivaException {
    switch (floatType.getPrecision()) {
      case HALF: {
        builder.setType(GandivaTypes.GandivaType.HALF_FLOAT);
        break;
      }
      case SINGLE: {
        builder.setType(GandivaTypes.GandivaType.FLOAT);
        break;
      }
      case DOUBLE: {
        builder.setType(GandivaTypes.GandivaType.DOUBLE);
        break;
      }
      default: {
        throw new UnsupportedTypeException("Floating point type with unknown precision");
      }
    }
  }

  private static void initArrowTypeDecimal(ArrowType.Decimal decimalType,
                                           GandivaTypes.ExtGandivaType.Builder builder) {
    builder.setPrecision(decimalType.getPrecision());
    builder.setScale(decimalType.getScale());
    builder.setType(GandivaTypes.GandivaType.DECIMAL);
  }

  /**
   * Converts an arrow type into a protobuf.
   * @param arrowType Arrow type to be converted
   * @return Protobuf representing the arrow type
   */
  public static GandivaTypes.ExtGandivaType arrowTypeToProtobuf(ArrowType arrowType)
          throws GandivaException {
    GandivaTypes.ExtGandivaType.Builder builder = GandivaTypes.ExtGandivaType.newBuilder();

    byte typeId = arrowType.getTypeID().getFlatbufID();
    switch (typeId) {
      case Type.NONE: { // 0
        builder.setType(GandivaTypes.GandivaType.NONE);
        break;
      }
      case Type.Null: { // 1
        // TODO: Need to handle this later
        break;
      }
      case Type.Int: { // 2
        ArrowTypeHelper.initArrowTypeInt((ArrowType.Int) arrowType, builder);
        break;
      }
      case Type.FloatingPoint: { // 3
        ArrowTypeHelper.initArrowTypeFloat((ArrowType.FloatingPoint) arrowType, builder);
        break;
      }
      case Type.Binary: { // 4
        builder.setType(GandivaTypes.GandivaType.BINARY);
        break;
      }
      case Type.Utf8: { // 5
        builder.setType(GandivaTypes.GandivaType.UTF8);
        break;
      }
      case Type.Bool: { // 6
        builder.setType(GandivaTypes.GandivaType.BOOL);
        break;
      }
      case Type.Decimal: { // 7
        ArrowTypeHelper.initArrowTypeDecimal((ArrowType.Decimal) arrowType, builder);
        break;
      }
      case Type.Date: { // 8
        break;
      }
      case Type.Time: { // 9
        break;
      }
      case Type.Timestamp: { // 10
        break;
      }
      case Type.Interval: { // 11
        break;
      }
      case Type.List: { // 12
        break;
      }
      case Type.Struct_: { // 13
        break;
      }
      case Type.Union: { // 14
        break;
      }
      case Type.FixedSizeBinary: { // 15
        break;
      }
      case Type.FixedSizeList: { // 16
        break;
      }
      case Type.Map: { // 17
        break;
      }
      default: {
        break;
      }
    }

    if (!builder.hasType()) {
      // type has not been set
      // throw an exception
      throw new UnsupportedTypeException("Unsupported type" + arrowType.toString());
    }

    return builder.build();
  }

  /**
   * Converts an arrow field object to a protobuf.
   * @param field Arrow field to be converted
   * @return Protobuf representing the arrow field
   */
  public static GandivaTypes.Field arrowFieldToProtobuf(Field field) throws GandivaException {
    GandivaTypes.Field.Builder builder = GandivaTypes.Field.newBuilder();
    builder.setName(field.getName());
    builder.setType(ArrowTypeHelper.arrowTypeToProtobuf(field.getType()));
    builder.setNullable(field.isNullable());

    for (Field child : field.getChildren()) {
      builder.addChildren(ArrowTypeHelper.arrowFieldToProtobuf(child));
    }

    return builder.build();
  }

  /**
   * Converts a schema object to a protobuf.
   * @param schema Schema object to be converted
   * @return Protobuf representing a schema object
   */
  public static GandivaTypes.Schema arrowSchemaToProtobuf(Schema schema) throws GandivaException {
    GandivaTypes.Schema.Builder builder = GandivaTypes.Schema.newBuilder();

    for (Field field : schema.getFields()) {
      builder.addColumns(ArrowTypeHelper.arrowFieldToProtobuf(field));
    }

    return builder.build();
  }
}
