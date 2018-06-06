/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.arrow.gandiva.expression;

import org.apache.arrow.flatbuf.Type;
import org.apache.arrow.gandiva.ipc.GandivaTypes;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

public class ArrowTypeHelper {
    private static void InitArrowTypeInt(ArrowType.Int intType, GandivaTypes.ExtGandivaType.Builder builder) {
        int width = intType.getBitWidth();

        if (intType.getIsSigned()) {
            switch (width) {
                case 8: {
                    builder.setType(GandivaTypes.GandivaType.INT8);
                    break;
                }
                case 16: {
                    builder.setType(GandivaTypes.GandivaType.INT16);
                    break;
                }
                case 32: {
                    builder.setType(GandivaTypes.GandivaType.INT32);
                    break;
                }
                case 64: {
                    builder.setType(GandivaTypes.GandivaType.INT64);
                    break;
                }
                default: {
                    // TODO: Cannot handle. Should throw exception
                    break;
                }
            }
            return;
        }

        // unsigned int
        switch (width) {
            case 8: {
                builder.setType(GandivaTypes.GandivaType.UINT8);
                break;
            }
            case 16: {
                builder.setType(GandivaTypes.GandivaType.UINT16);
                break;
            }
            case 32: {
                builder.setType(GandivaTypes.GandivaType.UINT32);
                break;
            }
            case 64: {
                builder.setType(GandivaTypes.GandivaType.UINT64);
                break;
            }
            default: {
                // TODO: Cannot handle. Should throw exception
                break;
            }
        }
    }

    private static void InitArrowTypeFloat(ArrowType.FloatingPoint floatType, GandivaTypes.ExtGandivaType.Builder builder) {
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
        }
    }

    private static void InitArrowTypeDecimal(ArrowType.Decimal decimalType, GandivaTypes.ExtGandivaType.Builder builder) {
        builder.setPrecision(decimalType.getPrecision());
        builder.setScale(decimalType.getScale());
        builder.setType(GandivaTypes.GandivaType.DECIMAL);
    }

    public static GandivaTypes.ExtGandivaType ArrowTypeToProtobuf(ArrowType arrowType) throws Exception {
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
                ArrowTypeHelper.InitArrowTypeInt((ArrowType.Int)arrowType, builder);
                break;
            }
            case Type.FloatingPoint: { // 3
                ArrowTypeHelper.InitArrowTypeFloat((ArrowType.FloatingPoint)arrowType, builder);
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
                ArrowTypeHelper.InitArrowTypeDecimal((ArrowType.Decimal)arrowType, builder);
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
        }

        if (!builder.hasType()) {
            // type has not been set
            // throw an exception
            throw new Exception("Unhandled type" + arrowType.toString());
        }

        return builder.build();
    }

    public static GandivaTypes.Field ArrowFieldToProtobuf(Field field) throws Exception {
        GandivaTypes.Field.Builder builder = GandivaTypes.Field.newBuilder();

        builder.setName(field.getName());
        builder.setType(ArrowTypeHelper.ArrowTypeToProtobuf(field.getType()));
        builder.setNullable(field.isNullable());

        for(Field child : field.getChildren()) {
            builder.addChildren(ArrowTypeHelper.ArrowFieldToProtobuf(child));
        }

        return builder.build();
    }

    public static GandivaTypes.Schema ArrowSchemaToProtobuf(Schema schema) throws Exception {
        GandivaTypes.Schema.Builder builder = GandivaTypes.Schema.newBuilder();

        for(Field field : schema.getFields()) {
            builder.addColumns(ArrowTypeHelper.ArrowFieldToProtobuf(field));
        }

        return builder.build();
    }
}
