/**
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
package org.apache.arrow.vector.util;

import java.util.Arrays;
import java.util.List;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import com.google.common.base.Objects;

/**
 * Utility class for validating arrow data structures
 */
public class Validator {

    /**
     * Validate two arrow schemas are equal.
     *
     * @param schema1 the 1st shema to compare
     * @param schema2 the 2nd shema to compare
     * @throws IllegalArgumentException if they are different.
     */
    public static void compareSchemas(Schema schema1, Schema schema2) {
        if (!schema2.equals(schema1)) {
            throw new IllegalArgumentException("Different schemas:\n" + schema2 + "\n" + schema1);
        }
    }

    /**
     * Validate two arrow vectorSchemaRoot are equal.
     *
     * @param root1 the 1st shema to compare
     * @param root2 the 2nd shema to compare
     * @throws IllegalArgumentException if they are different.
     */
    public static void compareVectorSchemaRoot(VectorSchemaRoot root1, VectorSchemaRoot root2) {
        compareSchemas(root2.getSchema(), root1.getSchema());
        if (root1.getRowCount() != root2.getRowCount()) {
            throw new IllegalArgumentException("Different row count:\n" + root1.getRowCount() + "\n" + root2.getRowCount());
        }
        List<FieldVector> arrowVectors = root1.getFieldVectors();
        List<FieldVector> jsonVectors = root2.getFieldVectors();
        if (arrowVectors.size() != jsonVectors.size()) {
            throw new IllegalArgumentException("Different column count:\n" + arrowVectors.size() + "\n" + jsonVectors.size());
        }
        for (int i = 0; i < arrowVectors.size(); i++) {
            Field field = root1.getSchema().getFields().get(i);
            FieldVector arrowVector = arrowVectors.get(i);
            FieldVector jsonVector = jsonVectors.get(i);
            int valueCount = arrowVector.getAccessor().getValueCount();
            if (valueCount != jsonVector.getAccessor().getValueCount()) {
                throw new IllegalArgumentException("Different value count for field " + field + " : " + valueCount + " != " + jsonVector.getAccessor().getValueCount());
            }
            for (int j = 0; j < valueCount; j++) {
                Object arrow = arrowVector.getAccessor().getObject(j);
                Object json = jsonVector.getAccessor().getObject(j);
                if (!equals(field.getType(), arrow, json)) {
                    throw new IllegalArgumentException(
                            "Different values in column:\n" + field + " at index " + j + ": " + arrow + " != " + json);
                }
            }
        }
    }

    static boolean equals(ArrowType type, final Object o1, final Object o2) {
        if (type instanceof ArrowType.FloatingPoint) {
            ArrowType.FloatingPoint fpType = (ArrowType.FloatingPoint) type;
            switch (fpType.getPrecision()) {
                case DOUBLE:
                    return equalEnough((Double)o1, (Double)o2);
                case SINGLE:
                    return equalEnough((Float)o1, (Float)o2);
                case HALF:
                default:
                    throw new UnsupportedOperationException("unsupported precision: " + fpType);
            }
        } else if (type instanceof ArrowType.Binary) {
            return Arrays.equals((byte[]) o1, (byte[]) o2);
        }

        return Objects.equal(o1, o2);
    }

    static boolean equalEnough(Float f1, Float f2) {
        if (f1 == null || f2 == null) {
            return f1 == null && f2 == null;
        }
        if (f1.isNaN()) {
            return f2.isNaN();
        }
        if (f1.isInfinite()) {
            return f2.isInfinite() && Math.signum(f1) == Math.signum(f2);
        }
        float average = Math.abs((f1 + f2) / 2);
        float differenceScaled = Math.abs(f1 - f2) / (average == 0.0f ? 1f : average);
        return differenceScaled < 1.0E-6f;
    }

    static boolean equalEnough(Double f1, Double f2) {
        if (f1 == null || f2 == null) {
            return f1 == null && f2 == null;
        }
        if (f1.isNaN()) {
            return f2.isNaN();
        }
        if (f1.isInfinite()) {
            return f2.isInfinite() && Math.signum(f1) == Math.signum(f2);
        }
        double average = Math.abs((f1 + f2) / 2);
        double differenceScaled = Math.abs(f1 - f2) / (average == 0.0d ? 1d : average);
        return differenceScaled < 1.0E-12d;
    }
}
