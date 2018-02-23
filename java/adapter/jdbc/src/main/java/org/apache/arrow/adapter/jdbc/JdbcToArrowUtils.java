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

package org.apache.arrow.adapter.jdbc;

import com.google.common.collect.ImmutableList;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.sql.*;
import java.util.List;

import static org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE;
import static org.apache.arrow.vector.types.FloatingPointPrecision.SINGLE;


/**
 *
 */
public class JdbcToArrowUtils {

    /**
     * JDBC type Java type
       CHAR	String
       VARCHAR String
       LONGVARCHAR String
       NUMERIC java.math.BigDecimal
       DECIMAL java.math.BigDecimal
       BIT boolean
       TINYINT byte
       SMALLINT short
       INTEGER int
       BIGINT long
       REAL float
       FLOAT double
       DOUBLE double
       BINARY byte[]
       VARBINARY byte[]
       LONGVARBINARY byte[]
       DATE java.sql.Date
       TIME java.sql.Time
       TIMESTAMP java.sql.Timestamp

     * @param rsmd
     * @return
     * @throws SQLException
     */
    public static Schema jdbcToArrowSchema(ResultSetMetaData rsmd) throws SQLException {

        assert rsmd != null;

        ImmutableList.Builder<Field> fields = ImmutableList.builder();
        int columnCount = rsmd.getColumnCount();
        for (int i = 1; i <= columnCount; i++) {
            String columnName = rsmd.getColumnName(i);
            switch (rsmd.getColumnType(i)) {
                case Types.BOOLEAN:
                case Types.BIT:
                    fields.add(new Field(columnName, FieldType.nullable(new ArrowType.Bool()), null));
                    break;
                case Types.TINYINT:
                    fields.add(new Field(columnName, FieldType.nullable(new ArrowType.Int(8, true)), null));
                    break;
                case Types.SMALLINT:
                    fields.add(new Field(columnName, FieldType.nullable(new ArrowType.Int(16, true)), null));
                    break;
                case Types.INTEGER:
                    fields.add(new Field(columnName, FieldType.nullable(new ArrowType.Int(32, true)), null));
                    break;
                case Types.BIGINT:
                    fields.add(new Field(columnName, FieldType.nullable(new ArrowType.Int(64, true)), null));
                    break;
                case Types.NUMERIC:
                case Types.DECIMAL:
                    int precision = rsmd.getPrecision(i);
                    int scale = rsmd.getScale(i);
                    fields.add(new Field(columnName, FieldType.nullable(new ArrowType.Decimal(precision, scale)), null));
                    break;
                case Types.REAL:
                case Types.FLOAT:
                    fields.add(new Field(columnName, FieldType.nullable(new ArrowType.FloatingPoint(SINGLE)), null));
                    break;
                case Types.DOUBLE:
                    fields.add(new Field(columnName, FieldType.nullable(new ArrowType.FloatingPoint(DOUBLE)), null));
                    break;
                case Types.CHAR:
                case Types.VARCHAR:
                case Types.LONGVARCHAR:
                    fields.add(new Field(columnName, FieldType.nullable(new ArrowType.Utf8()), null));
                    break;
                case Types.DATE:
                    fields.add(new Field(columnName, FieldType.nullable(new ArrowType.Date(DateUnit.MILLISECOND)), null));
                    break;
                case Types.TIME:
                    fields.add(new Field(columnName, FieldType.nullable(new ArrowType.Time(TimeUnit.MILLISECOND, 32)), null));
                    break;
                case Types.TIMESTAMP:
                    // timezone is null
                    fields.add(new Field(columnName, FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MILLISECOND, null)), null));
                    break;
                case Types.BINARY:
                case Types.VARBINARY:
                case Types.LONGVARBINARY:
                    fields.add(new Field(columnName, FieldType.nullable(new ArrowType.Binary()), null));
                    break;
                case Types.ARRAY:
                    // not handled
//                    fields.add(new Field("list", FieldType.nullable(new ArrowType.List()), null));
                   break;
                case Types.CLOB:
                    fields.add(new Field(columnName, FieldType.nullable(new ArrowType.Utf8()), null));
                    break;
                case Types.BLOB:
                    fields.add(new Field(columnName, FieldType.nullable(new ArrowType.Binary()), null));
                    break;

                default:
                    // no-op
                    break;
            }
        }

        return new Schema(fields.build(), null);
    }

    public static void allocateVectors(VectorSchemaRoot root, int size) {
        List<FieldVector> vectors = root.getFieldVectors();
        for (FieldVector fieldVector: vectors) {
            if (fieldVector instanceof BaseFixedWidthVector) {
                ((BaseFixedWidthVector) fieldVector).allocateNew(size);
            } else {
                fieldVector.allocateNew();
            }
            fieldVector.setInitialCapacity(size);
        }
    }

    public static void jdbcToArrowVectors(ResultSet rs, VectorSchemaRoot root, int size) throws Exception {

        assert rs != null;
        assert root != null;

        ResultSetMetaData rsmd = rs.getMetaData();
        int columnCount = rsmd.getColumnCount();

        int rowCount = 0;
        while (rs.next()) {
            // for each column get the value based on the type

            // need to change this to build Java lists and then build Arrow vectors
            for (int i = 1; i <= columnCount; i++) {
                String columnName = rsmd.getColumnName(i);
                switch (rsmd.getColumnType(i)) {
                    case Types.BOOLEAN:
                    case Types.BIT:
                        BitVector bitVector = (BitVector) root.getVector(columnName);
                        bitVector.setSafe(rowCount, rs.getBoolean(i)? 1: 0);
                        bitVector.setValueCount(rowCount + 1);
                        break;
                    case Types.TINYINT:
                        TinyIntVector tinyIntVector = (TinyIntVector)root.getVector(columnName);
                        tinyIntVector.setSafe(rowCount, rs.getInt(i));
                        tinyIntVector.setValueCount(rowCount + 1);
                        break;
                    case Types.SMALLINT:
                        SmallIntVector smallIntVector = (SmallIntVector)root.getVector(columnName);
                        smallIntVector.setSafe(rowCount, rs.getInt(i));
                        smallIntVector.setValueCount(rowCount + 1);
                        break;
                    case Types.INTEGER:
                        IntVector intVector = (IntVector)root.getVector(columnName);
                        intVector.setSafe(rowCount, rs.getInt(i));
                        intVector.setValueCount(rowCount + 1);
                        break;
                    case Types.BIGINT:
                        BigIntVector bigIntVector = (BigIntVector)root.getVector(columnName);
                        bigIntVector.setSafe(rowCount, rs.getInt(i));
                        bigIntVector.setValueCount(rowCount + 1);
                        break;
                    case Types.NUMERIC:
                    case Types.DECIMAL:
                        DecimalVector decimalVector = (DecimalVector)root.getVector(columnName);
                        decimalVector.setSafe(rowCount, rs.getBigDecimal(i));
                        decimalVector.setValueCount(rowCount + 1);
                        break;
                    case Types.REAL:
                    case Types.FLOAT:
                        Float4Vector float4Vector = (Float4Vector)root.getVector(columnName);
                        float4Vector.setSafe(rowCount, rs.getFloat(i));
                        float4Vector.setValueCount(rowCount + 1);
                        break;
                    case Types.DOUBLE:
                        Float8Vector float8Vector = (Float8Vector)root.getVector(columnName);
                        float8Vector.setSafe(rowCount, rs.getDouble(i));
                        float8Vector.setValueCount(rowCount + 1);
                        break;
                    case Types.CHAR:
                    case Types.VARCHAR:
                    case Types.LONGVARCHAR:
                        // TODO - How to handle the buffer size if it starts exceeding the iniital allocated buffer
                        VarCharVector varcharVector = (VarCharVector)root.getVector(columnName);
                        String value = rs.getString(i);
                        varcharVector.setIndexDefined(i);
                        varcharVector.setValueLengthSafe(i, value.length());
                        varcharVector.setSafe(rowCount, value.getBytes(), 0, value.length());
                        varcharVector.setValueCount(rowCount + 1);
                        break;
                    case Types.DATE:
                        DateMilliVector dateMilliVector = (DateMilliVector)root.getVector(columnName);
                        dateMilliVector.setSafe(rowCount, rs.getDate(i).getTime());
                        dateMilliVector.setValueCount(rowCount + 1);
                        break;
                    case Types.TIME:
                        TimeMilliVector timeMilliVector = (TimeMilliVector)root.getVector(columnName);
                        timeMilliVector.setSafe(rowCount, (int)rs.getTime(i).getTime());  // TODO - down conversion cast??
                        timeMilliVector.setValueCount(rowCount + 1);
                        break;
                    case Types.TIMESTAMP:
                        // timezone is null
                        TimeStampVector timeStampVector = (TimeStampVector)root.getVector(columnName);
                        timeStampVector.setSafe(rowCount, rs.getTimestamp(i).getTime());
                        timeStampVector.setValueCount(rowCount + 1);
                        break;
                    case Types.BINARY:
                    case Types.VARBINARY:
                    case Types.LONGVARBINARY:
                        // TODO - How to handle the buffer size if it starts exceeding the iniital allocated buffer
                        VarBinaryVector varBinaryVector = (VarBinaryVector)root.getVector(columnName);;
                        byte[] bytes = rs.getBytes(i);
                        varBinaryVector.setIndexDefined(i);
                        varBinaryVector.setValueLengthSafe(i, bytes.length);
                        varBinaryVector.setSafe(rowCount, bytes);
                        varBinaryVector.setValueCount(rowCount + 1);
                        break;
                    case Types.ARRAY:
                        // not handled
//                    fields.add(new Field("list", FieldType.nullable(new ArrowType.List()), null));
                        break;
                    case Types.CLOB:
                        // TODO - How to handle the buffer size if it starts exceeding the iniital allocated buffer
                        VarCharVector varcharVector1 = (VarCharVector)root.getVector(columnName);
                        Clob clob = rs.getClob(i);
                        int length = (int)clob.length();
                        varcharVector1.setIndexDefined(i);
                        varcharVector1.setValueLengthSafe(i, length);
                        varcharVector1.setSafe(varcharVector1.getValueCapacity(), clob.getSubString(1, length).getBytes(), 0, length);
                        varcharVector1.setValueCount(rowCount + 1);
                        break;
                    case Types.BLOB:
                        // TODO - How to handle the buffer size if it starts exceeding the iniital allocated buffer
                        VarBinaryVector varBinaryVector1 = (VarBinaryVector)root.getVector(columnName);;
                        Blob blob = rs.getBlob(i);
                        byte[] data = blob.getBytes(0, (int)blob.length());
                        varBinaryVector1.setIndexDefined(i);
                        varBinaryVector1.setValueLengthSafe(i, (int)blob.length());
                        varBinaryVector1.setSafe(i, data);
                        varBinaryVector1.setValueCount(rowCount + 1);
                        break;

                    default:
                        // no-op
                        break;
                }
            }
            rowCount++;
            if (rowCount == size) {
                break;
            }
        }
        root.setRowCount(rowCount);
    }




}
