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

import java.nio.charset.Charset;
import java.sql.*;
import java.util.List;

import static org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE;
import static org.apache.arrow.vector.types.FloatingPointPrecision.SINGLE;


/**
 *
 */
public class JdbcToArrowUtils {

    private static final int DEFAULT_BUFFER_SIZE = 256;

    /**
     * Create Arrow {@link Schema} object for the given JDBC {@link ResultSetMetaData}.
     *
     * This method currently performs following type mapping for JDBC SQL data types to corresponding Arrow data types.
     *
     * CHAR	--> ArrowType.Utf8
     * VARCHAR --> ArrowType.Utf8
     * LONGVARCHAR --> ArrowType.Utf8
     * NUMERIC --> ArrowType.Decimal(precision, scale)
     * DECIMAL --> ArrowType.Decimal(precision, scale)
     * BIT --> ArrowType.Bool
     * TINYINT --> ArrowType.Int(8, signed)
     * SMALLINT --> ArrowType.Int(16, signed)
     * INTEGER --> ArrowType.Int(32, signed)
     * BIGINT --> ArrowType.Int(64, signed)
     * REAL --> ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)
     * FLOAT --> ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)
     * DOUBLE --> ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)
     * BINARY --> ArrowType.Binary
     * VARBINARY --> ArrowType.Binary
     * LONGVARBINARY --> ArrowType.Binary
     * DATE --> ArrowType.Date(DateUnit.MILLISECOND)
     * TIME --> ArrowType.Time(TimeUnit.MILLISECOND, 32)
     * TIMESTAMP --> ArrowType.Timestamp(TimeUnit.MILLISECOND, timezone=null)
     * CLOB --> ArrowType.Utf8
     * BLOB --> ArrowType.Binary
     *
     * @param rsmd
     * @return {@link Schema}
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

    private static void allocateVectors(VectorSchemaRoot root, int size) {
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

    /**
     * Iterate the given JDBC {@link ResultSet} object to fetch the data and transpose it to populate
     * the Arrow Vector obejcts.
     *
     * @param rs ResultSet to use to fetch the data from underlying database
     * @param root Arrow {@link VectorSchemaRoot} object to populate
     * @throws Exception
     */
    public static void jdbcToArrowVectors(ResultSet rs, VectorSchemaRoot root) throws Exception {

        assert rs != null;
        assert root != null;

        ResultSetMetaData rsmd = rs.getMetaData();
        int columnCount = rsmd.getColumnCount();

        allocateVectors(root, DEFAULT_BUFFER_SIZE);

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
                        VarCharVector varcharVector = (VarCharVector)root.getVector(columnName);
                        String value = rs.getString(i) != null ? rs.getString(i) : "";
                        varcharVector.setIndexDefined(rowCount);
                        varcharVector.setValueLengthSafe(rowCount, value.length());
                        varcharVector.setSafe(rowCount, value.getBytes(Charset.forName("UTF-8")), 0, value.length());
                        varcharVector.setValueCount(rowCount + 1);
                        break;
                    case Types.DATE:
                        Date date = rs.getDate(i);
                        DateMilliVector dateMilliVector = (DateMilliVector) root.getVector(columnName);
                        dateMilliVector.setValueCount(rowCount + 1);
                        if (date != null) {
                            dateMilliVector.setSafe(rowCount, rs.getDate(i).getTime());
                        } else {
                            dateMilliVector.setNull(rowCount);
                        }
                        break;
                    case Types.TIME:
                        Time time = rs.getTime(i);
                        TimeMilliVector timeMilliVector = (TimeMilliVector)root.getVector(columnName);
                        timeMilliVector.setValueCount(rowCount + 1);
                        if (time != null) {
                            timeMilliVector.setSafe(rowCount, (int) rs.getTime(i).getTime());
                        } else {
                            timeMilliVector.setNull(rowCount);
                        }

                        break;
                    case Types.TIMESTAMP:
                        // timezone is null
                        Timestamp timestamp = rs.getTimestamp(i);
                        TimeStampVector timeStampVector = (TimeStampVector)root.getVector(columnName);
                        timeStampVector.setValueCount(rowCount + 1);
                        if (timestamp != null) {
                            timeStampVector.setSafe(rowCount, timestamp.getTime());
                        } else {
                            timeStampVector.setNull(rowCount);
                        }
                        break;
                    case Types.BINARY:
                    case Types.VARBINARY:
                    case Types.LONGVARBINARY:
                        VarBinaryVector varBinaryVector = (VarBinaryVector)root.getVector(columnName);;
                        varBinaryVector.setValueCount(rowCount + 1);
                        byte[] bytes = rs.getBytes(i);
                        if (bytes != null) {
                            varBinaryVector.setIndexDefined(rowCount);
                            varBinaryVector.setValueLengthSafe(rowCount, bytes.length);
                            varBinaryVector.setSafe(rowCount, bytes);
                        } else {
                            varBinaryVector.setNull(rowCount);
                        }
                        break;
                    case Types.ARRAY:
                        // not handled
//                    fields.add(new Field("list", FieldType.nullable(new ArrowType.List()), null));
                        break;
                    case Types.CLOB:
                        VarCharVector varcharVector1 = (VarCharVector)root.getVector(columnName);
                        varcharVector1.setValueCount(rowCount + 1);
                        Clob clob = rs.getClob(i);
                        if (clob != null) {
                            int length = (int) clob.length();
                            varcharVector1.setIndexDefined(rowCount);
                            varcharVector1.setValueLengthSafe(rowCount, length);
                            varcharVector1.setSafe(rowCount, clob.getSubString(1, length).getBytes(), 0, length);
                        } else {
                            varcharVector1.setNull(rowCount);
                        }
                        break;
                    case Types.BLOB:
                        VarBinaryVector varBinaryVector1 = (VarBinaryVector)root.getVector(columnName);;
                        varBinaryVector1.setValueCount(rowCount + 1);
                        Blob blob = rs.getBlob(i);
                        if (blob != null) {
                            byte[] data = blob.getBytes(0, (int) blob.length());
                            varBinaryVector1.setIndexDefined(rowCount);
                            varBinaryVector1.setValueLengthSafe(rowCount, (int) blob.length());
                            varBinaryVector1.setSafe(rowCount, data);
                        } else {
                            varBinaryVector1.setNull(rowCount);}

                        break;

                    default:
                        // no-op
                        break;
                }
            }
            rowCount++;
        }
        root.setRowCount(rowCount);
    }
}
