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

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;

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

    public static void jdbcToArrowVectors(ResultSet rs, VectorSchemaRoot root) throws Exception {

        assert rs != null;
        assert root != null;

        ResultSetMetaData rsmd = rs.getMetaData();
        int columnCount = rsmd.getColumnCount();

        int rowCount = 0;
        while (rs.next()) {
            // for each column get the value based on the type

            for (int i = 1; i < columnCount; i++) {
                String columnName = rsmd.getColumnName(i);
                switch (rsmd.getColumnType(i)) {
                    case Types.BOOLEAN:
                    case Types.BIT:
                        BitVector bitVector = (BitVector) root.getVector(columnName);
                        bitVector.setInitialCapacity(1);
                        bitVector.allocateNew();
                        bitVector.setSafe(rowCount,rs.getBoolean(i)? 1: 0);
                        bitVector.setValueCount(1);
                        break;
                    case Types.TINYINT:
                        TinyIntVector tinyIntVector = (TinyIntVector)root.getVector(columnName);
                        tinyIntVector.setInitialCapacity(1);
                        tinyIntVector.allocateNew();
                        tinyIntVector.setSafe(rowCount, rs.getInt(i));
                        tinyIntVector.setValueCount(1);
                        break;
                    case Types.SMALLINT:
                        SmallIntVector smallIntVector = (SmallIntVector)root.getVector(columnName);
                        smallIntVector.setInitialCapacity(1);
                        smallIntVector.allocateNew();
                        smallIntVector.setSafe(rowCount, rs.getInt(i));
                        smallIntVector.setValueCount(1);
                        break;
                    case Types.INTEGER:
                        IntVector intVector = (IntVector)root.getVector(columnName);
                        intVector.setInitialCapacity(1);
                        intVector.allocateNew();
                        intVector.setSafe(rowCount, rs.getInt(i));
                        intVector.setValueCount(1);
                        break;
                    case Types.BIGINT:
                        BigIntVector bigIntVector = (BigIntVector)root.getVector(columnName);
                        bigIntVector.setInitialCapacity(1);
                        bigIntVector.allocateNew();
                        bigIntVector.setSafe(rowCount, rs.getInt(i));
                        bigIntVector.setValueCount(1);
                        break;
                    case Types.NUMERIC:
                    case Types.DECIMAL:
                        DecimalVector decimalVector = (DecimalVector)root.getVector(columnName);
                        decimalVector.setInitialCapacity(1);
                        decimalVector.allocateNew();
                        decimalVector.setSafe(rowCount, rs.getBigDecimal(i));
                        decimalVector.setValueCount(1);
                        break;
                    case Types.REAL:
                    case Types.FLOAT:
                        Float4Vector float4Vector = (Float4Vector)root.getVector(columnName);
                        float4Vector.setInitialCapacity(1);
                        float4Vector.allocateNew();
                        float4Vector.setSafe(rowCount, rs.getFloat(i));
                        float4Vector.setValueCount(1);
                        break;
                    case Types.DOUBLE:
                        Float8Vector float8Vector = (Float8Vector)root.getVector(columnName);
                        float8Vector.setInitialCapacity(1);
                        float8Vector.allocateNew();
                        float8Vector.setSafe(rowCount, rs.getDouble(i));
                        float8Vector.setValueCount(1);
                        break;
                    case Types.CHAR:
                    case Types.VARCHAR:
                    case Types.LONGVARCHAR:
                        VarCharVector varcharVector = (VarCharVector)root.getVector(columnName);
                        varcharVector.setInitialCapacity(1);
                        varcharVector.allocateNew();
                        String value = rs.getString(i);
                        varcharVector.setIndexDefined(i);
                        varcharVector.setValueLengthSafe(i, value.length());
                        varcharVector.setSafe(varcharVector.getValueCapacity(), value.getBytes(), 0, value.length());
                        varcharVector.setValueCount(1);
                        break;
                    case Types.DATE:
                        DateMilliVector dateMilliVector = (DateMilliVector)root.getVector(columnName);
                        dateMilliVector.setInitialCapacity(1);
                        dateMilliVector.allocateNew();
                        dateMilliVector.setSafe(rowCount, rs.getDate(i).getTime());
                        dateMilliVector.setValueCount(1);
                        break;
                    case Types.TIME:
                        TimeMilliVector timeMilliVector = (TimeMilliVector)root.getVector(columnName);
                        timeMilliVector.setInitialCapacity(1);
                        timeMilliVector.allocateNew();
                        timeMilliVector.setSafe(rowCount, (int)rs.getTime(i).getTime());  // TODO - down conversion cast??
                        timeMilliVector.setValueCount(1);
                        break;
                    case Types.TIMESTAMP:
                        // timezone is null
                        TimeStampVector timeStampVector = (TimeStampVector)root.getVector(columnName);
                        timeStampVector.setInitialCapacity(1);
                        timeStampVector.allocateNew();
                        timeStampVector.setSafe(rowCount, rs.getTimestamp(i).getTime());
                        timeStampVector.setValueCount(1);
                        break;
                    case Types.BINARY:
                    case Types.VARBINARY:
                    case Types.LONGVARBINARY:
//                        fields.add(new Field(columnName, FieldType.nullable(new ArrowType.Binary()), null));
                        break;
                    case Types.ARRAY:
                        // not handled
//                    fields.add(new Field("list", FieldType.nullable(new ArrowType.List()), null));
                        break;
                    case Types.CLOB:
                        break;
                    case Types.BLOB:
//                        fields.add(new Field(columnName, FieldType.nullable(new ArrowType.Binary()), null));
                        break;

                    default:
                        // no-op
                        break;
                }
                rowCount++;

            }
        }
    }


}
