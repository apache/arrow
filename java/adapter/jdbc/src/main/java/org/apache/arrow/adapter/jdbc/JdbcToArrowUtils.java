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


import com.google.common.base.Preconditions;
import com.google.common.io.ByteStreams;
import com.google.common.io.CharStreams;

import org.apache.arrow.vector.BaseFixedWidthVector;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import static org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE;
import static org.apache.arrow.vector.types.FloatingPointPrecision.SINGLE;

/**
 * Class that does most of the work to convert JDBC ResultSet data into Arrow columnar format Vector objects.
 *
 * @since 0.10.0
 */
public class JdbcToArrowUtils {

    private static final int DEFAULT_BUFFER_SIZE = 256;

    /**
     * Create Arrow {@link Schema} object for the given JDBC {@link ResultSetMetaData}.
     *
     * This method currently performs following type mapping for JDBC SQL data types to corresponding Arrow data types.
     *
     * CHAR	--> ArrowType.Utf8
     * NCHAR	--> ArrowType.Utf8
     * VARCHAR --> ArrowType.Utf8
     * NVARCHAR --> ArrowType.Utf8
     * LONGVARCHAR --> ArrowType.Utf8
     * LONGNVARCHAR --> ArrowType.Utf8
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

        Preconditions.checkNotNull(rsmd, "JDBC ResultSetMetaData object can't be null");

        List<Field> fields = new ArrayList<>();
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
                case Types.NCHAR:
                case Types.VARCHAR:
                case Types.NVARCHAR:
                case Types.LONGVARCHAR:
                case Types.LONGNVARCHAR:
                    fields.add(new Field(columnName, FieldType.nullable(new ArrowType.Utf8()), null));
                    break;
                case Types.DATE:
                    fields.add(new Field(columnName, FieldType.nullable(new ArrowType.Date(DateUnit.MILLISECOND)), null));
                    break;
                case Types.TIME:
                    fields.add(new Field(columnName, FieldType.nullable(new ArrowType.Time(TimeUnit.MILLISECOND, 32)), null));
                    break;
                case Types.TIMESTAMP:
                    // TODO Need to handle timezone
                    fields.add(new Field(columnName, FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MILLISECOND, null)), null));
                    break;
                case Types.BINARY:
                case Types.VARBINARY:
                case Types.LONGVARBINARY:
                    fields.add(new Field(columnName, FieldType.nullable(new ArrowType.Binary()), null));
                    break;
                case Types.ARRAY:
                    // TODO Need to handle this type
                	// fields.add(new Field("list", FieldType.nullable(new ArrowType.List()), null));
                   break;
                case Types.CLOB:
                    fields.add(new Field(columnName, FieldType.nullable(new ArrowType.Utf8()), null));
                    break;
                case Types.BLOB:
                    fields.add(new Field(columnName, FieldType.nullable(new ArrowType.Binary()), null));
                    break;

                default:
                    // no-op, shouldn't get here
                    break;
            }
        }

        return new Schema(fields, null);
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
     * the given Arrow Vector objects.
     *
     * @param rs ResultSet to use to fetch the data from underlying database
     * @param root Arrow {@link VectorSchemaRoot} object to populate
     * @throws Exception
     */
    public static void jdbcToArrowVectors(ResultSet rs, VectorSchemaRoot root) throws SQLException, IOException {

        Preconditions.checkNotNull(rs, "JDBC ResultSet object can't be null");
        Preconditions.checkNotNull(root, "JDBC ResultSet object can't be null");

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
                        updateVector((BitVector)root.getVector(columnName),
                                rs.getBoolean(i), rowCount);
                        break;
                    case Types.TINYINT:
                        updateVector((TinyIntVector)root.getVector(columnName),
                                rs.getInt(i), rowCount);
                        break;
                    case Types.SMALLINT:
                        updateVector((SmallIntVector)root.getVector(columnName),
                                rs.getInt(i), rowCount);
                        break;
                    case Types.INTEGER:
                        updateVector((IntVector)root.getVector(columnName),
                                rs.getInt(i), rowCount);
                        break;
                    case Types.BIGINT:
                        updateVector((BigIntVector)root.getVector(columnName),
                                rs.getLong(i), rowCount);
                        break;
                    case Types.NUMERIC:
                    case Types.DECIMAL:
                        updateVector((DecimalVector)root.getVector(columnName),
                                rs.getBigDecimal(i), rowCount);
                        break;
                    case Types.REAL:
                    case Types.FLOAT:
                        updateVector((Float4Vector)root.getVector(columnName),
                                rs.getFloat(i), rowCount);
                        break;
                    case Types.DOUBLE:
                        updateVector((Float8Vector)root.getVector(columnName),
                                rs.getDouble(i), rowCount);
                        break;
                    case Types.CHAR:
                    case Types.NCHAR:
                    case Types.VARCHAR:
                    case Types.NVARCHAR:
                    case Types.LONGVARCHAR:
                    case Types.LONGNVARCHAR:
                        updateVector((VarCharVector)root.getVector(columnName),
                                rs.getString(i), rowCount);
                        break;
                    case Types.DATE:
                        updateVector((DateMilliVector) root.getVector(columnName),
                                rs.getDate(i), rowCount);
                        break;
                    case Types.TIME:
                        updateVector((TimeMilliVector) root.getVector(columnName),
                                rs.getTime(i), rowCount);
                        break;
                    case Types.TIMESTAMP:
                        updateVector((TimeStampVector)root.getVector(columnName),
                                rs.getTimestamp(i), rowCount);
                        break;
                    case Types.BINARY:
                    case Types.VARBINARY:
                    case Types.LONGVARBINARY:
                        updateVector((VarBinaryVector)root.getVector(columnName),
                                rs.getBytes(i), rowCount);
                        break;
                    case Types.ARRAY:
                        // TODO Need to handle this type
                    	// fields.add(new Field("list", FieldType.nullable(new ArrowType.List()), null));
                        break;
                    case Types.CLOB:
                        updateVector((VarCharVector)root.getVector(columnName), 
                        		rs.getCharacterStream(i), rowCount);
                        break;
                    case Types.BLOB:
                    	updateVector((VarBinaryVector)root.getVector(columnName),
                    			rs.getBinaryStream(i), rowCount);
                        break;

                    default:
                        // no-op, shouldn't get here
                        break;
                }
            }
            rowCount++;
        }
        root.setRowCount(rowCount);
    }

    private static void updateVector(BitVector bitVector, boolean value, int rowCount) {
        bitVector.setSafe(rowCount, value? 1: 0);
        bitVector.setValueCount(rowCount + 1);
    }

    private static void updateVector(TinyIntVector tinyIntVector, int value, int rowCount) {
        tinyIntVector.setSafe(rowCount, value);
        tinyIntVector.setValueCount(rowCount + 1);
    }

    private static  void updateVector(SmallIntVector smallIntVector, int value, int rowCount) {
        smallIntVector.setSafe(rowCount, value);
        smallIntVector.setValueCount(rowCount + 1);
    }

    private static  void updateVector(IntVector intVector, int value, int rowCount) {
        intVector.setSafe(rowCount, value);
        intVector.setValueCount(rowCount + 1);
    }

    private static  void updateVector(BigIntVector bigIntVector, long value, int rowCount) {
        bigIntVector.setSafe(rowCount, value);
        bigIntVector.setValueCount(rowCount + 1);
    }

    private static void updateVector(DecimalVector decimalVector, BigDecimal value, int rowCount) {
        decimalVector.setSafe(rowCount, value);
        decimalVector.setValueCount(rowCount + 1);
    }

    private static void updateVector(Float4Vector float4Vector, float value, int rowCount) {
        float4Vector.setSafe(rowCount, value);
        float4Vector.setValueCount(rowCount + 1);
    }

    private static void updateVector(Float8Vector float8Vector, double value, int rowCount) {
        float8Vector.setSafe(rowCount, value);
        float8Vector.setValueCount(rowCount + 1);
    }

    private static void updateVector(VarCharVector varcharVector, String value, int rowCount) {
        if (value != null) {
            varcharVector.setIndexDefined(rowCount);
            varcharVector.setValueLengthSafe(rowCount, value.length());
            varcharVector.setSafe(rowCount, value.getBytes(StandardCharsets.UTF_8), 0, value.length());
            varcharVector.setValueCount(rowCount + 1);
        }
        // TODO: not sure how to handle null string value ???
    }

    private static void updateVector(DateMilliVector dateMilliVector, Date date, int rowCount) {
        //TODO: Need to handle Timezone
        dateMilliVector.setValueCount(rowCount + 1);
        if (date != null) {
            dateMilliVector.setSafe(rowCount, date.getTime());
        } else {
            dateMilliVector.setNull(rowCount);
        }
    }

    private static void updateVector(TimeMilliVector timeMilliVector, Time time, int rowCount) {
        timeMilliVector.setValueCount(rowCount + 1);
        if (time != null) {
            timeMilliVector.setSafe(rowCount, (int) time.getTime());
        } else {
            timeMilliVector.setNull(rowCount);
        }
    }

    private static void updateVector(TimeStampVector timeStampVector, Timestamp timestamp, int rowCount) {
        //TODO Need to handle timezone ???
        timeStampVector.setValueCount(rowCount + 1);
        if (timestamp != null) {
            timeStampVector.setSafe(rowCount, timestamp.getTime());
        } else {
            timeStampVector.setNull(rowCount);
        }
    }

    private static void updateVector(VarBinaryVector varBinaryVector, byte[] bytes, int rowCount) {
        varBinaryVector.setValueCount(rowCount + 1);
        if (bytes != null) {
            varBinaryVector.setIndexDefined(rowCount);
            varBinaryVector.setValueLengthSafe(rowCount, bytes.length);
            varBinaryVector.setSafe(rowCount, bytes);
        } else {
            varBinaryVector.setNull(rowCount);
        }
    }
    
    private static void updateVector(VarCharVector varcharVector, Reader clobReader, int rowCount) throws SQLException, IOException {
    	Preconditions.checkNotNull(clobReader, "Reader object for Clob can't be null");
       	
        ByteBuffer clobBuff = ByteBuffer.wrap(CharStreams.toString(clobReader).getBytes());
        clobReader.close();
        varcharVector.setValueCount(rowCount + 1);
        if (clobBuff != null) {
            int length = clobBuff.capacity();
            varcharVector.setIndexDefined(rowCount);
            varcharVector.setValueLengthSafe(rowCount, length);
            varcharVector.setSafe(rowCount, clobBuff, 0, length);
        } else {
            varcharVector.setNull(rowCount);
        }
    }

    private static void updateVector(VarBinaryVector varBinaryVector, InputStream blobStream, int rowCount) throws SQLException, IOException {
    	Preconditions.checkNotNull(blobStream, "InputStream object for Blob can't be null");
               
        ByteBuffer blobBuff = ByteBuffer.wrap(ByteStreams.toByteArray(blobStream));
        blobStream.close();
            	
    	varBinaryVector.setValueCount(rowCount + 1);
        if (blobBuff != null) {
        	int length = blobBuff.capacity();
            varBinaryVector.setIndexDefined(rowCount);
            varBinaryVector.setValueLengthSafe(rowCount, length);
            varBinaryVector.setSafe(rowCount, blobBuff, 0, length);
        } else {
            varBinaryVector.setNull(rowCount);
        }
    }
    
}
