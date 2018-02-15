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
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;

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
        ImmutableList.Builder<Field> fields = ImmutableList.builder();
        int columnCount = rsmd.getColumnCount();
        for (int i = 1; i <= columnCount; i++) {
            switch (rsmd.getColumnType(i)) {
                case Types.ARRAY:
                    // no-op as of now
                    break;
                case Types.BIGINT:
                    fields.add(new Field("long", FieldType.nullable(new ArrowType.Int(64, true)), null));
                    break;
                case Types.BINARY: case Types.LONGVARBINARY: case Types.VARBINARY:
                    break;
                case Types.BIT:
                    break;
                case Types.BLOB:
                    fields.add(new Field("binary", FieldType.nullable(new ArrowType.Binary()), null));
                    break;
                case Types.BOOLEAN:
                    break;
                case Types.CHAR:
                    break;
                case Types.CLOB:
                    break;
                case Types.DATE:
                    break;
                case Types.DECIMAL: case Types.NUMERIC:
                    break;
                case Types.DOUBLE: case Types.FLOAT:
                    fields.add(new Field("double", FieldType.nullable(new ArrowType.FloatingPoint(SINGLE)), null));
                    break;
                case Types.INTEGER:
                    fields.add(new Field("int", FieldType.nullable(new ArrowType.Int(32, true)), null));
                    break;
                case Types.LONGVARCHAR: case Types.LONGNVARCHAR: case Types.VARCHAR: case Types.NVARCHAR:
                    break;
                case Types.REAL:
                    break;
                case Types.SMALLINT:
                    break;
                case Types.TIME:
                    break;
                case Types.TIMESTAMP:
                    break;
                case Types.TINYINT:
                    break;
                default:
//                    throw new TypeConversionNotSupportedException();
                    // no-op

            }
        }

        return new Schema(fields.build(), null);
    }


}
