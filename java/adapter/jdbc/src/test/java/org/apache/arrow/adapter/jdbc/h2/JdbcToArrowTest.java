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

package org.apache.arrow.adapter.jdbc.h2;

import org.apache.arrow.adapter.jdbc.AbstractJdbcToArrowTest;
import org.apache.arrow.adapter.jdbc.JdbcToArrow;
import org.apache.arrow.adapter.jdbc.Table;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.After;
import org.junit.Test;

import java.math.BigDecimal;

import static org.apache.arrow.adapter.jdbc.JdbcToArrowTestHelper.*;

/**
 *
 */
public class JdbcToArrowTest extends AbstractJdbcToArrowTest {


    @Test
    public void sqlToArrowTestInt() throws Exception {

        Table table =
                mapper.readValue(
                        this.getClass().getClassLoader().getResourceAsStream("h2/test1_int_h2.yml"),
                        Table.class);

        try {
            createTestData(table);

            VectorSchemaRoot root = JdbcToArrow.sqlToArrow(conn, table.getQuery());

            int[] values = {
                    101, 101, 101, 101, 101, 101, 101, 101, 101, 101, 101, 101, 101, 101, 101,
            };
            assertIntVectorValues(root.getVector("INT_FIELD1"), 15, values);

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            deleteTestData(table);
        }

    }

    @Test
    public void sqlToArrowTestBool() throws Exception {

        Table table =
                mapper.readValue(
                        this.getClass().getClassLoader().getResourceAsStream("h2/test1_bool_h2.yml"),
                        Table.class);

        try {
            createTestData(table);

            VectorSchemaRoot root = JdbcToArrow.sqlToArrow(conn, table.getQuery());

            int[] bools = {
                    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1
            };
            assertBitBooleanVectorValues(root.getVector("BOOL_FIELD2"), 15, bools);

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            deleteTestData(table);
        }

    }

    @Test
    public void sqlToArrowTestTinyInt() throws Exception {

        Table table =
                mapper.readValue(
                        this.getClass().getClassLoader().getResourceAsStream("h2/test1_tinyint_h2.yml"),
                        Table.class);

        try {
            createTestData(table);

            VectorSchemaRoot root = JdbcToArrow.sqlToArrow(conn, table.getQuery());

            int[] tinyints = {
                    45, 45, 45, 45, 45, 45, 45, 45, 45, 45, 45, 45, 45, 45, 45
            };
            assertTinyIntVectorValues(root.getVector("TINYINT_FIELD3"), 15, tinyints);

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            deleteTestData(table);
        }

    }

    @Test
    public void sqlToArrowTestSmallInt() throws Exception {

        Table table =
                mapper.readValue(
                        this.getClass().getClassLoader().getResourceAsStream("h2/test1_smallint_h2.yml"),
                        Table.class);

        try {
            createTestData(table);

            VectorSchemaRoot root = JdbcToArrow.sqlToArrow(conn, table.getQuery());

            int[] smallints = {
                    12000, 12000, 12000, 12000, 12000, 12000, 12000, 12000, 12000, 12000, 12000, 12000, 12000, 12000, 12000
            };
            assertSmallIntVectorValues(root.getVector("SMALLINT_FIELD4"), 15, smallints);

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            deleteTestData(table);
        }

    }

    @Test
    public void sqlToArrowTestBigInt() throws Exception {

        Table table =
                mapper.readValue(
                        this.getClass().getClassLoader().getResourceAsStream("h2/test1_bigint_h2.yml"),
                        Table.class);

        try {
            createTestData(table);

            VectorSchemaRoot root = JdbcToArrow.sqlToArrow(conn, table.getQuery());

            int[] bigints = {
                    92233720, 92233720, 92233720, 92233720, 92233720, 92233720, 92233720, 92233720, 92233720,
                    92233720, 92233720, 92233720, 92233720, 92233720, 92233720
            };
            assertBigIntVectorValues(root.getVector("BIGINT_FIELD5"), 15, bigints);

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            deleteTestData(table);
        }

    }

    @Test
    public void sqlToArrowTestAllDataTypes() throws Exception {

        Table table =
                mapper.readValue(
                        this.getClass().getClassLoader().getResourceAsStream("h2/test1_all_datatypes_h2.yml"),
                        Table.class);

        try {
            createTestData(table);

            VectorSchemaRoot root = JdbcToArrow.sqlToArrow(conn, table.getQuery());

            int[] ints = {
                    101, 101, 101, 101, 101, 101, 101, 101, 101, 101, 101, 101, 101, 101, 101
            };
            assertIntVectorValues(root.getVector("INT_FIELD1"), 15, ints);

            int[] bools = {
                    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1
            };
            assertBitBooleanVectorValues(root.getVector("BOOL_FIELD2"), 15, bools);

            int[] tinyints = {
                    45, 45, 45, 45, 45, 45, 45, 45, 45, 45, 45, 45, 45, 45, 45
            };
            assertTinyIntVectorValues(root.getVector("TINYINT_FIELD3"), 15, tinyints);

            int[] smallints = {
                    12000, 12000, 12000, 12000, 12000, 12000, 12000, 12000, 12000, 12000, 12000, 12000, 12000, 12000, 12000
            };
            assertSmallIntVectorValues(root.getVector("SMALLINT_FIELD4"), 15, smallints);

            int[] bigints = {
                    92233720, 92233720, 92233720, 92233720, 92233720, 92233720, 92233720, 92233720, 92233720,
                    92233720, 92233720, 92233720, 92233720, 92233720, 92233720
            };
            assertBigIntVectorValues(root.getVector("BIGINT_FIELD5"), 15, bigints);

            BigDecimal[] bigdecimals = {
                    new BigDecimal(17345667789.23), new BigDecimal(17345667789.23), new BigDecimal(17345667789.23),
                    new BigDecimal(17345667789.23), new BigDecimal(17345667789.23), new BigDecimal(17345667789.23),
                    new BigDecimal(17345667789.23), new BigDecimal(17345667789.23), new BigDecimal(17345667789.23),
                    new BigDecimal(17345667789.23), new BigDecimal(17345667789.23), new BigDecimal(17345667789.23),
                    new BigDecimal(17345667789.23), new BigDecimal(17345667789.23), new BigDecimal(17345667789.23)
            };
            assertDecimalVectorValues(root.getVector("DECIMAL_FIELD6"), 15, bigdecimals);

            double[] doubles = {
                    56478356785.345, 56478356785.345, 56478356785.345, 56478356785.345, 56478356785.345, 56478356785.345,
                    56478356785.345, 56478356785.345, 56478356785.345,
                    56478356785.345, 56478356785.345, 56478356785.345, 56478356785.345, 56478356785.345, 56478356785.345
            };
            assertFloat8VectorValues(root.getVector("DOUBLE_FIELD7"), 15, doubles);

            float[] reals = {
                    56478356785.345f, 56478356785.345f, 56478356785.345f, 56478356785.345f, 56478356785.345f, 56478356785.345f,
                    56478356785.345f, 56478356785.345f, 56478356785.345f,
                    56478356785.345f, 56478356785.345f, 56478356785.345f, 56478356785.345f, 56478356785.345f, 56478356785.345f
            };
            assertFloat4VectorValues(root.getVector("REAL_FIELD8"), 15, reals);

            long[] times = {
                    45935000, 45935000, 45935000, 45935000, 45935000, 45935000, 45935000, 45935000,
                    45935000, 45935000, 45935000, 45935000, 45935000, 45935000, 45935000
            };
            assertTimeVectorValues(root.getVector("TIME_FIELD9"), 15, times);

            long[] dates = {
                    1518336000000l, 1518336000000l, 1518336000000l, 1518336000000l, 1518336000000l, 1518336000000l, 1518336000000l, 1518336000000l,
                    1518336000000l, 1518336000000l, 1518336000000l, 1518336000000l, 1518336000000l, 1518336000000l, 1518336000000l
            };
            assertDateVectorValues(root.getVector("DATE_FIELD10"), 15, dates);

            long[] timestamps = {
                    1518439535000l, 1518439535000l, 1518439535000l, 1518439535000l, 1518439535000l, 1518439535000l, 1518439535000l, 1518439535000l,
                    1518439535000l, 1518439535000l, 1518439535000l, 1518439535000l, 1518439535000l, 1518439535000l, 1518439535000l
            };
            assertTimeStampVectorValues(root.getVector("TIMESTAMP_FIELD11"), 15, timestamps);

            byte[][] bytes = {
                    hexStringToByteArray("736f6d6520746578742074686174206e6565647320746f20626520636f6e76657274656420746f2062696e617279"),
                    hexStringToByteArray("736f6d6520746578742074686174206e6565647320746f20626520636f6e76657274656420746f2062696e617279"),
                    hexStringToByteArray("736f6d6520746578742074686174206e6565647320746f20626520636f6e76657274656420746f2062696e617279"),
                    hexStringToByteArray("736f6d6520746578742074686174206e6565647320746f20626520636f6e76657274656420746f2062696e617279"),
                    hexStringToByteArray("736f6d6520746578742074686174206e6565647320746f20626520636f6e76657274656420746f2062696e617279"),
                    hexStringToByteArray("736f6d6520746578742074686174206e6565647320746f20626520636f6e76657274656420746f2062696e617279"),
                    hexStringToByteArray("736f6d6520746578742074686174206e6565647320746f20626520636f6e76657274656420746f2062696e617279"),
                    hexStringToByteArray("736f6d6520746578742074686174206e6565647320746f20626520636f6e76657274656420746f2062696e617279"),
                    hexStringToByteArray("736f6d6520746578742074686174206e6565647320746f20626520636f6e76657274656420746f2062696e617279"),
                    hexStringToByteArray("736f6d6520746578742074686174206e6565647320746f20626520636f6e76657274656420746f2062696e617279"),
                    hexStringToByteArray("736f6d6520746578742074686174206e6565647320746f20626520636f6e76657274656420746f2062696e617279"),
                    hexStringToByteArray("736f6d6520746578742074686174206e6565647320746f20626520636f6e76657274656420746f2062696e617279"),
                    hexStringToByteArray("736f6d6520746578742074686174206e6565647320746f20626520636f6e76657274656420746f2062696e617279"),
                    hexStringToByteArray("736f6d6520746578742074686174206e6565647320746f20626520636f6e76657274656420746f2062696e617279"),
                    hexStringToByteArray("736f6d6520746578742074686174206e6565647320746f20626520636f6e76657274656420746f2062696e617279")
            };
            assertVarBinaryVectorValues(root.getVector("BINARY_FIELD12"), 15, bytes);

            byte[] strb = "some text that needs to be converted to varchar".getBytes();
            byte[][] varchars = {
                strb, strb, strb, strb, strb, strb, strb, strb, strb, strb, strb, strb, strb, strb, strb
            };
            assertVarcharVectorValues(root.getVector("VARCHAR_FIELD13"), 15, varchars);

            assertVarBinaryVectorValues(root.getVector("BLOB_FIELD14"), 15, bytes);

            strb = "some text that needs to be converted to clob".getBytes();
            varchars = new byte[][] {
                    strb, strb, strb, strb, strb, strb, strb, strb, strb, strb, strb, strb, strb, strb, strb
            };
            assertVarcharVectorValues(root.getVector("CLOB_FIELD15"), 15, varchars);

            strb = "some char text".getBytes();
            varchars = new byte[][] {
                    strb, strb, strb, strb, strb, strb, strb, strb, strb, strb, strb, strb, strb, strb, strb
            };
            assertVarcharVectorValues(root.getVector("CHAR_FIELD16"), 15, varchars);

            assertBitBooleanVectorValues(root.getVector("BIT_FIELD17"), 15, bools);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            deleteTestData(table);
        }

    }

    @After
    public void destroy() throws Exception {
        if (conn != null) {
            conn.close();
        }
    }

}
