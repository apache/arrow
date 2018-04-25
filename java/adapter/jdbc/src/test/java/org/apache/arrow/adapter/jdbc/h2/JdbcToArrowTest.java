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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.arrow.adapter.jdbc.AbstractJdbcToArrowTest;
import org.apache.arrow.adapter.jdbc.JdbcToArrow;
import org.apache.arrow.adapter.jdbc.Table;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;

import static org.apache.arrow.adapter.jdbc.JdbcToArrowTestHelper.*;

/**
 *
 */
public class JdbcToArrowTest extends AbstractJdbcToArrowTest {

    private Connection conn = null;
    private ObjectMapper mapper = null;


    @Before
    public void setUp() throws Exception {
        String url = "jdbc:h2:mem:JdbcToArrowTest";
        String driver = "org.h2.Driver";

        mapper = new ObjectMapper(new YAMLFactory());

        Class.forName(driver);

        conn = DriverManager.getConnection(url);
    }

    @After
    public void destroy() throws Exception {
        if (conn != null) {
            conn.close();
            conn = null;
        }
    }

    @Test
    public void sqlToArrowTestInt() throws Exception {

        Table table =
                mapper.readValue(
                        this.getClass().getClassLoader().getResourceAsStream("h2/test1_int_h2.yml"),
                        Table.class);

        RootAllocator rootAllocator = new RootAllocator(Integer.MAX_VALUE);
        try {
            createTestData(conn, table);

            VectorSchemaRoot root = JdbcToArrow.sqlToArrow(conn, table.getQuery(), rootAllocator);

            int[] values = {
                    101, 101, 101, 101, 101, 101, 101, 101, 101, 101, 101, 101, 101, 101, 101,
            };
            assertIntVectorValues((IntVector)root.getVector("INT_FIELD1"), 15, values);

        } catch (Exception e) {
            throw e;
        } finally {
            deleteTestData(conn, table);
        }

    }

    @Test
    public void sqlToArrowTestBool() throws Exception {

        Table table =
                mapper.readValue(
                        this.getClass().getClassLoader().getResourceAsStream("h2/test1_bool_h2.yml"),
                        Table.class);
        RootAllocator rootAllocator = new RootAllocator(Integer.MAX_VALUE);

        try {
            createTestData(conn, table);

            VectorSchemaRoot root = JdbcToArrow.sqlToArrow(conn, table.getQuery(), rootAllocator);

            int[] bools = {
                    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1
            };
            assertBitBooleanVectorValues((BitVector)root.getVector("BOOL_FIELD2"), 15, bools);

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            deleteTestData(conn, table);
        }

    }

    @Test
    public void sqlToArrowTestTinyInt() throws Exception {

        Table table =
                mapper.readValue(
                        this.getClass().getClassLoader().getResourceAsStream("h2/test1_tinyint_h2.yml"),
                        Table.class);
        RootAllocator rootAllocator = new RootAllocator(Integer.MAX_VALUE);
        try {
            createTestData(conn, table);

            VectorSchemaRoot root = JdbcToArrow.sqlToArrow(conn, table.getQuery(), rootAllocator);

            int[] tinyints = {
                    45, 45, 45, 45, 45, 45, 45, 45, 45, 45, 45, 45, 45, 45, 45
            };
            assertTinyIntVectorValues((TinyIntVector)root.getVector("TINYINT_FIELD3"), 15, tinyints);

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            deleteTestData(conn, table);
        }

    }

    @Test
    public void sqlToArrowTestSmallInt() throws Exception {

        Table table =
                mapper.readValue(
                        this.getClass().getClassLoader().getResourceAsStream("h2/test1_smallint_h2.yml"),
                        Table.class);
        RootAllocator rootAllocator = new RootAllocator(Integer.MAX_VALUE);
        try {
            createTestData(conn, table);

            VectorSchemaRoot root = JdbcToArrow.sqlToArrow(conn, table.getQuery(), rootAllocator);

            int[] smallints = {
                    12000, 12000, 12000, 12000, 12000, 12000, 12000, 12000, 12000, 12000, 12000, 12000, 12000, 12000, 12000
            };
            assertSmallIntVectorValues((SmallIntVector)root.getVector("SMALLINT_FIELD4"), 15, smallints);

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            deleteTestData(conn, table);
        }

    }

    @Test
    public void sqlToArrowTestBigInt() throws Exception {

        Table table =
                mapper.readValue(
                        this.getClass().getClassLoader().getResourceAsStream("h2/test1_bigint_h2.yml"),
                        Table.class);
        RootAllocator rootAllocator = new RootAllocator(Integer.MAX_VALUE);
        try {
            createTestData(conn, table);

            VectorSchemaRoot root = JdbcToArrow.sqlToArrow(conn, table.getQuery(), rootAllocator);

            int[] bigints = {
                    92233720, 92233720, 92233720, 92233720, 92233720, 92233720, 92233720, 92233720, 92233720,
                    92233720, 92233720, 92233720, 92233720, 92233720, 92233720
            };
            assertBigIntVectorValues((BigIntVector)root.getVector("BIGINT_FIELD5"), 15, bigints);

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            deleteTestData(conn, table);
        }

    }
    
    @Test
    public void sqlToArrowTestBlob() throws Exception {

        Table table =
                mapper.readValue(
                        this.getClass().getClassLoader().getResourceAsStream("h2/test1_blob_h2.yml"),
                        Table.class);
        RootAllocator rootAllocator = new RootAllocator(Integer.MAX_VALUE);
        try {
            createTestData(conn, table);

            VectorSchemaRoot root = JdbcToArrow.sqlToArrow(conn, table.getQuery(), rootAllocator);

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
            assertVarBinaryVectorValues((VarBinaryVector)root.getVector("BLOB_FIELD14"), 15, bytes);

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            deleteTestData(conn, table);
        }

    }

    @Test
    public void sqlToArrowTestClobs() throws Exception {
        Table table =
                mapper.readValue(
                        this.getClass().getClassLoader().getResourceAsStream("h2/test1_clob_h2.yml"),
                        Table.class);

        RootAllocator rootAllocator = new RootAllocator(Integer.MAX_VALUE);
        try {
            createTestData(conn, table);

            VectorSchemaRoot root = JdbcToArrow.sqlToArrow(conn, table.getQuery(), rootAllocator);
                       
            byte[] strb = "some text that needs to be converted to clob".getBytes();
            byte[][] varchars = {
                    strb, strb, strb, strb, strb, strb, strb, strb, strb, strb, strb, strb, strb, strb, strb
            };
            assertVarcharVectorValues((VarCharVector)root.getVector("CLOB_FIELD15"), 15, varchars);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            deleteTestData(conn, table);
        }
    }
    
    @Test
    public void sqlToArrowTestAllDataTypes() throws Exception {

        Table table =
                mapper.readValue(
                        this.getClass().getClassLoader().getResourceAsStream("h2/test1_all_datatypes_h2.yml"),
                        Table.class);

        RootAllocator rootAllocator = new RootAllocator(Integer.MAX_VALUE);
        try {
            createTestData(conn, table);

            VectorSchemaRoot root = JdbcToArrow.sqlToArrow(conn, table.getQuery(), rootAllocator);

            int[] ints = {
                    101, 101, 101, 101, 101, 101, 101, 101, 101, 101, 101, 101, 101, 101, 101
            };
            assertIntVectorValues((IntVector)root.getVector("INT_FIELD1"), 15, table.getInts());

            int[] bools = {
                    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1
            };
            assertBitBooleanVectorValues((BitVector)root.getVector("BOOL_FIELD2"), 15, bools);

            int[] tinyints = {
                    45, 45, 45, 45, 45, 45, 45, 45, 45, 45, 45, 45, 45, 45, 45
            };
            assertTinyIntVectorValues((TinyIntVector)root.getVector("TINYINT_FIELD3"), 15, tinyints);

            int[] smallints = {
                    12000, 12000, 12000, 12000, 12000, 12000, 12000, 12000, 12000, 12000, 12000, 12000, 12000, 12000, 12000
            };
            assertSmallIntVectorValues((SmallIntVector)root.getVector("SMALLINT_FIELD4"), 15, smallints);

            int[] bigints = {
                    92233720, 92233720, 92233720, 92233720, 92233720, 92233720, 92233720, 92233720, 92233720,
                    92233720, 92233720, 92233720, 92233720, 92233720, 92233720
            };
            assertBigIntVectorValues((BigIntVector)root.getVector("BIGINT_FIELD5"), 15, bigints);

            BigDecimal[] bigdecimals = {
                    new BigDecimal(17345667789.23), new BigDecimal(17345667789.23), new BigDecimal(17345667789.23),
                    new BigDecimal(17345667789.23), new BigDecimal(17345667789.23), new BigDecimal(17345667789.23),
                    new BigDecimal(17345667789.23), new BigDecimal(17345667789.23), new BigDecimal(17345667789.23),
                    new BigDecimal(17345667789.23), new BigDecimal(17345667789.23), new BigDecimal(17345667789.23),
                    new BigDecimal(17345667789.23), new BigDecimal(17345667789.23), new BigDecimal(17345667789.23)
            };
            assertDecimalVectorValues((DecimalVector)root.getVector("DECIMAL_FIELD6"), 15, bigdecimals);

            double[] doubles = {
                    56478356785.345, 56478356785.345, 56478356785.345, 56478356785.345, 56478356785.345, 56478356785.345,
                    56478356785.345, 56478356785.345, 56478356785.345,
                    56478356785.345, 56478356785.345, 56478356785.345, 56478356785.345, 56478356785.345, 56478356785.345
            };
            assertFloat8VectorValues((Float8Vector)root.getVector("DOUBLE_FIELD7"), 15, doubles);

            float[] reals = {
                    56478356785.345f, 56478356785.345f, 56478356785.345f, 56478356785.345f, 56478356785.345f, 56478356785.345f,
                    56478356785.345f, 56478356785.345f, 56478356785.345f,
                    56478356785.345f, 56478356785.345f, 56478356785.345f, 56478356785.345f, 56478356785.345f, 56478356785.345f
            };
            assertFloat4VectorValues((Float4Vector)root.getVector("REAL_FIELD8"), 15, reals);

            long[] times = {
                    45935000, 45935000, 45935000, 45935000, 45935000, 45935000, 45935000, 45935000,
                    45935000, 45935000, 45935000, 45935000, 45935000, 45935000, 45935000
            };
            assertTimeVectorValues((TimeMilliVector)root.getVector("TIME_FIELD9"), 15, times);

            long[] dates = {
            		1518393600000l, 1518393600000l, 1518393600000l, 1518393600000l, 1518393600000l, 1518393600000l, 1518393600000l, 1518393600000l,
            		1518393600000l, 1518393600000l, 1518393600000l, 1518393600000l, 1518393600000l, 1518393600000l, 1518393600000l
            };
            assertDateVectorValues((DateMilliVector)root.getVector("DATE_FIELD10"), 15, dates);

            long[] timestamps = {
                    1518439535000l, 1518439535000l, 1518439535000l, 1518439535000l, 1518439535000l, 1518439535000l, 1518439535000l, 1518439535000l,
                    1518439535000l, 1518439535000l, 1518439535000l, 1518439535000l, 1518439535000l, 1518439535000l, 1518439535000l
            };
            assertTimeStampVectorValues((TimeStampVector)root.getVector("TIMESTAMP_FIELD11"), 15, timestamps);

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
            assertVarBinaryVectorValues((VarBinaryVector)root.getVector("BINARY_FIELD12"), 15, bytes);

            byte[] strb = "some text that needs to be converted to varchar".getBytes();
            byte[][] varchars = {
                strb, strb, strb, strb, strb, strb, strb, strb, strb, strb, strb, strb, strb, strb, strb
            };
            assertVarcharVectorValues((VarCharVector)root.getVector("VARCHAR_FIELD13"), 15, varchars);

            assertVarBinaryVectorValues((VarBinaryVector)root.getVector("BLOB_FIELD14"), 15, bytes);

            strb = "some text that needs to be converted to clob".getBytes();
            varchars = new byte[][] {
                    strb, strb, strb, strb, strb, strb, strb, strb, strb, strb, strb, strb, strb, strb, strb
            };
            assertVarcharVectorValues((VarCharVector)root.getVector("CLOB_FIELD15"), 15, varchars);

            strb = "some char text".getBytes();
            varchars = new byte[][] {
                    strb, strb, strb, strb, strb, strb, strb, strb, strb, strb, strb, strb, strb, strb, strb
            };
            assertVarcharVectorValues((VarCharVector)root.getVector("CHAR_FIELD16"), 15, varchars);

            assertBitBooleanVectorValues((BitVector)root.getVector("BIT_FIELD17"), 15, bools);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            deleteTestData(conn, table);
        }

    }

}
