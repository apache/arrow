/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.arrow.adapter.jdbc.h2;

import static org.apache.arrow.adapter.jdbc.JdbcToArrowTestHelper.assertMapVectorValues;
import static org.apache.arrow.adapter.jdbc.JdbcToArrowTestHelper.getMapValues;

import java.io.IOException;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.stream.Stream;
import org.apache.arrow.adapter.jdbc.AbstractJdbcToArrowTest;
import org.apache.arrow.adapter.jdbc.JdbcToArrowConfigBuilder;
import org.apache.arrow.adapter.jdbc.Table;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.MapVector;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/** Test MapConsumer with OTHER jdbc type. */
public class JdbcToArrowMapDataTypeTest extends AbstractJdbcToArrowTest {

  public static Stream<Arguments> getTestData() throws IOException {
    return Stream.of(
        Arguments.of(getTable("h2/test1_map_h2.yml", JdbcToArrowMapDataTypeTest.class)));
  }

  /** Test Method to test JdbcToArrow Functionality for Map form Types.OTHER column. */
  @ParameterizedTest
  @MethodSource("getTestData")
  public void testJdbcToArrowValues(Table table)
      throws SQLException, IOException, ClassNotFoundException {
    this.initializeDatabase(table);

    Calendar calendar = Calendar.getInstance();
    ResultSetMetaData rsmd = getQueryMetaData(table.getQuery());
    testDataSets(
        sqlToArrow(
            conn.createStatement().executeQuery(table.getQuery()),
            new JdbcToArrowConfigBuilder(
                    new RootAllocator(Integer.MAX_VALUE), Calendar.getInstance())
                .setJdbcToArrowTypeConverter(jdbcToArrowTypeConverter(calendar, rsmd))
                .build()),
        true);
    testDataSets(
        sqlToArrow(
            conn,
            table.getQuery(),
            new JdbcToArrowConfigBuilder(
                    new RootAllocator(Integer.MAX_VALUE), Calendar.getInstance())
                .setJdbcToArrowTypeConverter(jdbcToArrowTypeConverter(calendar, rsmd))
                .build()),
        true);
  }

  /**
   * This method calls the assert methods for various DataSets.
   *
   * @param root VectorSchemaRoot for test
   * @param isIncludeMapVector is this dataset checks includes map column. Jdbc type to 'map'
   *     mapping declared in configuration only manually
   */
  @Override
  public void testDataSets(VectorSchemaRoot root, boolean isIncludeMapVector) {
    assertMapVectorValues(
        (MapVector) root.getVector(MAP), table.getRowCount(), getMapValues(table.getValues(), MAP));
  }
}
