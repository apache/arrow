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
package org.apache.arrow.driver.jdbc.utils;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.arrow.flight.sql.FlightSqlColumnMetadata;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.proto.Common;
import org.junit.jupiter.api.Test;

public class ConvertUtilsTest {

  @Test
  public void testShouldSetOnColumnMetaDataBuilder() {

    final Common.ColumnMetaData.Builder builder = Common.ColumnMetaData.newBuilder();
    final FlightSqlColumnMetadata expectedColumnMetaData =
        new FlightSqlColumnMetadata.Builder()
            .catalogName("catalog1")
            .schemaName("schema1")
            .tableName("table1")
            .isAutoIncrement(true)
            .isCaseSensitive(true)
            .isReadOnly(true)
            .isSearchable(true)
            .precision(20)
            .scale(10)
            .build();
    ConvertUtils.setOnColumnMetaDataBuilder(builder, expectedColumnMetaData.getMetadataMap());
    assertBuilder(builder, expectedColumnMetaData);
  }

  @Test
  public void testShouldConvertArrowFieldsToColumnMetaDataList() {

    final List<Field> listField =
        ImmutableList.of(
            new Field(
                "col1",
                new FieldType(
                    true,
                    ArrowType.Utf8.INSTANCE,
                    null,
                    new FlightSqlColumnMetadata.Builder()
                        .catalogName("catalog1")
                        .schemaName("schema1")
                        .tableName("table1")
                        .build()
                        .getMetadataMap()),
                null));

    final List<ColumnMetaData> expectedColumnMetaData =
        ImmutableList.of(
            ColumnMetaData.fromProto(
                Common.ColumnMetaData.newBuilder()
                    .setCatalogName("catalog1")
                    .setSchemaName("schema1")
                    .setTableName("table1")
                    .build()));

    final List<ColumnMetaData> actualColumnMetaData =
        ConvertUtils.convertArrowFieldsToColumnMetaDataList(listField);
    assertColumnMetaData(expectedColumnMetaData, actualColumnMetaData);
  }

  private void assertColumnMetaData(
      final List<ColumnMetaData> expected, final List<ColumnMetaData> actual) {
    assertThat(expected.size(), equalTo(actual.size()));
    int size = expected.size();
    for (int i = 0; i < size; i++) {
      final ColumnMetaData expectedColumnMetaData = expected.get(i);
      final ColumnMetaData actualColumnMetaData = actual.get(i);
      assertThat(expectedColumnMetaData.catalogName, equalTo(actualColumnMetaData.catalogName));
      assertThat(expectedColumnMetaData.schemaName, equalTo(actualColumnMetaData.schemaName));
      assertThat(expectedColumnMetaData.tableName, equalTo(actualColumnMetaData.tableName));
      assertThat(expectedColumnMetaData.readOnly, equalTo(actualColumnMetaData.readOnly));
      assertThat(expectedColumnMetaData.autoIncrement, equalTo(actualColumnMetaData.autoIncrement));
      assertThat(expectedColumnMetaData.precision, equalTo(actualColumnMetaData.precision));
      assertThat(expectedColumnMetaData.scale, equalTo(actualColumnMetaData.scale));
      assertThat(expectedColumnMetaData.caseSensitive, equalTo(actualColumnMetaData.caseSensitive));
      assertThat(expectedColumnMetaData.searchable, equalTo(actualColumnMetaData.searchable));
    }
  }

  private void assertBuilder(
      final Common.ColumnMetaData.Builder builder,
      final FlightSqlColumnMetadata flightSqlColumnMetaData) {

    final Integer precision = flightSqlColumnMetaData.getPrecision();
    final Integer scale = flightSqlColumnMetaData.getScale();

    assertThat(flightSqlColumnMetaData.getCatalogName(), equalTo(builder.getCatalogName()));
    assertThat(flightSqlColumnMetaData.getSchemaName(), equalTo(builder.getSchemaName()));
    assertThat(flightSqlColumnMetaData.getTableName(), equalTo(builder.getTableName()));
    assertThat(flightSqlColumnMetaData.isAutoIncrement(), equalTo(builder.getAutoIncrement()));
    assertThat(flightSqlColumnMetaData.isCaseSensitive(), equalTo(builder.getCaseSensitive()));
    assertThat(flightSqlColumnMetaData.isSearchable(), equalTo(builder.getSearchable()));
    assertThat(flightSqlColumnMetaData.isReadOnly(), equalTo(builder.getReadOnly()));
    assertThat(precision == null ? 0 : precision, equalTo(builder.getPrecision()));
    assertThat(scale == null ? 0 : scale, equalTo(builder.getScale()));
  }
}
