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

package org.apache.arrow.flight.sql;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Metadata for a column in a Flight SQL query.
 *
 * This can be used with FlightSqlClient to access column's metadata contained in schemas returned
 * by GetTables and query execution as follows:
 * <pre>
 *   FlightSqlColumnMetadata metadata = new FlightSqlColumnMetadata(field.getMetadata());
 *   Integer precision = metadata.getPrecision();
 * </pre>
 *
 * FlightSqlProducer can use this to set metadata on a column in a schema as follows:
 * <pre>
 *   FlightSqlColumnMetadata metadata = new FlightSqlColumnMetadata.Builder()
 *         .precision(10)
 *         .scale(5)
 *         .build();
 *   Field field = new Field("column", new FieldType(..., metadata.getMetadataMap()), null);
 * </pre>
 */
public class FlightSqlColumnMetadata {

  private static final String CATALOG_NAME = "ARROW:FLIGHT:SQL:CATALOG_NAME";
  private static final String SCHEMA_NAME = "ARROW:FLIGHT:SQL:SCHEMA_NAME";
  private static final String TABLE_NAME = "ARROW:FLIGHT:SQL:TABLE_NAME";
  private static final String TYPE_NAME = "ARROW:FLIGHT:SQL:TYPE_NAME";
  private static final String PRECISION = "ARROW:FLIGHT:SQL:PRECISION";
  private static final String SCALE = "ARROW:FLIGHT:SQL:SCALE";
  private static final String IS_AUTO_INCREMENT = "ARROW:FLIGHT:SQL:IS_AUTO_INCREMENT";
  private static final String IS_CASE_SENSITIVE = "ARROW:FLIGHT:SQL:IS_CASE_SENSITIVE";
  private static final String IS_READ_ONLY = "ARROW:FLIGHT:SQL:IS_READ_ONLY";
  private static final String IS_SEARCHABLE = "ARROW:FLIGHT:SQL:IS_SEARCHABLE";

  private static final String BOOLEAN_TRUE_STR = "1";
  private static final String BOOLEAN_FALSE_STR = "0";

  private final Map<String, String> metadataMap;

  /**
   * Creates a new instance of FlightSqlColumnMetadata.
   */
  public FlightSqlColumnMetadata(Map<String, String> metadataMap) {
    this.metadataMap = new HashMap<>(metadataMap);
  }

  /**
   * Returns the metadata map.
   * @return The metadata map.
   */
  public Map<String, String> getMetadataMap() {
    return Collections.unmodifiableMap(metadataMap);
  }

  /**
   * Returns the catalog name.
   * @return The catalog name.
   */
  public String getCatalogName() {
    return metadataMap.get(CATALOG_NAME);
  }

  /**
   * Returns the schema name.
   * @return The schema name.
   */
  public String getSchemaName() {
    return metadataMap.get(SCHEMA_NAME);
  }

  /**
   * Returns the table name.
   * @return The table name.
   */
  public String getTableName() {
    return metadataMap.get(TABLE_NAME);
  }

  /**
   * Returns the type name.
   * @return The type name.
   */
  public String getTypeName() {
    return metadataMap.get(TYPE_NAME);
  }

  /**
   * Returns the precision / column size.
   * @return The precision / column size.
   */
  public Integer getPrecision() {
    String value = metadataMap.get(PRECISION);
    if (value == null) {
      return null;
    }

    return Integer.valueOf(value);
  }

  /**
   * Returns the scale / decimal digits.
   * @return The scale / decimal digits.
   */
  public Integer getScale() {
    String value = metadataMap.get(SCALE);
    if (value == null) {
      return null;
    }

    return Integer.valueOf(value);
  }

  /**
   * Returns if the column is auto incremented.
   * @return True if the column is auto incremented, false otherwise.
   */
  public Boolean isAutoIncrement() {
    String value = metadataMap.get(IS_AUTO_INCREMENT);
    if (value == null) {
      return null;
    }

    return stringToBoolean(value);
  }

  /**
   * Returns if the column is case-sensitive.
   * @return True if the column is case-sensitive, false otherwise.
   */
  public Boolean isCaseSensitive() {
    String value = metadataMap.get(IS_CASE_SENSITIVE);
    if (value == null) {
      return null;
    }

    return stringToBoolean(value);
  }

  /**
   * Returns if the column is read only.
   * @return True if the column is read only, false otherwise.
   */
  public Boolean isReadOnly() {
    String value = metadataMap.get(IS_READ_ONLY);
    if (value == null) {
      return null;
    }

    return stringToBoolean(value);
  }

  /**
   * Returns if the column is searchable.
   * @return True if the column is searchable, false otherwise.
   */
  public Boolean isSearchable() {
    String value = metadataMap.get(IS_SEARCHABLE);
    if (value == null) {
      return null;
    }

    return stringToBoolean(value);
  }

  /**
   * Builder of FlightSqlColumnMetadata, used on FlightSqlProducer implementations.
   */
  public static class Builder {
    private final Map<String, String> metadataMap;

    /**
     * Creates a new instance of FlightSqlColumnMetadata.Builder.
     */
    public Builder() {
      this.metadataMap = new HashMap<>();
    }

    /**
     * Sets the catalog name.
     * @param catalogName the catalog name.
     * @return This builder.
     */
    public Builder catalogName(String catalogName) {
      metadataMap.put(CATALOG_NAME, catalogName);
      return this;
    }

    /**
     * Sets the schema name.
     * @param schemaName The schema name.
     * @return This builder.
     */
    public Builder schemaName(String schemaName) {
      metadataMap.put(SCHEMA_NAME, schemaName);
      return this;
    }

    /**
     * Sets the table name.
     * @param tableName The table name.
     * @return This builder.
     */
    public Builder tableName(String tableName) {
      metadataMap.put(TABLE_NAME, tableName);
      return this;
    }

    /**
     * Sets the type name.
     * @param typeName The type name.
     * @return This builder.
     */
    public Builder typeName(String typeName) {
      metadataMap.put(TYPE_NAME, typeName);
      return this;
    }

    /**
     * Sets the precision / column size.
     * @param precision The precision / column size.
     * @return This builder.
     */
    public Builder precision(int precision) {
      metadataMap.put(PRECISION, Integer.toString(precision));
      return this;
    }

    /**
     * Sets the scale / decimal digits.
     * @param scale The scale / decimal digits.
     * @return This builder.
     */
    public Builder scale(int scale) {
      metadataMap.put(SCALE, Integer.toString(scale));
      return this;
    }

    /**
     * Sets if the column is auto incremented.
     * @param isAutoIncrement True if the column is auto incremented.
     * @return This builder.
     */
    public Builder isAutoIncrement(boolean isAutoIncrement) {
      metadataMap.put(IS_AUTO_INCREMENT, booleanToString(isAutoIncrement));
      return this;
    }

    /**
     * Sets if the column is case-sensitive.
     * @param isCaseSensitive If the column is case-sensitive.
     * @return This builder.
     */
    public Builder isCaseSensitive(boolean isCaseSensitive) {
      metadataMap.put(IS_CASE_SENSITIVE, booleanToString(isCaseSensitive));
      return this;
    }

    /**
     * Sets if the column is read only.
     * @param isReadOnly If the column is read only.
     * @return This builder.
     */
    public Builder isReadOnly(boolean isReadOnly) {
      metadataMap.put(IS_READ_ONLY, booleanToString(isReadOnly));
      return this;
    }

    /**
     * Sets if the column is searchable.
     * @param isSearchable If the column is searchable.
     * @return This builder.
     */
    public Builder isSearchable(boolean isSearchable) {
      metadataMap.put(IS_SEARCHABLE, booleanToString(isSearchable));
      return this;
    }

    /**
     * Builds a new instance of FlightSqlColumnMetadata.
     * @return A new instance of FlightSqlColumnMetadata.
     */
    public FlightSqlColumnMetadata build() {
      return new FlightSqlColumnMetadata(metadataMap);
    }
  }

  private static String booleanToString(boolean boolValue) {
    return boolValue ? BOOLEAN_TRUE_STR : BOOLEAN_FALSE_STR;
  }

  private static boolean stringToBoolean(String value) {
    return value.equals(BOOLEAN_TRUE_STR);
  }
}
