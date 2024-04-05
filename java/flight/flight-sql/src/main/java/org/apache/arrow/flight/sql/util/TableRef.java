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

package org.apache.arrow.flight.sql.util;

/**
 * A helper class to reference a table to be passed to the flight
 * sql client.
 */
public class TableRef {
  private final String catalog;
  private final String dbSchema;
  private final String table;

  /**
   * The complete constructor for the TableRef class.
   * @param catalog   the catalog from a table.
   * @param dbSchema  the database schema from a table.
   * @param table     the table name from a table.
   */
  public TableRef(String catalog, String dbSchema, String table) {
    this.catalog = catalog;
    this.dbSchema = dbSchema;
    this.table = table;
  }

  /**
   * A static initializer of the TableRef with all the arguments.
   * @param catalog   the catalog from a table.
   * @param dbSchema  the database schema from a table.
   * @param table     the table name from a table.
   * @return  A TableRef object.
   */
  public static TableRef of(String catalog, String dbSchema, String table) {
    return new TableRef(catalog, dbSchema, table);
  }

  /**
   * Retrieve the catalog from the object.
   * @return  the catalog.
   */
  public String getCatalog() {
    return catalog;
  }

  /**
   * Retrieves the db schema from the object.
   * @return  the dbSchema
   */
  public String getDbSchema() {
    return dbSchema;
  }

  /**
   * Retrieves the table from the object.
   * @return  the table.
   */
  public String getTable() {
    return table;
  }
}

