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

import org.apache.arrow.vector.types.pojo.Schema;

/**
 * Utility class for testing {@link FlightSqlExample}.
 */
public class SampleTestUtils {
  public static final Schema GET_TABLES_SCHEMA = FlightSqlProducer.GET_TABLES_SCHEMA;
  public static final Schema GET_TABLES_SCHEMA_NO_SCHEMA = FlightSqlProducer.GET_TABLES_SCHEMA_NO_SCHEMA;
  public static final Schema GET_CATALOGS_SCHEMA = FlightSqlProducer.GET_CATALOGS_SCHEMA;
  public static final Schema GET_TABLE_TYPES_SCHEMA = FlightSqlProducer.GET_TABLE_TYPES_SCHEMA;
  public static final Schema GET_SCHEMAS_SCHEMA = FlightSqlProducer.GET_SCHEMAS_SCHEMA;
}
