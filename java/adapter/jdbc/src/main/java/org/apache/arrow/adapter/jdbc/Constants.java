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

package org.apache.arrow.adapter.jdbc;

/**
 * String constants used for metadata returned on Vectors.
 */
public class Constants {
  private Constants() {}

  public static final String SQL_CATALOG_NAME_KEY = "SQL_CATALOG_NAME";
  public static final String SQL_TABLE_NAME_KEY = "SQL_TABLE_NAME";
  public static final String SQL_COLUMN_NAME_KEY = "SQL_COLUMN_NAME";
  public static final String SQL_TYPE_KEY = "SQL_TYPE";

}
