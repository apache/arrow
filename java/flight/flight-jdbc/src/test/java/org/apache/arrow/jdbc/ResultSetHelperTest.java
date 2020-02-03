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

package org.apache.arrow.jdbc;

import static org.junit.Assert.*;

import java.sql.SQLException;

import org.junit.Test;

/**
 * JDBC Driver unit tests.
 */
public class ResultSetHelperTest {

  //TODO add exhaustive tests based on suggested conversions in JDBC specification

  @Test
  public void getString() throws SQLException {
    assertNull(ResultSetHelper.getString(null));
    assertEquals("a", ResultSetHelper.getString("a"));
    assertEquals("123", ResultSetHelper.getString(123));
  }

  @Test
  public void getInt() throws SQLException {
    assertEquals(0, ResultSetHelper.getInt(null));
    assertEquals(123, ResultSetHelper.getInt("123"));
    assertEquals(123, ResultSetHelper.getInt(123));
  }

}
