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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.junit.Test;

/**
 * JDBC Driver unit tests.
 */
public class DriverTest {

  final Driver driver = new org.apache.arrow.jdbc.Driver();

  @Test
  public void acceptsValidUrl() throws SQLException {
    assertTrue(driver.acceptsURL("jdbc:arrow://localhost:50051"));
  }

  @Test
  public void rejectsInvalidUrl() throws SQLException {
    assertFalse(driver.acceptsURL("jdbc:mysql://localhost:50051"));
  }

  @Test
  public void rejectsNullUrl() throws SQLException {
    assertFalse(driver.acceptsURL(null));
  }

  /**
   * Note that this is a manual integration test that requires the Rust flight-server example to be running.
   */
  @Test
  public void executeQuery() throws SQLException {
    try (Connection conn = driver.connect("jdbc:arrow://localhost:50051", new Properties())) {
      try (Statement stmt = conn.createStatement()) {
        try (ResultSet rs = stmt.executeQuery("SELECT id FROM alltypes_plain")) {
          assertTrue(rs.next());
          assertEquals(5, rs.getInt(1));
        }
      }
    }
  }
}
