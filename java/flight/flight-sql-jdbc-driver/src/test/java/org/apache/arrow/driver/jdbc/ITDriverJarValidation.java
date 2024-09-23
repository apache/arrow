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
package org.apache.arrow.driver.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

import com.google.common.collect.ImmutableSet;
import java.io.File;
import java.io.IOException;
import java.net.JarURLConnection;
import java.net.URL;
import java.util.Enumeration;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Check the content of the JDBC driver jar
 *
 * <p>After shading everything should be either under org.apache.arrow.driver.jdbc. package
 */
public class ITDriverJarValidation {
  /**
   * Use this property to provide path to the JDBC driver jar. Can be used to run the test from an
   * IDE
   */
  public static final String JDBC_DRIVER_PATH_OVERRIDE =
      System.getProperty("arrow-flight-jdbc-driver.jar.override");

  /** List of allowed prefixes a jar entry may match. */
  public static final Set<String> ALLOWED_PREFIXES =
      ImmutableSet.of("org/apache/arrow/driver/jdbc/", "META-INF/");

  /** List of allowed files a jar entry may match. */
  public static final Set<String> ALLOWED_FILES =
      ImmutableSet.of("arrow-git.properties", "properties/flight.properties");

  // This method is designed to work with Maven failsafe plugin and expects the
  // JDBC driver jar to be present in the test classpath (instead of the individual classes)
  private static JarFile getJdbcJarFile() throws IOException {
    // Check if an override has been set
    if (JDBC_DRIVER_PATH_OVERRIDE != null) {
      return new JarFile(new File(JDBC_DRIVER_PATH_OVERRIDE));
    }

    // Check classpath to find the driver jar
    URL driverClassURL =
        ITDriverJarValidation.class
            .getClassLoader()
            .getResource("org/apache/arrow/driver/jdbc/ArrowFlightJdbcDriver.class");

    assertNotNull(driverClassURL, "Driver jar was not detected in the classpath");
    assertEquals(
        "jar", driverClassURL.getProtocol(), "Driver jar was not detected in the classpath");

    JarURLConnection connection = (JarURLConnection) driverClassURL.openConnection();
    return connection.getJarFile();
  }

  @Test
  @Timeout(value = 2, unit = TimeUnit.MINUTES)
  public void validateShadedJar() throws IOException {
    // Validate the content of the jar to enforce all 3rd party dependencies have
    // been shaded
    try (JarFile jar = getJdbcJarFile()) {
      for (Enumeration<JarEntry> entries = jar.entries(); entries.hasMoreElements(); ) {
        final JarEntry entry = entries.nextElement();
        if (entry.isDirectory()) {
          // Directories are ignored
          continue;
        }

        try {
          checkEntryAllowed(entry.getName());
        } catch (AssertionError e) {
          fail(e.getMessage());
        }
      }
    }
  }

  /**
   * Check if a jar entry is allowed.
   *
   * <p>A jar entry is allowed if either it is part of the allowed files or it matches one of the
   * allowed prefixes
   *
   * @param name the jar entry name
   * @throws AssertionError if the entry is not allowed
   */
  private void checkEntryAllowed(String name) {
    // Check if there's a matching file entry first
    if (ALLOWED_FILES.contains(name)) {
      return;
    }

    for (String prefix : ALLOWED_PREFIXES) {
      if (name.startsWith(prefix)) {
        return;
      }
    }

    throw new AssertionError("'" + name + "' is not an allowed jar entry");
  }
}
