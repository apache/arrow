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

import com.google.common.collect.ImmutableSet;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.JarURLConnection;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.stream.Stream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.function.Executable;

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
      ImmutableSet.of(
          "org/apache/arrow/driver/jdbc/", // Driver code
          "META-INF/maven/", // Maven metadata (useful for security scanner
          "META-INF/services/", // ServiceLoader implementations
          "META-INF/license/",
          "META-INF/licenses/",
          // Prefixes for native libraries
          "META-INF/native/liborg_apache_arrow_driver_jdbc_shaded_",
          "META-INF/native/org_apache_arrow_driver_jdbc_shaded_");

  /** List of allowed files a jar entry may match. */
  public static final Set<String> ALLOWED_FILES =
      ImmutableSet.of(
          "arrow-git.properties",
          "properties/flight.properties",
          "META-INF/io.netty.versions.properties",
          "META-INF/MANIFEST.MF",
          "META-INF/DEPENDENCIES",
          "META-INF/FastDoubleParser-LICENSE",
          "META-INF/FastDoubleParser-NOTICE",
          "META-INF/LICENSE",
          "META-INF/LICENSE.txt",
          "META-INF/NOTICE",
          "META-INF/NOTICE.txt",
          "META-INF/thirdparty-LICENSE",
          "META-INF/bigint-LICENSE");

  // This method is designed to work with Maven failsafe plugin and expects the
  // JDBC driver jar to be present in the test classpath (instead of the individual classes)
  private static File getJdbcJarFile() throws IOException {
    // Check if an override has been set
    if (JDBC_DRIVER_PATH_OVERRIDE != null) {
      return new File(JDBC_DRIVER_PATH_OVERRIDE);
    }

    // Check classpath to find the driver jar (without loading the class)
    URL driverClassURL =
        ITDriverJarValidation.class
            .getClassLoader()
            .getResource("org/apache/arrow/driver/jdbc/ArrowFlightJdbcDriver.class");

    assertNotNull(driverClassURL, "Driver class was not detected in the classpath");
    assertEquals(
        "jar", driverClassURL.getProtocol(), "Driver class was not found inside a jar file");

    // Return the enclosing jar file
    JarURLConnection connection = (JarURLConnection) driverClassURL.openConnection();
    try {
      return new File(connection.getJarFileURL().toURI());
    } catch (URISyntaxException e) {
      throw new IOException(e);
    }
  }

  /** Validate the content of the jar to enforce all 3rd party dependencies have been shaded. */
  @Test
  @Timeout(value = 2, unit = TimeUnit.MINUTES)
  public void validateShadedJar() throws IOException {

    try (JarFile jar = new JarFile(getJdbcJarFile())) {
      Stream<Executable> executables =
          jar.stream()
              .filter(Predicate.not(JarEntry::isDirectory))
              .map(
                  entry -> {
                    return () -> checkEntryAllowed(entry.getName());
                  });

      Assertions.assertAll(executables);
    }
  }

  /** Check that relocated netty code can also load matching native library. */
  @Test
  @Timeout(value = 2, unit = TimeUnit.MINUTES)
  public void checkNettyOpenSslNativeLoader() throws Throwable {
    try (URLClassLoader driverClassLoader =
        new URLClassLoader(new URL[] {getJdbcJarFile().toURI().toURL()}, null)) {
      Class<?> openSslClass =
          driverClassLoader.loadClass(
              "org.apache.arrow.driver.jdbc.shaded.io.netty.handler.ssl.OpenSsl");
      Method method = openSslClass.getDeclaredMethod("ensureAvailability");
      try {
        method.invoke(null);
      } catch (InvocationTargetException e) {
        throw e.getCause();
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
