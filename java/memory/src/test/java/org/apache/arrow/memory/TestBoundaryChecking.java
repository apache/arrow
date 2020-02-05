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

package org.apache.arrow.memory;

import java.lang.reflect.Field;
import java.net.URLClassLoader;

import org.junit.Assert;
import org.junit.Test;

/**
 * Test cases for evaluating the value of {@link BoundsChecking#BOUNDS_CHECKING_ENABLED}.
 */
public class TestBoundaryChecking {

  /**
   * Get a copy of the current class loader.
   * @return the newly created class loader.
   */
  private ClassLoader copyClassLoader() {
    ClassLoader curClassLoader = this.getClass().getClassLoader();
    if (curClassLoader instanceof URLClassLoader) {
      // for Java 1.8
      return new URLClassLoader(((URLClassLoader) curClassLoader).getURLs(), null);
    }

    // for Java 1.9 and Java 11.
    return null;
  }

  /**
   * Get the value of flag  {@link BoundsChecking#BOUNDS_CHECKING_ENABLED}.
   * @param classLoader the class loader from which to get the flag value.
   * @return value of the flag.
   */
  private boolean getFlagValue(ClassLoader classLoader) throws Exception {
    Class<?> clazz = classLoader.loadClass("org.apache.arrow.memory.BoundsChecking");
    Field field = clazz.getField("BOUNDS_CHECKING_ENABLED");
    return (Boolean) field.get(null);
  }

  /**
   * Ensure the flag for bounds checking is enabled by default.
   * This will protect users from JVM crashes.
   */
  @Test
  public void testDefaultValue() throws Exception {
    ClassLoader classLoader = copyClassLoader();
    if (classLoader != null) {
      boolean boundsCheckingEnabled = getFlagValue(classLoader);
      Assert.assertTrue(boundsCheckingEnabled);
    }
  }

  /**
   * Test setting the bounds checking flag by the old property.
   * @throws Exception if loading class {@link BoundsChecking#BOUNDS_CHECKING_ENABLED} fails.
   */
  @Test
  public void testEnableOldProperty() throws Exception {
    String savedOldProperty = System.getProperty("drill.enable_unsafe_memory_access");
    System.setProperty("drill.enable_unsafe_memory_access", "true");

    ClassLoader classLoader = copyClassLoader();
    if (classLoader != null) {
      boolean boundsCheckingEnabled = getFlagValue(classLoader);
      Assert.assertFalse(boundsCheckingEnabled);
    }

    // restore system property
    if (savedOldProperty != null) {
      System.setProperty("drill.enable_unsafe_memory_access", savedOldProperty);
    } else {
      System.clearProperty("drill.enable_unsafe_memory_access");
    }
  }

  /**
   * Test setting the bounds checking flag by the new property.
   * @throws Exception if loading class {@link BoundsChecking#BOUNDS_CHECKING_ENABLED} fails.
   */
  @Test
  public void testEnableNewProperty() throws Exception {
    String savedNewProperty = System.getProperty("arrow.enable_unsafe_memory_access");

    System.setProperty("arrow.enable_unsafe_memory_access", "true");

    ClassLoader classLoader = copyClassLoader();
    if (classLoader != null) {
      boolean boundsCheckingEnabled = getFlagValue(classLoader);
      Assert.assertFalse(boundsCheckingEnabled);
    }

    // restore system property
    if (savedNewProperty != null) {
      System.setProperty("arrow.enable_unsafe_memory_access", savedNewProperty);
    } else {
      System.clearProperty("arrow.enable_unsafe_memory_access");
    }
  }

  /**
   * Test setting the bounds checking flag by both old and new properties.
   * In this case, the new property should take precedence.
   * @throws Exception if loading class {@link BoundsChecking#BOUNDS_CHECKING_ENABLED} fails.
   */
  @Test
  public void testEnableBothProperties() throws Exception {
    String savedOldProperty = System.getProperty("drill.enable_unsafe_memory_access");
    String savedNewProperty = System.getProperty("arrow.enable_unsafe_memory_access");

    System.setProperty("drill.enable_unsafe_memory_access", "false");
    System.setProperty("arrow.enable_unsafe_memory_access", "true");

    // new property takes precedence.
    ClassLoader classLoader = copyClassLoader();
    if (classLoader != null) {
      boolean boundsCheckingEnabled = getFlagValue(classLoader);
      Assert.assertFalse(boundsCheckingEnabled);
    }

    // restore system property
    if (savedOldProperty != null) {
      System.setProperty("drill.enable_unsafe_memory_access", savedOldProperty);
    } else {
      System.clearProperty("drill.enable_unsafe_memory_access");
    }

    if (savedNewProperty != null) {
      System.setProperty("arrow.enable_unsafe_memory_access", savedNewProperty);
    } else {
      System.clearProperty("arrow.enable_unsafe_memory_access");
    }
  }
}
