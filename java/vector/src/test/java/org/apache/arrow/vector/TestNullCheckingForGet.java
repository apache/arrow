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

package org.apache.arrow.vector;

import java.lang.reflect.Field;
import java.net.URLClassLoader;

import org.junit.Assert;
import org.junit.Test;

/**
 * Test cases for {@link NullCheckingForGet}.
 */
public class TestNullCheckingForGet {

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
   * Get the value of flag  {@link NullCheckingForGet#NULL_CHECKING_ENABLED}.
   * @param classLoader the class loader from which to get the flag value.
   * @return value of the flag.
   */
  private boolean getFlagValue(ClassLoader classLoader) throws Exception {
    Class<?> clazz = classLoader.loadClass("org.apache.arrow.vector.NullCheckingForGet");
    Field field = clazz.getField("NULL_CHECKING_ENABLED");
    return (Boolean) field.get(null);
  }

  /**
   * Ensure the flag for null checking is enabled by default.
   * This will protect users from JVM crashes.
   */
  @Test
  public void testDefaultValue() throws Exception {
    ClassLoader classLoader = copyClassLoader();
    if (classLoader != null) {
      boolean nullCheckingEnabled = getFlagValue(classLoader);
      Assert.assertTrue(nullCheckingEnabled);
    }
  }

  /**
   * Test setting the null checking flag by the system property.
   * @throws Exception if loading class {@link NullCheckingForGet#NULL_CHECKING_ENABLED} fails.
   */
  @Test
  public void testEnableSysProperty() throws Exception {
    String sysProperty = System.getProperty("arrow.enable_null_check_for_get");
    System.setProperty("arrow.enable_null_check_for_get", "false");

    ClassLoader classLoader = copyClassLoader();
    if (classLoader != null) {
      boolean nullCheckingEnabled = getFlagValue(classLoader);
      Assert.assertFalse(nullCheckingEnabled);
    }

    // restore system property
    if (sysProperty != null) {
      System.setProperty("arrow.enable_null_check_for_get", sysProperty);
    } else {
      System.clearProperty("arrow.enable_null_check_for_get");
    }
  }
}
