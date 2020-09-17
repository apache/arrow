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

package org.apache.arrow.memory.util;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URLClassLoader;

import org.junit.Assert;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

public class TestLargeMemoryUtil {

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
   * Use the checkedCastToInt method from the current classloader.
   * @param classLoader the class loader from which to call the method.
   * @return the return value of the method.
   */
  private int checkedCastToInt(ClassLoader classLoader, long value) throws Exception {
    Class<?> clazz = classLoader.loadClass("org.apache.arrow.memory.util.LargeMemoryUtil");
    Method method = clazz.getMethod("checkedCastToInt", long.class);
    return (int) method.invoke(null, value);
  }

  private void checkExpectedOverflow(ClassLoader classLoader, long value) {
    InvocationTargetException ex = Assertions.assertThrows(InvocationTargetException.class, () -> {
      checkedCastToInt(classLoader, value);
    });
    Assert.assertTrue(ex.getCause() instanceof ArithmeticException);
    Assert.assertEquals("integer overflow", ex.getCause().getMessage());
  }

  @Test
  public void testEnableLargeMemoryUtilCheck() throws Exception {
    String savedNewProperty = System.getProperty("arrow.enable_unsafe_memory_access");
    System.setProperty("arrow.enable_unsafe_memory_access", "false");
    try {
      ClassLoader classLoader = copyClassLoader();
      if (classLoader != null) {
        Assert.assertEquals(Integer.MAX_VALUE, checkedCastToInt(classLoader, Integer.MAX_VALUE));
        checkExpectedOverflow(classLoader, Integer.MAX_VALUE + 1L);
        checkExpectedOverflow(classLoader, Integer.MIN_VALUE - 1L);
      }
    } finally {
      // restore system property
      if (savedNewProperty != null) {
        System.setProperty("arrow.enable_unsafe_memory_access", savedNewProperty);
      } else {
        System.clearProperty("arrow.enable_unsafe_memory_access");
      }
    }
  }

  @Test
  public void testDisabledLargeMemoryUtilCheck() throws Exception {
    String savedNewProperty = System.getProperty("arrow.enable_unsafe_memory_access");
    System.setProperty("arrow.enable_unsafe_memory_access", "true");
    try {
      ClassLoader classLoader = copyClassLoader();
      if (classLoader != null) {
        Assert.assertEquals(Integer.MAX_VALUE, checkedCastToInt(classLoader, Integer.MAX_VALUE));
        Assert.assertEquals(Integer.MIN_VALUE, checkedCastToInt(classLoader, Integer.MAX_VALUE + 1L));
        Assert.assertEquals(Integer.MAX_VALUE, checkedCastToInt(classLoader, Integer.MIN_VALUE - 1L));
      }
    } finally {
      // restore system property
      if (savedNewProperty != null) {
        System.setProperty("arrow.enable_unsafe_memory_access", savedNewProperty);
      } else {
        System.clearProperty("arrow.enable_unsafe_memory_access");
      }
    }
  }
}
