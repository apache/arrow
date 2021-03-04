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

/**
 * A class for choosing the default allocation manager.
 */
public class DefaultAllocationManagerOption {

  /**
   * The environmental variable to set the default allocation manager type.
   */
  public static final String ALLOCATION_MANAGER_TYPE_ENV_NAME = "ARROW_ALLOCATION_MANAGER_TYPE";

  /**
   * The system property to set the default allocation manager type.
   */
  public static final String ALLOCATION_MANAGER_TYPE_PROPERTY_NAME = "arrow.allocation.manager.type";

  static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(DefaultAllocationManagerOption.class);

  /**
   * The default allocation manager factory.
   */
  private static AllocationManager.Factory DEFAULT_ALLOCATION_MANAGER_FACTORY = null;

  /**
   * The allocation manager type.
   */
  public enum AllocationManagerType {
    /**
     * Netty based allocation manager.
     */
    Netty,

    /**
     * Unsafe based allocation manager.
     */
    Unsafe,

    /**
     * Unknown type.
     */
    Unknown,
  }

  static AllocationManagerType getDefaultAllocationManagerType() {
    AllocationManagerType ret = AllocationManagerType.Unknown;

    try {
      String envValue = System.getenv(ALLOCATION_MANAGER_TYPE_ENV_NAME);
      ret = AllocationManagerType.valueOf(envValue);
    } catch (IllegalArgumentException | NullPointerException e) {
      // ignore the exception, and make the allocation manager type remain unchanged
    }

    // system property takes precedence
    try {
      String propValue = System.getProperty(ALLOCATION_MANAGER_TYPE_PROPERTY_NAME);
      ret = AllocationManagerType.valueOf(propValue);
    } catch (IllegalArgumentException | NullPointerException e) {
      // ignore the exception, and make the allocation manager type remain unchanged
    }
    return ret;
  }

  static AllocationManager.Factory getDefaultAllocationManagerFactory() {
    if (DEFAULT_ALLOCATION_MANAGER_FACTORY != null) {
      return DEFAULT_ALLOCATION_MANAGER_FACTORY;
    }
    AllocationManagerType type = getDefaultAllocationManagerType();
    switch (type) {
      case Netty:
        DEFAULT_ALLOCATION_MANAGER_FACTORY = getNettyFactory();
        break;
      case Unsafe:
        DEFAULT_ALLOCATION_MANAGER_FACTORY = getUnsafeFactory();
        break;
      case Unknown:
        LOGGER.info("allocation manager type not specified, using netty as the default type");
        DEFAULT_ALLOCATION_MANAGER_FACTORY = getFactory(CheckAllocator.check());
        break;
      default:
        throw new IllegalStateException("Unknown allocation manager type: " + type);
    }
    return DEFAULT_ALLOCATION_MANAGER_FACTORY;
  }

  private static AllocationManager.Factory getFactory(String clazzName) {
    try {
      Field field = Class.forName(clazzName).getDeclaredField("FACTORY");
      field.setAccessible(true);
      return (AllocationManager.Factory) field.get(null);
    } catch (Exception e) {
      throw new RuntimeException("Unable to instantiate Allocation Manager for " + clazzName, e);
    }
  }

  private static AllocationManager.Factory getUnsafeFactory() {
    try {
      return getFactory("org.apache.arrow.memory.UnsafeAllocationManager");
    } catch (RuntimeException e) {
      throw new RuntimeException("Please add arrow-memory-unsafe to your classpath," +
          " No DefaultAllocationManager found to instantiate an UnsafeAllocationManager", e);
    }
  }

  private static AllocationManager.Factory getNettyFactory() {
    try {
      return getFactory("org.apache.arrow.memory.NettyAllocationManager");
    } catch (RuntimeException e) {
      throw new RuntimeException("Please add arrow-memory-netty to your classpath," +
          " No DefaultAllocationManager found to instantiate an NettyAllocationManager", e);
    }
  }
}
