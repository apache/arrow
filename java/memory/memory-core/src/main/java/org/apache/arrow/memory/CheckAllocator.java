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

import java.io.IOException;
import java.net.URL;
import java.util.Enumeration;
import java.util.LinkedHashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Static method to ensure we have a RootAllocator on the classpath and report which one is used.
 */
final class CheckAllocator {
  private static final Logger logger = LoggerFactory.getLogger(CheckAllocator.class);
  private static final String ALLOCATOR_PATH = "org/apache/arrow/memory/DefaultAllocationManagerFactory.class";

  private CheckAllocator() {

  }

  static String check() {
    Set<URL> urls = scanClasspath();
    URL rootAllocator = assertOnlyOne(urls);
    reportResult(rootAllocator);
    return "org.apache.arrow.memory.DefaultAllocationManagerFactory";
  }


  private static Set<URL> scanClasspath() {
    // LinkedHashSet appropriate here because it preserves insertion order
    // during iteration
    Set<URL> allocatorPathSet = new LinkedHashSet<>();
    try {
      ClassLoader allocatorClassLoader = CheckAllocator.class.getClassLoader();
      Enumeration<URL> paths;
      if (allocatorClassLoader == null) {
        paths = ClassLoader.getSystemResources(ALLOCATOR_PATH);
      } else {
        paths = allocatorClassLoader.getResources(ALLOCATOR_PATH);
      }
      while (paths.hasMoreElements()) {
        URL path = paths.nextElement();
        allocatorPathSet.add(path);
      }
    } catch (IOException ioe) {
      logger.error("Error getting resources from path", ioe);
    }
    return allocatorPathSet;
  }

  private static void reportResult(URL rootAllocator) {
    String path = rootAllocator.getPath();
    String subPath = path.substring(path.indexOf("memory"));
    logger.info("Using DefaultAllocationManager at {}", subPath);
  }

  private static URL assertOnlyOne(Set<URL> urls) {
    if (urls.size() > 1) {
      logger.warn("More than one DefaultAllocationManager on classpath. Choosing first found");
    }
    if (urls.isEmpty()) {
      throw new RuntimeException("No DefaultAllocationManager found on classpath. Can't allocate Arrow buffers." +
          " Please consider adding arrow-memory-netty or arrow-memory-unsafe as a dependency.");
    }
    return urls.iterator().next();
  }

}
