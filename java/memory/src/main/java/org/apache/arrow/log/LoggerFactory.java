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

package org.apache.arrow.log;

/**
 * Used to create Arrow Logger instance.
 *
 */
public class LoggerFactory {
  /**
   * get Slf4j logger if the library exists, otherwise get the dummy one.
   * @param clazz Class type that the logger is instantiated
   * @return An ArrowLogger instance given the name of the class
   */
  public static Logger getLogger(Class<?> clazz) {
    // try to use slf4j but if failed, returns a JDK14Logger
    try {
      return new SLF4JLogger(clazz);
    } catch (Throwable ex) {
      return new JDK14Logger(clazz.getName());
    }
  }
}
