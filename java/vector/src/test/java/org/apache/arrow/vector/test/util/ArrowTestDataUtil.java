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

package org.apache.arrow.vector.test.util;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;

/**
 * Utility methods and constants for working with the arrow-testing repo.
 */
public final class ArrowTestDataUtil {
  public static final String TEST_DATA_ENV_VAR = "ARROW_TEST_DATA";
  public static final String TEST_DATA_PROPERTY = "arrow.test.dataRoot";

  public static Path getTestDataRoot() {
    String path = System.getenv(TEST_DATA_ENV_VAR);
    if (path == null) {
      path = System.getProperty(TEST_DATA_PROPERTY);
    }
    return Paths.get(Objects.requireNonNull(path,
        String.format("Could not find test data path. Set the environment variable %s or the JVM property %s.",
            TEST_DATA_ENV_VAR, TEST_DATA_PROPERTY)));
  }

  private ArrowTestDataUtil() {
  }
}
