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

package org.apache.arrow.adapter.orc;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Locale;

/**
 * Helper class for JNI related operations.
 */
class OrcJniUtils {
  private static final String LIBRARY_NAME = "arrow_orc_jni";
  private static boolean isLoaded = false;

  private OrcJniUtils() {}

  static void loadOrcAdapterLibraryFromJar()
          throws IOException, IllegalAccessException {
    synchronized (OrcJniUtils.class) {
      if (!isLoaded) {
        final String libraryToLoad =
            getNormalizedArch() + File.separator + System.mapLibraryName(LIBRARY_NAME);
        final File libraryFile =
            moveFileFromJarToTemp(System.getProperty("java.io.tmpdir"), libraryToLoad, LIBRARY_NAME);
        System.load(libraryFile.getAbsolutePath());
        isLoaded = true;
      }
    }
  }

  private static String getNormalizedArch() {
    String arch = System.getProperty("os.arch").toLowerCase(Locale.US);
    switch (arch) {
      case "amd64":
        arch = "x86_64";
        break;
      case "aarch64":
        arch = "aarch_64";
        break;
      default:
        break;
    }
    return arch;
  }

  private static File moveFileFromJarToTemp(final String tmpDir, String libraryToLoad, String libraryName)
          throws IOException {
    final File temp = File.createTempFile(tmpDir, libraryName);
    try (final InputStream is = OrcReaderJniWrapper.class.getClassLoader()
            .getResourceAsStream(libraryToLoad)) {
      if (is == null) {
        throw new FileNotFoundException(libraryToLoad);
      } else {
        Files.copy(is, temp.toPath(), StandardCopyOption.REPLACE_EXISTING);
      }
    }
    return temp;
  }
}
