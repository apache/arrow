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
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.StandardCopyOption;
import java.util.UUID;

public class OrcJniUtils {
  private static final String LIBRARY_NAME = "arrow_orc_jni";
  private static boolean isLoaded = false;

  public static void loadOrcAdapterLibraryFromJar()
          throws IOException {
    synchronized (OrcJniUtils.class) {
      if (!isLoaded) {
        final String libraryToLoad = System.mapLibraryName(LIBRARY_NAME);
        final File libraryFile = moveFileFromJarToTemp(
                System.getProperty("java.io.tmpdir"), libraryToLoad);
        System.load(libraryFile.getAbsolutePath());
        isLoaded = true;
      }
    }
  }

  private static File moveFileFromJarToTemp(final String tmpDir, String libraryToLoad)
          throws IOException {
    final File temp = setupFile(tmpDir, libraryToLoad);
    try (final InputStream is = OrcReaderJniWrapper.class.getClassLoader()
            .getResourceAsStream(libraryToLoad)) {
      if (is == null) {
        throw new InvalidPathException(libraryToLoad,  "file was not found inside JAR.");
      } else {
        Files.copy(is, temp.toPath(), StandardCopyOption.REPLACE_EXISTING);
      }
    }
    return temp;
  }

  private static File setupFile(String tmpDir, String libraryToLoad)
          throws IOException {
    final String randomizeFileName = libraryToLoad + UUID.randomUUID();
    final File temp = new File(tmpDir, randomizeFileName);
    if (temp.exists() && !temp.delete()) {
      throw new InvalidPathException(
              temp.getAbsolutePath(), "File already exists and cannot be removed.");
    }
    if (!temp.createNewFile()) {
      throw new InvalidPathException(temp.getAbsolutePath(), "File could not be created.");
    }
    temp.deleteOnExit();
    return temp;
  }
}
