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

package org.apache.arrow.c.jni;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * The JniLoader for C Data Interface API's native implementation.
 */
public class JniLoader {
  private static final JniLoader INSTANCE = new JniLoader(Collections.singletonList("arrow_cdata_jni"));

  public static JniLoader get() {
    return INSTANCE;
  }

  private final Set<String> librariesToLoad;

  private JniLoader(List<String> libraryNames) {
    librariesToLoad = new HashSet<>(libraryNames);
  }

  private boolean finished() {
    return librariesToLoad.isEmpty();
  }

  /**
   * If required JNI libraries are not loaded, then load them.
   */
  public void ensureLoaded() {
    if (finished()) {
      return;
    }
    loadRemaining();
  }

  private synchronized void loadRemaining() {
    // The method is protected by a mutex via synchronized, if more than one thread
    // race to call
    // loadRemaining, at same time only one will do the actual loading and the
    // others will wait for
    // the mutex to be acquired then check on the remaining list: if there are
    // libraries that were not
    // successfully loaded then the mutex owner will try to load them again.
    if (finished()) {
      return;
    }
    List<String> libs = new ArrayList<>(librariesToLoad);
    for (String lib : libs) {
      load(lib);
      librariesToLoad.remove(lib);
    }
  }

  private void load(String name) {
    final String libraryToLoad =
        getNormalizedArch() + File.separator + System.mapLibraryName(name);
    try {
      File temp = File.createTempFile("jnilib-", ".tmp", new File(System.getProperty("java.io.tmpdir")));
      temp.deleteOnExit();
      try (final InputStream is = JniWrapper.class.getClassLoader().getResourceAsStream(libraryToLoad)) {
        if (is == null) {
          throw new FileNotFoundException(libraryToLoad);
        }
        Files.copy(is, temp.toPath(), StandardCopyOption.REPLACE_EXISTING);
        System.load(temp.getAbsolutePath());
      }
    } catch (IOException e) {
      throw new IllegalStateException("error loading native libraries: " + e);
    }
  }

  private String getNormalizedArch() {
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
}
