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
package org.apache.arrow.gandiva.evaluator;

import static java.util.UUID.randomUUID;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Locale;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.arrow.gandiva.exceptions.GandivaException;

/** This class handles loading of the jni library, and acts as a bridge for the native functions. */
class JniLoader {
  private static final String LIBRARY_NAME = "gandiva_jni";

  private static volatile JniLoader INSTANCE;
  private static volatile long defaultConfiguration = 0L;
  private static final ConcurrentMap<ConfigurationBuilder.ConfigOptions, Long> configurationMap =
      new ConcurrentHashMap<>();

  private final JniWrapper wrapper;

  private JniLoader() {
    this.wrapper = new JniWrapper();
  }

  static JniLoader getInstance() throws GandivaException {
    if (INSTANCE == null) {
      synchronized (JniLoader.class) {
        if (INSTANCE == null) {
          INSTANCE = setupInstance();
        }
      }
    }
    return INSTANCE;
  }

  private static JniLoader setupInstance() throws GandivaException {
    try {
      String tempDir = System.getProperty("java.io.tmpdir");
      loadGandivaLibraryFromJar(tempDir);
      return new JniLoader();
    } catch (IOException ioException) {
      throw new GandivaException("unable to create native instance", ioException);
    }
  }

  private static void loadGandivaLibraryFromJar(final String tmpDir)
      throws IOException, GandivaException {
    final String libraryToLoad =
        LIBRARY_NAME + "/" + getNormalizedArch() + "/" + System.mapLibraryName(LIBRARY_NAME);
    final File libraryFile = moveFileFromJarToTemp(tmpDir, libraryToLoad, LIBRARY_NAME);
    System.load(libraryFile.getAbsolutePath());
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

  private static File moveFileFromJarToTemp(
      final String tmpDir, String libraryToLoad, String libraryName)
      throws IOException, GandivaException {
    final File temp = setupFile(tmpDir, libraryName);
    try (final InputStream is =
        JniLoader.class.getClassLoader().getResourceAsStream(libraryToLoad)) {
      if (is == null) {
        throw new GandivaException(libraryToLoad + " was not found inside JAR.");
      } else {
        Files.copy(is, temp.toPath(), StandardCopyOption.REPLACE_EXISTING);
      }
    }
    return temp;
  }

  private static File setupFile(String tmpDir, String libraryToLoad)
      throws IOException, GandivaException {
    // accommodate multiple processes running with gandiva jar.
    // length should be ok since uuid is only 36 characters.
    final String randomizeFileName = libraryToLoad + randomUUID();
    final File temp = new File(tmpDir, randomizeFileName);
    if (temp.exists() && !temp.delete()) {
      throw new GandivaException(
          "File: " + temp.getAbsolutePath() + " already exists and cannot be removed.");
    }
    if (!temp.createNewFile()) {
      throw new GandivaException("File: " + temp.getAbsolutePath() + " could not be created.");
    }
    temp.deleteOnExit();
    return temp;
  }

  /** Returns the jni wrapper. */
  JniWrapper getWrapper() throws GandivaException {
    return wrapper;
  }

  static long getConfiguration(ConfigurationBuilder.ConfigOptions configOptions)
      throws GandivaException {
    if (!configurationMap.containsKey(configOptions)) {
      synchronized (ConfigurationBuilder.class) {
        if (!configurationMap.containsKey(configOptions)) {
          JniLoader.getInstance(); // setup
          long configInstance = new ConfigurationBuilder().buildConfigInstance(configOptions);
          configurationMap.put(configOptions, configInstance);
          if (ConfigurationBuilder.ConfigOptions.getDefault().equals(configOptions)) {
            defaultConfiguration = configInstance;
          }
          return configInstance;
        }
      }
    }
    return configurationMap.get(configOptions);
  }

  /**
   * Get the default configuration to invoke gandiva.
   *
   * @return default configuration
   * @throws GandivaException if unable to get native builder instance.
   */
  static long getDefaultConfiguration() throws GandivaException {
    if (defaultConfiguration == 0L) {
      synchronized (ConfigurationBuilder.class) {
        if (defaultConfiguration == 0L) {
          JniLoader.getInstance(); // setup
          ConfigurationBuilder.ConfigOptions defaultConfigOptions =
              ConfigurationBuilder.ConfigOptions.getDefault();
          defaultConfiguration =
              new ConfigurationBuilder().buildConfigInstance(defaultConfigOptions);
          configurationMap.put(defaultConfigOptions, defaultConfiguration);
        }
      }
    }
    return defaultConfiguration;
  }

  /** Remove the configuration. */
  static void removeConfiguration(ConfigurationBuilder.ConfigOptions configOptions) {
    if (configurationMap.containsKey(configOptions)) {
      synchronized (ConfigurationBuilder.class) {
        if (configurationMap.containsKey(configOptions)) {
          (new ConfigurationBuilder())
              .releaseConfigInstance(configurationMap.remove(configOptions));
          if (configOptions.equals(ConfigurationBuilder.ConfigOptions.getDefault())) {
            defaultConfiguration = 0;
          }
        }
      }
    }
  }
}
