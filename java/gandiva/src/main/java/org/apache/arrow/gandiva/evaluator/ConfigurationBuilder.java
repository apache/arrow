/*
 * Copyright (C) 2017-2018 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.arrow.gandiva.evaluator;

import org.apache.arrow.gandiva.exceptions.GandivaException;

/**
 * Used to construct gandiva configuration objects.
 */
public class ConfigurationBuilder {

  private String byteCodeFilePath = "";

  private static volatile long defaultConfiguration = 0L;

  /**
   * Ctor - ensure that gandiva is loaded.
   * @throws GandivaException - if library cannot be loaded.
   */
  public ConfigurationBuilder() throws GandivaException {
    JniWrapper.getInstance();
  }

  public ConfigurationBuilder withByteCodeFilePath(final String byteCodeFilePath) {
    this.byteCodeFilePath = byteCodeFilePath;
    return this;
  }

  public String getByteCodeFilePath() {
    return byteCodeFilePath;
  }

  /**
   * Get the default configuration to invoke gandiva.
   * @return default configuration
   * @throws GandivaException if unable to get native builder instance.
   */
  static long getDefaultConfiguration() throws GandivaException {
    if (defaultConfiguration == 0L) {
      synchronized (ConfigurationBuilder.class) {
        if (defaultConfiguration == 0L) {
          ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
          String defaultFilePath = JniWrapper.getInstance().getByteCodeFilePath();
          configurationBuilder.withByteCodeFilePath(defaultFilePath);
          defaultConfiguration = configurationBuilder.buildConfigInstance();
        }
      }
    }
    return defaultConfiguration;
  }

  public native long buildConfigInstance();

  public native void releaseConfigInstance(long configId);
}
