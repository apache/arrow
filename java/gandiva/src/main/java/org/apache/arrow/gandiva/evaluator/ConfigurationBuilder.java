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

import java.util.Objects;

/**
 * Used to construct gandiva configuration objects.
 */
public class ConfigurationBuilder {

  public long buildConfigInstance(ConfigOptions configOptions) {
    return buildConfigInstance(configOptions.optimize, configOptions.targetCPU);
  }

  private native long buildConfigInstance(boolean optimize, boolean detectHostCPU);

  public native void releaseConfigInstance(long configId);

  /**
   * ConfigOptions contains the configuration parameters to provide to gandiva.
   */
  public static class ConfigOptions {
    private boolean optimize = true;
    private boolean targetCPU = true;

    public static ConfigOptions getDefault() {
      return new ConfigOptions();
    }

    public ConfigOptions() {
    }

    public ConfigOptions withOptimize(boolean optimize) {
      this.optimize = optimize;
      return this;
    }

    public ConfigOptions withTargetCPU(boolean targetCPU) {
      this.targetCPU = targetCPU;
      return this;
    }

    @Override
    public int hashCode() {
      return Objects.hash(optimize, targetCPU);
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof ConfigOptions)) {
        return false;
      }
      return this.optimize == ((ConfigOptions) obj).optimize &&
              this.targetCPU == ((ConfigOptions) obj).targetCPU;
    }
  }
}
