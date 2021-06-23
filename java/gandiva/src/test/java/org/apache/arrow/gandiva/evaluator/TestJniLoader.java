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

import org.junit.Assert;
import org.junit.Test;

public class TestJniLoader {

  @Test
  public void testDefaultConfiguration() throws Exception {
    long configId = JniLoader.getConfiguration(ConfigurationBuilder.ConfigOptions.getDefault());
    Assert.assertEquals(configId, JniLoader.getDefaultConfiguration());
    Assert.assertEquals(configId, JniLoader.getConfiguration(ConfigurationBuilder.ConfigOptions.getDefault()));

    long configId2 = JniLoader.getConfiguration(new ConfigurationBuilder.ConfigOptions().withOptimize(false));
    long configId3 = JniLoader.getConfiguration(new ConfigurationBuilder.ConfigOptions().withTargetCPU(false));
    long configId4 = JniLoader.getConfiguration(new ConfigurationBuilder.ConfigOptions().withOptimize(false)
        .withTargetCPU(false));

    Assert.assertTrue(configId != configId2 && configId2 != configId3 && configId3 != configId4);

    Assert.assertEquals(configId2, JniLoader.getConfiguration(new ConfigurationBuilder.ConfigOptions()
        .withOptimize(false)));
    Assert.assertEquals(configId3, JniLoader.getConfiguration(new ConfigurationBuilder.ConfigOptions()
        .withTargetCPU(false)));
    Assert.assertEquals(configId4, JniLoader.getConfiguration(new ConfigurationBuilder.ConfigOptions()
        .withOptimize(false).withTargetCPU(false)));

    JniLoader.removeConfiguration(new ConfigurationBuilder.ConfigOptions().withOptimize(false));
    // configids are monotonically updated. after a config is removed, new one is assigned with higher id
    Assert.assertNotEquals(configId2, JniLoader.getConfiguration(new ConfigurationBuilder.ConfigOptions()
        .withOptimize(false)));

    JniLoader.removeConfiguration(new ConfigurationBuilder.ConfigOptions());
    Assert.assertNotEquals(configId, JniLoader.getConfiguration(ConfigurationBuilder.ConfigOptions.getDefault()));
  }
}
