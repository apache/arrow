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
package org.apache.arrow.compression;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.arrow.vector.compression.CompressionCodec;
import org.apache.arrow.vector.compression.CompressionUtil;
import org.apache.arrow.vector.compression.NoCompressionCodec;
import org.junit.jupiter.api.Test;

public class TestCompressionCodecServiceProvider {

  /**
   * When arrow-compression is in the classpath/module-path, {@link
   * CompressionCodec.Factory#INSTANCE} should be able to handle all codec types.
   */
  @Test
  public void testSupportedCompressionTypes() {
    assertThrows( // no-compression doesn't support any actual compression types
        IllegalArgumentException.class,
        () -> checkAllCodecTypes(NoCompressionCodec.Factory.INSTANCE));
    assertThrows( // commons-compression doesn't support the uncompressed type
        IllegalArgumentException.class,
        () -> checkAllCodecTypes(CommonsCompressionFactory.INSTANCE));
    checkAllCodecTypes( // and the winner is...
        CompressionCodec.Factory.INSTANCE); // combines the two above to support all types
  }

  private void checkAllCodecTypes(CompressionCodec.Factory factory) {
    for (CompressionUtil.CodecType codecType : CompressionUtil.CodecType.values()) {
      assertNotNull(factory.createCodec(codecType));
    }
  }
}
