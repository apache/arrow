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
package org.apache.arrow.vector.ipc;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * Magic header/footer helpers for {@link ArrowFileWriter} and {@link ArrowFileReader} formatted
 * files.
 */
class ArrowMagic {
  private ArrowMagic() {}

  private static final byte[] MAGIC = "ARROW1".getBytes(StandardCharsets.UTF_8);

  public static final int MAGIC_LENGTH = MAGIC.length;

  public static void writeMagic(WriteChannel out, boolean align) throws IOException {
    out.write(MAGIC);
    if (align) {
      out.align();
    }
  }

  public static boolean validateMagic(byte[] array) {
    return Arrays.equals(MAGIC, array);
  }
}
