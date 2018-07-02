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

import static java.util.UUID.randomUUID;

import org.apache.arrow.gandiva.exceptions.GandivaException;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

/**
 * This class is implemented in JNI. This provides the Java interface
 * to invoke functions in JNI
 */
class NativeBuilder {
  private static final String LIBRARY_NAME = "gandiva_jni";
  private static final String IRHELPERS_BC = "irhelpers.bc";

  private static volatile NativeBuilder INSTANCE;

  private final String byteCodeFilePath;

  private NativeBuilder(String byteCodeFilePath) {
    this.byteCodeFilePath = byteCodeFilePath;
  }

  static NativeBuilder getInstance() throws GandivaException {
    if (INSTANCE == null) {
      synchronized (NativeBuilder.class) {
        if (INSTANCE == null) {
          INSTANCE = setupInstance();
        }
      }
    }
    return INSTANCE;
  }

  private static NativeBuilder setupInstance() throws GandivaException {
    try {
      String tempDir = System.getProperty("java.io.tmpdir");
      loadGandivaLibraryFromJar(tempDir);
      File byteCodeFile = moveFileFromJarToTemp(tempDir, IRHELPERS_BC);
      return new NativeBuilder(byteCodeFile.getAbsolutePath());
    } catch (IOException ioException) {
      throw new GandivaException("unable to create native instance", ioException);
    }
  }

  private static void loadGandivaLibraryFromJar(final String tmpDir)
          throws IOException, GandivaException {
    final String libraryToLoad = System.mapLibraryName(LIBRARY_NAME);
    final File libraryFile = moveFileFromJarToTemp(tmpDir, libraryToLoad);
    System.load(libraryFile.getAbsolutePath());
  }


  private static File moveFileFromJarToTemp(final String tmpDir, String libraryToLoad)
          throws IOException, GandivaException {
    final File temp = setupFile(tmpDir, libraryToLoad);
    try (final InputStream is = NativeBuilder.class.getClassLoader()
            .getResourceAsStream(libraryToLoad)) {
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
      throw new GandivaException("File: " + temp.getAbsolutePath()
              + " already exists and cannot be removed.");
    }
    if (!temp.createNewFile()) {
      throw new GandivaException("File: " + temp.getAbsolutePath()
              + " could not be created.");
    }
    temp.deleteOnExit();
    return temp;
  }

  /**
   * Returns the byte code file path extracted from jar.
   */
  public String getByteCodeFilePath() {
    return byteCodeFilePath;
  }

  /**
   * Generates the LLVM module to evaluate the expressions with
   * custom configuration.
   *
   * @param schemaBuf   The schema serialized as a protobuf. See Types.proto
   *                    to see the protobuf specification
   * @param exprListBuf The serialized protobuf of the expression vector. Each
   *                    expression is created using TreeBuilder::MakeExpression
   * @param configId    Configuration to gandiva.
   * @return A moduleId that is passed to the evaluate() and close() methods
   */
  native long buildNativeCode(byte[] schemaBuf, byte[] exprListBuf,
                              long configId);

  /**
   * Evaluate the expressions represented by the moduleId on a record batch
   * and store the output in ValueVectors. Throws an exception in case of errors
   *
   * @param moduleId moduleId representing expressions. Created using a call to
   *                 buildNativeCode
   * @param numRows Number of rows in the record batch
   * @param bufAddrs An array of memory addresses. Each memory address points to
   *                 a validity vector or a data vector (will add support for offset
   *                 vectors later).
   * @param bufSizes An array of buffer sizes. For each memory address in bufAddrs,
   *                 the size of the buffer is present in bufSizes
   * @param outAddrs An array of output buffers, including the validity and data
   *                 addresses.
   * @param outSizes The allocated size of the output buffers. On successful evaluation,
   *                 the result is stored in the output buffers
   */
  native void evaluate(long moduleId, int numRows,
                              long[] bufAddrs, long[] bufSizes,
                              long[] outAddrs, long[] outSizes);

  /**
   * Closes the LLVM module referenced by moduleId.
   *
   * @param moduleId moduleId that needs to be closed
   */
  native void close(long moduleId);
}
