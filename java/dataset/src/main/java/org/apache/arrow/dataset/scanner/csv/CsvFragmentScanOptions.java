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
package org.apache.arrow.dataset.scanner.csv;

import java.io.Serializable;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.arrow.dataset.file.FileFormat;
import org.apache.arrow.dataset.scanner.FragmentScanOptions;

public class CsvFragmentScanOptions implements Serializable, FragmentScanOptions {
  private final CsvConvertOptions convertOptions;
  private final Map<String, String> readOptions;
  private final Map<String, String> parseOptions;

  /**
   * csv scan options, map to CPP struct CsvFragmentScanOptions.
   *
   * @param convertOptions same struct in CPP
   * @param readOptions same struct in CPP
   * @param parseOptions same struct in CPP
   */
  public CsvFragmentScanOptions(
      CsvConvertOptions convertOptions,
      Map<String, String> readOptions,
      Map<String, String> parseOptions) {
    this.convertOptions = convertOptions;
    this.readOptions = readOptions;
    this.parseOptions = parseOptions;
  }

  public String typeName() {
    return FileFormat.CSV.name().toLowerCase(Locale.ROOT);
  }

  /**
   * File format id.
   *
   * @return id
   */
  public int fileFormatId() {
    return FileFormat.CSV.id();
  }

  /**
   * Serialize this class to ByteBuffer and then called by jni call.
   *
   * @return DirectByteBuffer
   */
  public String[] serialize() {
    Map<String, String> options =
        Stream.concat(
                Stream.concat(readOptions.entrySet().stream(), parseOptions.entrySet().stream()),
                convertOptions.getConfigs().entrySet().stream())
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    if (convertOptions.getArrowSchema().isPresent()) {
      options.put(
          "column_types", Long.toString(convertOptions.getArrowSchema().get().memoryAddress()));
    }
    return serializeMap(options);
  }

  public static CsvFragmentScanOptions deserialize(String serialized) {
    throw new UnsupportedOperationException("Not implemented now");
  }

  public CsvConvertOptions getConvertOptions() {
    return convertOptions;
  }

  public Map<String, String> getReadOptions() {
    return readOptions;
  }

  public Map<String, String> getParseOptions() {
    return parseOptions;
  }
}
