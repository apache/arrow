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

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.arrow.dataset.file.FileFormat;
import org.apache.arrow.dataset.scanner.FragmentScanOptions;
import org.apache.arrow.dataset.utils.MapUtil;

public class CsvFragmentScanOptions implements FragmentScanOptions {
  private final CsvConvertOptions convertOptions;
  private final Map<String, String> readOptions;
  private final Map<String, String> parseOptions;

  /**
   * CSV scan options, map to CPP struct CsvFragmentScanOptions. The key in config map is the field
   * name of mapping cpp struct
   *
   * @param convertOptions similar to CsvFragmentScanOptions#convert_options in CPP, the ArrowSchema
   *     represents column_types, convert data option such as null value recognition.
   * @param readOptions similar to CsvFragmentScanOptions#read_options in CPP, specify how to read
   *     the file such as block_size
   * @param parseOptions similar to CsvFragmentScanOptions#parse_options in CPP, parse file option
   *     such as delimiter
   */
  public CsvFragmentScanOptions(
      CsvConvertOptions convertOptions,
      Map<String, String> readOptions,
      Map<String, String> parseOptions) {
    this.convertOptions = convertOptions;
    this.readOptions = readOptions;
    this.parseOptions = parseOptions;
  }

  /**
   * File format.
   *
   * @return file format.
   */
  @Override
  public FileFormat fileFormat() {
    return FileFormat.CSV;
  }

  /**
   * This is an internal function to invoke by serializer. Serialize this class to string array and
   * then called by JNI call.
   *
   * @return string array as Map JNI bridge format.
   */
  @Override
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
    return MapUtil.convertMapToStringArray(options);
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
