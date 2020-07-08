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

package org.apache.arrow.vector.types;

/**
 * Metadata version for Arrow metadata.
 */
public enum MetadataVersion {
  /// 0.1.0
  V1(org.apache.arrow.flatbuf.MetadataVersion.V1),

  /// 0.2.0
  V2(org.apache.arrow.flatbuf.MetadataVersion.V2),

  /// 0.3.0 to 0.7.1
  V3(org.apache.arrow.flatbuf.MetadataVersion.V3),

  /// 0.8.0 to 0.17.1
  V4(org.apache.arrow.flatbuf.MetadataVersion.V4),

  /// >= 1.0.0
  V5(org.apache.arrow.flatbuf.MetadataVersion.V5),

  ;

  public static final MetadataVersion DEFAULT = V5;

  private static final MetadataVersion[] valuesByFlatbufId =
      new MetadataVersion[MetadataVersion.values().length];

  static {
    for (MetadataVersion v : MetadataVersion.values()) {
      valuesByFlatbufId[v.flatbufID] = v;
    }
  }

  private final short flatbufID;

  MetadataVersion(short flatbufID) {
    this.flatbufID = flatbufID;
  }

  public short toFlatbufID() {
    return flatbufID;
  }

  public static MetadataVersion fromFlatbufID(short id) {
    return valuesByFlatbufId[id];
  }
}
