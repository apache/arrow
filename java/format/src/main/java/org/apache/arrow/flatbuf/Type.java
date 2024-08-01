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
package org.apache.arrow.flatbuf;

/**
 * ----------------------------------------------------------------------
 * Top-level Type value, enabling extensible type-specific metadata. We can
 * add new logical types to Type without breaking backwards compatibility
 */
@SuppressWarnings("unused")
public final class Type {
  private Type() { }
  public static final byte NONE = 0;
  public static final byte Null = 1;
  public static final byte Int = 2;
  public static final byte FloatingPoint = 3;
  public static final byte Binary = 4;
  public static final byte Utf8 = 5;
  public static final byte Bool = 6;
  public static final byte Decimal = 7;
  public static final byte Date = 8;
  public static final byte Time = 9;
  public static final byte Timestamp = 10;
  public static final byte Interval = 11;
  public static final byte List = 12;
  public static final byte Struct_ = 13;
  public static final byte Union = 14;
  public static final byte FixedSizeBinary = 15;
  public static final byte FixedSizeList = 16;
  public static final byte Map = 17;
  public static final byte Duration = 18;
  public static final byte LargeBinary = 19;
  public static final byte LargeUtf8 = 20;
  public static final byte LargeList = 21;
  public static final byte RunEndEncoded = 22;
  public static final byte BinaryView = 23;
  public static final byte Utf8View = 24;
  public static final byte ListView = 25;
  public static final byte LargeListView = 26;

  public static final String[] names = { "NONE", "Null", "Int", "FloatingPoint", "Binary", "Utf8", "Bool", "Decimal", "Date", "Time", "Timestamp", "Interval", "List", "Struct_", "Union", "FixedSizeBinary", "FixedSizeList", "Map", "Duration", "LargeBinary", "LargeUtf8", "LargeList", "RunEndEncoded", "BinaryView", "Utf8View", "ListView", "LargeListView", };

  public static String name(int e) { return names[e]; }
}

