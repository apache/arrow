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

package org.apache.arrow.adapter.jdbc;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * POJO to handle the YAML data from the test YAML file.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Table {
  private String name;
  private String type;
  private String vector;
  private String timezone;
  private String create;
  private String[] data;
  private String query;
  private String drop;
  private String[] values;
  private String[] vectors;
  private int rowCount;

  public Table() {
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public String getVector() {
    return vector;
  }

  public void setVector(String vector) {
    this.vector = vector;
  }

  public String[] getValues() {
    return values;
  }

  public void setValues(String[] values) {
    this.values = values;
  }

  public Long[] getLongValues() {
    Long[] arr = new Long[values.length];
    int i = 0;
    for (String str : values) {
      arr[i++] = Long.parseLong(str);
    }
    return arr;
  }

  public Integer[] getIntValues() {
    Integer[] arr = new Integer[values.length];
    int i = 0;
    for (String str : values) {
      arr[i++] = Integer.parseInt(str);
    }
    return arr;
  }

  public Boolean[] getBoolValues() {
    Boolean[] arr = new Boolean[values.length];
    int i = 0;
    for (String str : values) {
      arr[i++] = Boolean.parseBoolean(str);
    }
    return arr;
  }

  public BigDecimal[] getBigDecimalValues() {
    BigDecimal[] arr = new BigDecimal[values.length];
    int i = 0;
    for (String str : values) {
      arr[i++] = new BigDecimal(str);
    }
    return arr;
  }

  public Double[] getDoubleValues() {
    Double[] arr = new Double[values.length];
    int i = 0;
    for (String str : values) {
      arr[i++] = Double.parseDouble(str);
    }
    return arr;
  }

  public Float[] getFloatValues() {
    Float[] arr = new Float[values.length];
    int i = 0;
    for (String str : values) {
      arr[i++] = Float.parseFloat(str);
    }
    return arr;
  }

  public byte[][] getBinaryValues() {
    return getHexToByteArray(values);
  }

  public byte[][] getVarCharValues() {
    return getByteArray(values);
  }

  public byte[][] getBlobValues() {
    return getBinaryValues();
  }

  public byte[][] getClobValues() {
    return getByteArray(values);
  }

  public byte[][] getCharValues() {
    return getByteArray(values);
  }

  public String getCreate() {
    return create;
  }

  public void setCreate(String create) {
    this.create = create;
  }

  public String[] getData() {
    return data;
  }

  public void setData(String[] data) {
    this.data = data;
  }

  public String getQuery() {
    return query;
  }

  public void setQuery(String query) {
    this.query = query;
  }

  public String getDrop() {
    return drop;
  }

  public void setDrop(String drop) {
    this.drop = drop;
  }

  public String getTimezone() {
    return timezone;
  }

  public void setTimezone(String timezone) {
    this.timezone = timezone;
  }

  public String[] getVectors() {
    return vectors;
  }

  public void setVectors(String[] vectors) {
    this.vectors = vectors;
  }

  public int getRowCount() {
    return rowCount;
  }

  public void setRowCount(int rowCount) {
    this.rowCount = rowCount;
  }

  static byte[][] getByteArray(String[] data) {
    byte[][] byteArr = new byte[data.length][];

    for (int i = 0; i < data.length; i++) {
      byteArr[i] = data[i].getBytes(StandardCharsets.UTF_8);
    }
    return byteArr;
  }

  static byte[][] getHexToByteArray(String[] data) {
    byte[][] byteArr = new byte[data.length][];

    for (int i = 0; i < data.length; i++) {
      byteArr[i] = hexStringToByteArray(data[i]);
    }
    return byteArr;
  }

  static byte[] hexStringToByteArray(String s) {
    int len = s.length();
    byte[] data = new byte[len / 2];
    for (int i = 0; i < len; i += 2) {
      data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4) +
              Character.digit(s.charAt(i + 1), 16));
    }
    return data;
  }
}
