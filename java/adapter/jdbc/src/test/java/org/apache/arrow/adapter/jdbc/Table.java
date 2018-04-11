/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.arrow.adapter.jdbc;

import java.math.BigDecimal;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * POJO to handle the YAML data from the test YAML file.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Table {

    private String name;
    private String create;
    private String[] data;
    private String query;
    private String drop;
    
	private int [] integers;
	private int [] booleans;
	private BigDecimal [] decimals;
	private double [] doubles;
	
	private String intQuery;
	private String booleanQuery;
	private String decimalQuery;
	private String doubleQuery;
    
    public Table() {
    }

	public String getName() {
        return name;
    }
    public void setName(String name) {
        this.name = name;
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
        
	public int[] getIntegers() {
		return integers;
	}
	public void setIntegers(int[] integers) {
		this.integers = integers;
	}
	public int[] getBooleans() {
		return booleans;
	}
	public void setBooleans(int[] booleans) {
		this.booleans = booleans;
	}
	public BigDecimal [] getDecimals() {
		return decimals;
	}
	public void setDecimals(BigDecimal [] decimals) {
		this.decimals = decimals;
	}
	public double[] getDoubles() {
		return doubles;
	}
	public void setDoubles(double[] doubles) {
		this.doubles = doubles;
	}
	public String getIntQuery() {
		return intQuery;
	}
	public void setIntQuery(String intQuery) {
		this.intQuery = intQuery;
	}
	public String getBooleanQuery() {
		return booleanQuery;
	}
	public void setBooleanQuery(String booleanQuery) {
		this.booleanQuery = booleanQuery;
	}
	public String getDecimalQuery() {
		return decimalQuery;
	}
	public void setDecimalQuery(String decimalQuery) {
		this.decimalQuery = decimalQuery;
	}
	public String getDoubleQuery() {
		return doubleQuery;
	}
	public void setDoubleQyery(String doubleQuery) {
		this.doubleQuery = doubleQuery;
	}
}
