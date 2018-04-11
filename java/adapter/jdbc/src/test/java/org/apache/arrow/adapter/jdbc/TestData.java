package org.apache.arrow.adapter.jdbc;

import java.math.BigDecimal;

public class TestData {
	
	public int [] intValues;
	public int [] boolValues;
	public BigDecimal [] decimalValues;
	public double [] doubleValues;
	  
	public int[] getIntValues() {
		return intValues;
	}
	public void setIntValues(int[] intValues) {
		this.intValues = intValues;
	}
	public int[] getBoolValues() {
		return boolValues;
	}
	public void setBoolValues(int[] boolValues) {
		this.boolValues = boolValues;
	}
	public BigDecimal[] getDecimalValues() {
		return decimalValues;
	}
	public void setDecimalValues(BigDecimal[] decimalValues) {
		this.decimalValues = decimalValues;
	}
	public double[] getDoubleValues() {
		return doubleValues;
	}
	public void setDoubleValues(double[] doubleValues) {
		this.doubleValues = doubleValues;
	}

}
