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
import java.util.Arrays;

import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
/**
 * This is a Helper class which has functionalities to read and assert the values from the given FieldVector object
 *
 */
public class JdbcToArrowTestHelper {

    public static void assertIntVectorValues(FieldVector fx, int rowCount, int[] values) {
        IntVector intVector = ((IntVector) fx);
        assertEquals(rowCount, intVector.getValueCount());

        for(int j = 0; j < intVector.getValueCount(); j++) {
            //assertNotNull(intVector.get(j));
            assertEquals(values[j], intVector.get(j));
        }
    }

    public static void assertBitBooleanVectorValues(FieldVector fx, int rowCount, int[] values){
        BitVector bitVector = ((BitVector)fx);
        assertEquals(rowCount, bitVector.getValueCount());
        
        for(int j = 0; j < bitVector.getValueCount(); j++){
            assertEquals(values[j], bitVector.get(j));
        }
    }

    public static void assertTinyIntVectorValues(FieldVector fx, int rowCount, int[] values){
        TinyIntVector tinyIntVector = ((TinyIntVector)fx);
        assertEquals(rowCount, tinyIntVector.getValueCount());

        for(int j = 0; j < tinyIntVector.getValueCount(); j++){
            assertEquals(values[j], tinyIntVector.get(j));
        }
    }

    public static void assertSmallIntVectorValues(FieldVector fx, int rowCount, int[] values){
        SmallIntVector smallIntVector = ((SmallIntVector)fx);
        assertEquals(rowCount, smallIntVector.getValueCount());

        for(int j = 0; j < smallIntVector.getValueCount(); j++){
            assertEquals(values[j], smallIntVector.get(j));
        }
    }

    public static void assertBigIntVectorValues(FieldVector fx, int rowCount, int[] values){
        BigIntVector bigIntVector = ((BigIntVector)fx);
        assertEquals(rowCount, bigIntVector.getValueCount());

        for(int j = 0; j < bigIntVector.getValueCount(); j++){
            assertEquals(values[j], bigIntVector.get(j));
        }
    }

    public static void assertDecimalVectorValues(FieldVector fx, int rowCount, BigDecimal[] values){
        DecimalVector decimalVector = ((DecimalVector)fx);
        assertEquals(rowCount, decimalVector.getValueCount());

        for(int j = 0; j < decimalVector.getValueCount(); j++){
        	assertNotNull(decimalVector.getObject(j));
            assertEquals(values[j].doubleValue(), decimalVector.getObject(j).doubleValue(), 0);
        }
    }

    public static void assertFloat8VectorValues(FieldVector fx, int rowCount, double[] values){
        Float8Vector float8Vector = ((Float8Vector)fx);
        assertEquals(rowCount, float8Vector.getValueCount());

        for(int j = 0; j < float8Vector.getValueCount(); j++){
            assertEquals(values[j], float8Vector.get(j), 0.01);
        }
    }

    public static void assertFloat4VectorValues(FieldVector fx, int rowCount, float[] values){
        Float4Vector float4Vector = ((Float4Vector)fx);
        assertEquals(rowCount, float4Vector.getValueCount());

        for(int j = 0; j < float4Vector.getValueCount(); j++){
            assertEquals(values[j], float4Vector.get(j), 0.01);
        }
    }

    public static void assertTimeVectorValues(FieldVector fx, int rowCount, long[] values){
        TimeMilliVector timeMilliVector = ((TimeMilliVector)fx);
        assertEquals(rowCount, timeMilliVector.getValueCount());

        for(int j = 0; j < timeMilliVector.getValueCount(); j++){
                assertEquals(values[j], timeMilliVector.get(j));
        }
    }

    public static void assertDateVectorValues(FieldVector fx, int rowCount, long[] values){
        DateMilliVector dateMilliVector = ((DateMilliVector)fx);
        assertEquals(rowCount, dateMilliVector.getValueCount());

        for(int j = 0; j < dateMilliVector.getValueCount(); j++){
            assertEquals(values[j], dateMilliVector.get(j));
        }
    }

    public static void assertTimeStampVectorValues(FieldVector fx, int rowCount, long[] values){
        TimeStampVector timeStampVector = ((TimeStampVector)fx);
        assertEquals(rowCount, timeStampVector.getValueCount());

        for(int j = 0; j < timeStampVector.getValueCount(); j++){
            assertEquals(values[j], timeStampVector.get(j));
        }
    }

    public static void assertVarBinaryVectorValues (FieldVector fx, int rowCount, byte[][] values) {
    	try {
	    	VarBinaryVector varBinaryVector =((VarBinaryVector) fx);
	        assertEquals(rowCount, varBinaryVector.getValueCount());
	
	        for(int j = 0; j < varBinaryVector.getValueCount(); j++){
	            assertEquals(Arrays.hashCode(values[j]), Arrays.hashCode(varBinaryVector.get(j)));
	        }
        } catch (AssertionError ae) {
        	ae.printStackTrace();
        }
    }

    public static void assertVarcharVectorValues(FieldVector fx, int rowCount, byte[][] values) {
        try {
	    	VarCharVector varCharVector = ((VarCharVector)fx);
	        assertEquals(rowCount, varCharVector.getValueCount());
	
	        for(int j = 0; j < varCharVector.getValueCount(); j++){
	            assertEquals(Arrays.hashCode(values[j]), Arrays.hashCode(varCharVector.get(j)));
	        }
        } catch (AssertionError ae) {
        	ae.printStackTrace();
        } 
    }

    public static long hashArray(byte[] data){
        long ret = 0;
        for(int i = 0; i < data.length;i++)
            ret+=data[i];
        return ret;
    }

    public static String firstX(byte[] data, int items){
        int toProcess = Math.min(items, data.length);
        StringBuilder sb = new StringBuilder();
        for(int i = 0; i < toProcess; i++) {
            sb.append(String.format("0x%02x", data[i])+ " ");
        }
        return sb.toString();
    }

    public static byte[] hexStringToByteArray(String s) {
        int len = s.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4)
                    + Character.digit(s.charAt(i+1), 16));
        }
        return data;
    }

}
