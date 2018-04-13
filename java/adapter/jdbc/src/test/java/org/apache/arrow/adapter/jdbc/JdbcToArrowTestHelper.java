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

import org.apache.arrow.vector.BaseFixedWidthVector;
import org.apache.arrow.vector.BaseValueVector;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.DecimalVector;
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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotNull;
/**
 * This is a Helper class which has functionalities to read and assert the values from the given FieldVector object
 *
 */
public class JdbcToArrowTestHelper {

    public static void assertIntVectorValues(IntVector intVector, int rowCount, int [] values) {
        assertEquals(rowCount, intVector.getValueCount());

        for(int j = 0; j < intVector.getValueCount(); j++) {
        	assertEquals(values[j], intVector.get(j));
        } 
    }
        
    public static void assertBitBooleanVectorValues(BitVector bitVector, int rowCount, int[] values){
        assertEquals(rowCount, bitVector.getValueCount());
        
        for(int j = 0; j < bitVector.getValueCount(); j++){
            assertEquals(values[j], bitVector.get(j));
        }
    }

    public static void assertTinyIntVectorValues(TinyIntVector tinyIntVector, int rowCount, int[] values){
        assertEquals(rowCount, tinyIntVector.getValueCount());

        for(int j = 0; j < tinyIntVector.getValueCount(); j++){
            assertEquals(values[j], tinyIntVector.get(j));
        }
    }

    public static void assertSmallIntVectorValues(SmallIntVector smallIntVector, int rowCount, int[] values){
        assertEquals(rowCount, smallIntVector.getValueCount());

        for(int j = 0; j < smallIntVector.getValueCount(); j++){
            assertEquals(values[j], smallIntVector.get(j));
        }
    }

    public static void assertBigIntVectorValues(BigIntVector bigIntVector, int rowCount, int[] values){
        assertEquals(rowCount, bigIntVector.getValueCount());

        for(int j = 0; j < bigIntVector.getValueCount(); j++){
            assertEquals(values[j], bigIntVector.get(j));
        }
    }

    public static void assertDecimalVectorValues(DecimalVector decimalVector, int rowCount, BigDecimal[] values){
        assertEquals(rowCount, decimalVector.getValueCount());

        for(int j = 0; j < decimalVector.getValueCount(); j++){
        	assertNotNull(decimalVector.getObject(j));
            assertEquals(values[j].doubleValue(), decimalVector.getObject(j).doubleValue(), 0);
        }
    }

    public static void assertFloat8VectorValues(Float8Vector float8Vector, int rowCount, double[] values){
        assertEquals(rowCount, float8Vector.getValueCount());

        for(int j = 0; j < float8Vector.getValueCount(); j++){
            assertEquals(values[j], float8Vector.get(j), 0.01);
        }
    }

    public static void assertFloat4VectorValues(Float4Vector float4Vector, int rowCount, float[] values){
        assertEquals(rowCount, float4Vector.getValueCount());

        for(int j = 0; j < float4Vector.getValueCount(); j++){
            assertEquals(values[j], float4Vector.get(j), 0.01);
        }
    }

    public static void assertTimeVectorValues(TimeMilliVector timeMilliVector, int rowCount, long[] values){
        assertEquals(rowCount, timeMilliVector.getValueCount());

        for(int j = 0; j < timeMilliVector.getValueCount(); j++){
                assertEquals(values[j], timeMilliVector.get(j));
        }
    }

    public static void assertDateVectorValues(DateMilliVector dateMilliVector, int rowCount, long[] values){
        assertEquals(rowCount, dateMilliVector.getValueCount());

        for(int j = 0; j < dateMilliVector.getValueCount(); j++){
            assertEquals(values[j], dateMilliVector.get(j));
        }
    }

    public static void assertTimeStampVectorValues(TimeStampVector timeStampVector, int rowCount, long[] values){
        assertEquals(rowCount, timeStampVector.getValueCount());

        for(int j = 0; j < timeStampVector.getValueCount(); j++){
            assertEquals(values[j], timeStampVector.get(j));
        }
    }

    public static void assertVarBinaryVectorValues (VarBinaryVector varBinaryVector, int rowCount, byte[][] values) {
    	try {
	        assertEquals(rowCount, varBinaryVector.getValueCount());
	
	        for(int j = 0; j < varBinaryVector.getValueCount(); j++){
	            assertEquals(Arrays.hashCode(values[j]), Arrays.hashCode(varBinaryVector.get(j)));
	        }
        } catch (AssertionError ae) {
        	ae.printStackTrace();
        }
    }

    public static void assertVarcharVectorValues(VarCharVector varCharVector, int rowCount, byte[][] values) {
        try {
	        assertEquals(rowCount, varCharVector.getValueCount());
	
	        for(int j = 0; j < varCharVector.getValueCount(); j++){
	            assertEquals(Arrays.hashCode(values[j]), Arrays.hashCode(varCharVector.get(j)));
	        }
        } catch (AssertionError ae) {
        	ae.printStackTrace();
        }
    }
    
    public static void assertNullValues(BaseValueVector vector, int rowCount) {
        assertEquals(rowCount, vector.getValueCount());

        for(int j = 0; j < vector.getValueCount(); j++) {
        	assertTrue(vector.isNull(j));
        } 
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
