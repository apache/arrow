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

import static org.junit.Assert.*;


/**
 * This is a Helper class which has functionalities to read and print the values from FieldVector object
 *
 */
public class JdbcToArrowTestHelper {

    public static boolean assertIntVectorValues(FieldVector fx, int rowCount, int[] values) {
        IntVector intVector = ((IntVector) fx);

        assertEquals(rowCount, intVector.getValueCount());

        for(int j = 0; j < intVector.getValueCount(); j++) {
            if(!intVector.isNull(j)) {
                assertEquals(values[j], intVector.get(j));
            }
        }
        return true;
    }

    public void getBigIntVectorValues(FieldVector fx){
        BigIntVector bigIntVector = ((BigIntVector)fx);
        for(int j = 0; j < bigIntVector.getValueCount(); j++){
            if(!bigIntVector.isNull(j)){
                long value = bigIntVector.get(j);
                System.out.println("Big Int Vector Value [" + j +"] : " + value);
            } else {
                System.out.println("Big Int Vector Value [" + j +"] : NULL ");
            }
        }
    }

    public void getVarBinaryVectorValues(FieldVector fx){
        VarBinaryVector varBinaryVector =((VarBinaryVector) fx);
        for(int j = 0; j < varBinaryVector.getValueCount(); j++){
            if(!varBinaryVector.isNull(j)){
                byte[] value = varBinaryVector.get(j);
                long valHash = hashArray(value);
                System.out.println("Var Binary Vector Value [" + j +"] : " + firstX(value, 5));
            } else {
                System.out.println("Var Binary Vector Value [" + j +"] : NULL ");
            }
        }
    }

    public void getFloat4VectorValues(FieldVector fx){
        Float4Vector float4Vector = ((Float4Vector)fx);
        for(int j = 0; j < float4Vector.getValueCount(); j++){
            if(!float4Vector.isNull(j)){
                float value = float4Vector.get(j);
                System.out.println("Float Vector Value [" + j +"] : " + value);
            } else {
                System.out.println("Float Vector Value [" + j +"] : NULL ");
            }
        }
    }

    public void getFloat8VectorValues(FieldVector fx){
        Float8Vector float8Vector = ((Float8Vector)fx);
        for(int j = 0; j < float8Vector.getValueCount(); j++){
            if(!float8Vector.isNull(j)){
                double value = float8Vector.get(j);
                System.out.println("Double Vector Value [" + j +"] : " + value);
            } else {
                System.out.println("Double Vector Value [" + j +"] : NULL ");
            }
        }
    }

    public void getBitBooleanVectorValues(FieldVector fx){
        BitVector bitVector = ((BitVector)fx);
        for(int j = 0; j < bitVector.getValueCount(); j++){
            if(!bitVector.isNull(j)){
                int value = bitVector.get(j);
                System.out.println("Bit Boolean Vector Value[" + j +"] : " + value);
            } else {
                System.out.println("Bit Boolean Vector Value[" + j +"] : NULL ");
            }
        }
    }

    public void getTinyIntVectorValues(FieldVector fx){
        TinyIntVector tinyIntVector = ((TinyIntVector)fx);
        for(int j = 0; j < tinyIntVector.getValueCount(); j++){
            if(!tinyIntVector.isNull(j)){
                byte value = tinyIntVector.get(j);
                System.out.println("Tiny Int Vector Value[" + j +"] : " + value);
            } else {
                System.out.println("Tiny Int Vector Value[" + j +"] : NULL ");
            }
        }
    }

    public void getSmallIntVectorValues(FieldVector fx){
        SmallIntVector smallIntVector = ((SmallIntVector)fx);
        for(int j = 0; j < smallIntVector.getValueCount(); j++){
            if(!smallIntVector.isNull(j)){
                short value = smallIntVector.get(j);
                System.out.println("Small Int Vector Value[" + j +"] : " + value);
            } else {
                System.out.println("Small Int Vector Value[" + j +"] : NULL ");
            }
        }
    }

    public void getDecimalVectorValues(FieldVector fx){
        DecimalVector decimalVector = ((DecimalVector)fx);
        for(int j = 0; j < decimalVector.getValueCount(); j++){
            if(!decimalVector.isNull(j)){
                BigDecimal value = decimalVector.getObject(j);
                System.out.println("Decimal Vector Value[" + j +"] : " + value);
            } else {
                System.out.println("Decimal Vector Value[" + j +"] : NULL ");
            }
        }
    }

    public void getDateVectorValues(FieldVector fx){
        DateMilliVector dateMilliVector = ((DateMilliVector)fx);
        for(int j = 0; j < dateMilliVector.getValueCount(); j++){
            if(!dateMilliVector.isNull(j)){
                long value = dateMilliVector.get(j);
                System.out.println("Date Milli Vector Value[" + j +"] : " + value);
            } else {
                System.out.println("Date Milli Vector Value[" + j +"] : NULL ");
            }
        }
    }

    public void getTimeVectorValues(FieldVector fx){
        TimeMilliVector timeMilliVector = ((TimeMilliVector)fx);
        for(int j = 0; j < timeMilliVector.getValueCount(); j++){
            if(!timeMilliVector.isNull(j)){
                int value = timeMilliVector.get(j);
                System.out.println("Time Milli Vector Value[" + j +"] : " + value);
            } else {
                System.out.println("Time Milli Vector Value[" + j +"] : NULL ");
            }
        }
    }

    public void getTimeStampVectorValues(FieldVector fx){
        TimeStampVector timeStampVector = ((TimeStampVector)fx);
        for(int j = 0; j < timeStampVector.getValueCount(); j++){
            if(!timeStampVector.isNull(j)){
                long value = timeStampVector.get(j);
                System.out.println("Time Stamp Vector Value [" + j +"] : " + value);
            } else {
                System.out.println("Time Stamp Vector Value [" + j +"] : NULL ");
            }
        }
    }

    public void getVarcharVectorValues(FieldVector fx){
        VarCharVector varCharVector = ((VarCharVector)fx);
        for(int j = 0; j < varCharVector.getValueCount(); j++){
            if(!varCharVector.isNull(j)){
                byte[] valArr = varCharVector.get(j);
                //Text value = varCharVector.getObject(j);
                //System.out.println("Var Char Vector Value [" + j +"] : " + value.toString());
                System.out.println("Var Char Accessor as byte[" + j +"] " + firstX(valArr, 5));
            } else {
                System.out.println("\t\t varCharAccessor[" + j +"] : NULL ");
            }
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

}
