%TSTRUCTARRAY Unit tests for arrow.array.StructArray

% Licensed to the Apache Software Foundation (ASF) under one or more
% contributor license agreements.  See the NOTICE file distributed with
% this work for additional information regarding copyright ownership.
% The ASF licenses this file to you under the Apache License, Version
% 2.0 (the "License"); you may not use this file except in compliance
% with the License.  You may obtain a copy of the License at
%
%   http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS,
% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
% implied.  See the License for the specific language governing
% permissions and limitations under the License.

classdef tStructArray < matlab.unittest.TestCase

    properties
        Float64Array = arrow.array([1 NaN 3 4 5]);
        StringArray = arrow.array(["A" "B" "C" "D" missing]);
    end

    methods (Test)
        function Basic(tc)
            import arrow.array.StructArray
            array = StructArray.fromArrays(tc.Float64Array, tc.StringArray);
            tc.verifyInstanceOf(array, "arrow.array.StructArray");
        end

        function FieldNames(tc)
            % Verify the FieldNames property is set to the expected value.
            import arrow.array.StructArray

            % Default field names used
            array = StructArray.fromArrays(tc.Float64Array, tc.StringArray);
            tc.verifyEqual(array.FieldNames, ["Field1", "Field2"]);
            
            % Field names provided
            array = StructArray.fromArrays(tc.Float64Array, tc.StringArray, FieldNames=["A", "B"]);
            tc.verifyEqual(array.FieldNames, ["A", "B"]);

            % Duplicate field names provided
            array = StructArray.fromArrays(tc.Float64Array, tc.StringArray, FieldNames=["C", "C"]);
            tc.verifyEqual(array.FieldNames, ["C", "C"]);
        end

        function FieldNamesError(tc)
            % Verify the FieldNames nv-pair errors when expected.
            import arrow.array.StructArray

            % Wrong type provided
            fcn = @() StructArray.fromArrays(tc.Float64Array, tc.StringArray, FieldNames={table table});
            tc.verifyError(fcn, "MATLAB:validation:UnableToConvert");
            
            % Wrong number of field names provided
            fcn = @() StructArray.fromArrays(tc.Float64Array, tc.StringArray, FieldNames="A");
            tc.verifyError(fcn, "arrow:tabular:WrongNumberColumnNames");

             % Missing string provided
            fcn = @() StructArray.fromArrays(tc.Float64Array, tc.StringArray, FieldNames=["A" missing]);
            tc.verifyError(fcn, "MATLAB:validators:mustBeNonmissing");
        end

        function FieldNamesNoSetter(tc)
            % Verify the FieldNames property is read-only. 
            import arrow.array.StructArray

            array = StructArray.fromArrays(tc.Float64Array, tc.StringArray, FieldNames=["X", "Y"]);
            fcn = @() setfield(array, "FieldNames", ["A", "B"]);
            tc.verifyError(fcn, "MATLAB:class:SetProhibited");
        end

        function NumFields(tc)
            % Verify the NumFields property is set to the expected value.
            import arrow.array.StructArray

            array = StructArray.fromArrays(tc.Float64Array, tc.StringArray);
            tc.verifyEqual(array.NumFields, int32(2));
        end

         function NumFieldsNoSetter(tc)
            % Verify the NumFields property is read-only.
            import arrow.array.StructArray

            array = StructArray.fromArrays(tc.Float64Array, tc.StringArray);
            fcn = @() setfield(array, "NumFields", 10);
            tc.verifyError(fcn, "MATLAB:class:SetProhibited");
         end

         function Valid(tc)
            % Verify the Valid property is set to the expected value.
            import arrow.array.StructArray

            array = StructArray.fromArrays(tc.Float64Array, tc.StringArray);
            expectedValid = true([5 1]);
            tc.verifyEqual(array.Valid, expectedValid);

            % Supply the Valid nv-pair
            valid = [true true false true false];
            array = StructArray.fromArrays(tc.Float64Array, tc.StringArray, Valid=valid);
            tc.verifyEqual(array.Valid, valid');
         end

         function ValidNVPairError(tc)
            % Verify the Valid nv-pair errors when expected.
            import arrow.array.StructArray

            % Provided an invalid index
            fcn = @() StructArray.fromArrays(tc.Float64Array, tc.StringArray, Valid=10);
            tc.verifyError(fcn, "MATLAB:notLessEqual");

            % Provided a logical vector with more elements than the array
            fcn = @() StructArray.fromArrays(tc.Float64Array, tc.StringArray, Valid=false([7 1]));
            tc.verifyError(fcn, "MATLAB:incorrectNumel");
         end

        function ValidNoSetter(tc)
            % Verify the Valid property is read-only.
            import arrow.array.StructArray

            array = StructArray.fromArrays(tc.Float64Array, tc.StringArray);
            fcn = @() setfield(array, "Valid", false);
            tc.verifyError(fcn, "MATLAB:class:SetProhibited");
        end

        function NumElements(tc)
            % Verify the NumElements property is set to the expected value.
            import arrow.array.StructArray

            array = StructArray.fromArrays(tc.Float64Array, tc.StringArray);
            tc.verifyEqual(array.NumElements, int64(5));
        end

        function NumElementsNoSetter(tc)
            % Verify the NumElements property is read-only.
            import arrow.array.StructArray

            array = StructArray.fromArrays(tc.Float64Array, tc.StringArray);
            fcn = @() setfield(array, "NumElements", 1);
            tc.verifyError(fcn, "MATLAB:class:SetProhibited");
        end

        function Type(tc)
            % Verify the Type property is set to the expected value.
            import arrow.array.StructArray

            array = StructArray.fromArrays(tc.Float64Array, tc.StringArray, FieldNames=["X", "Y"]);
            field1 = arrow.field("X", arrow.float64());
            field2 = arrow.field("Y", arrow.string());
            expectedType = arrow.struct(field1, field2);
            tc.verifyEqual(array.Type, expectedType);
        end

        function TypeNoSetter(tc)
            % Verify the Type property is read-only.
            import arrow.array.StructArray

            array = StructArray.fromArrays(tc.Float64Array, tc.StringArray);
            fcn = @() setfield(array, "Type", tc.Float64Array.Type);
            tc.verifyError(fcn, "MATLAB:class:SetProhibited");
        end

        function FieldByIndex(tc)
            import arrow.array.StructArray
            array = StructArray.fromArrays(tc.Float64Array, tc.StringArray);

            % Extract 1st field
            field1 = array.field(1);
            tc.verifyEqual(field1, tc.Float64Array);

            % Extract 2nd field
            field2 = array.field(2);
            tc.verifyEqual(field2, tc.StringArray);
        end

        function FieldByIndexError(tc)
            import arrow.array.StructArray
            array = StructArray.fromArrays(tc.Float64Array, tc.StringArray);

            % Supply a nonscalar vector
            fcn = @() array.field([1 2]);
            tc.verifyError(fcn, "arrow:badsubscript:NonScalar");

            % Supply a noninteger
            fcn = @() array.field(1.1);
            tc.verifyError(fcn, "arrow:badsubscript:NonInteger");
        end

        function FieldByName(tc)
            import arrow.array.StructArray
            array = StructArray.fromArrays(tc.Float64Array, tc.StringArray);

            % Extract 1st field
            field1 = array.field("Field1");
            tc.verifyEqual(field1, tc.Float64Array);

            % Extract 2nd field
            field2 = array.field("Field2");
            tc.verifyEqual(field2, tc.StringArray);
        end

        function FieldByNameError(tc)
            import arrow.array.StructArray
            array = StructArray.fromArrays(tc.Float64Array, tc.StringArray);

            % Supply a nonscalar string array
            fcn = @() array.field(["Field1" "Field2"]);
            tc.verifyError(fcn, "arrow:badsubscript:NonScalar");

            % Supply a nonexistent field name
            fcn = @() array.field("B");
            tc.verifyError(fcn, "arrow:tabular:schema:AmbiguousFieldName");
        end

        function toMATLAB(tc)
            % Verify toMATLAB returns the expected MATLAB table
            import arrow.array.StructArray
            array = StructArray.fromArrays(tc.Float64Array, tc.StringArray, FieldNames=["X", "Y"]);
            expectedTable = table(toMATLAB(tc.Float64Array), toMATLAB(tc.StringArray), VariableNames=["X", "Y"]);
            actualTable = toMATLAB(array);
            tc.verifyEqual(actualTable, expectedTable);

            % Verify table elements that correspond to "null" values 
            % in the StructArray are set to the type-specific null values.
            valid = [1 2 5];
            array = StructArray.fromArrays(tc.Float64Array, tc.StringArray, FieldNames=["X", "Y"], Valid=valid);
            float64NullValue = tc.Float64Array.NullSubstitutionValue;
            stringNullValue = tc.StringArray.NullSubstitutionValue;
            expectedTable([3 4], :) = repmat({float64NullValue stringNullValue}, [2 1]);
            actualTable = toMATLAB(array);
            tc.verifyEqual(actualTable, expectedTable);
        end

        function table(tc)
            % Verify toMATLAB returns the expected MATLAB table
            import arrow.array.StructArray
            array = StructArray.fromArrays(tc.Float64Array, tc.StringArray, FieldNames=["X", "Y"]);
            expectedTable = table(toMATLAB(tc.Float64Array), toMATLAB(tc.StringArray), VariableNames=["X", "Y"]);
            actualTable = table(array);
            tc.verifyEqual(actualTable, expectedTable);

            % Verify table elements that correspond to "null" values 
            % in the StructArray are set to the type-specific null values.
            valid = [1 2 5];
            array = StructArray.fromArrays(tc.Float64Array, tc.StringArray, FieldNames=["X", "Y"], Valid=valid);
            float64NullValue = tc.Float64Array.NullSubstitutionValue;
            stringNullValue = tc.StringArray.NullSubstitutionValue;
            expectedTable([3 4], :) = repmat({float64NullValue stringNullValue}, [2 1]);
            actualTable = toMATLAB(array);
            tc.verifyEqual(actualTable, expectedTable);
        end

        function IsEqualTrue(tc)
            % Verify isequal returns true when expected.
            import arrow.array.StructArray
            array1 = StructArray.fromArrays(tc.Float64Array, tc.StringArray, FieldNames=["X", "Y"]);
            array2 = StructArray.fromArrays(tc.Float64Array, tc.StringArray, FieldNames=["X", "Y"]);
            tc.verifyTrue(isequal(array1, array2));
        end

        function IsEqualFalse(tc)
            % Verify isequal returns false when expected.
            import arrow.array.StructArray
            array1 = StructArray.fromArrays(tc.Float64Array, tc.StringArray, FieldNames=["X", "Y"]);
            array2 = StructArray.fromArrays(tc.StringArray, tc.Float64Array, FieldNames=["X", "Y"]);
            array3 = StructArray.fromArrays(tc.Float64Array, tc.StringArray, FieldNames=["A", "B"]);
            % StructArrays have the same FieldNames but the Fields have different types.
            tc.verifyFalse(isequal(array1, array2));
            % Fields of the StructArrays have the same types but the StructArrays have different FieldNames.
            tc.verifyFalse(isequal(array1, array3));
        end

        function FromMATLABBasic(tc)
            % Verify StructArray.fromMATLAB returns the expected
            % StructArray.
            import arrow.array.StructArray

            T = table([1 2]', ["A1" "A2"]', VariableNames=["Number" "String"]);
            array = StructArray.fromMATLAB(T);
            tc.verifyEqual(array.NumElements, int64(2));
            tc.verifyEqual(array.NumFields, int32(2));
            tc.verifyEqual(array.FieldNames, ["Number" "String"]);

            field1 = arrow.array([1 2]');
            field2 = arrow.array(["A1" "A2"]');

            tc.verifyEqual(field1, array.field(1));
            tc.verifyEqual(field2, array.field(2));
        end

        function FromMATLABFieldNames(tc)
            % Verify StructArray.fromMATLAB returns the expected
            % StructArray when the FieldNames nv-pair is supplied.
            import arrow.array.StructArray

            T = table([1 2]', ["A1" "A2"]', VariableNames=["Number" "String"]);
            array = StructArray.fromMATLAB(T, FieldNames=["Custom" "Name"]);
            tc.verifyEqual(array.NumElements, int64(2));
            tc.verifyEqual(array.NumFields, int32(2));
            tc.verifyEqual(array.FieldNames, ["Custom" "Name"]);
            tc.verifyEqual(array.Valid, [true; true]);

            field1 = arrow.array([1 2]');
            field2 = arrow.array(["A1" "A2"]');

            tc.verifyEqual(field1, array.field(1));
            tc.verifyEqual(field2, array.field(2));
        end

        function FromMATLABValid(tc)
            % Verify StructArray.fromMATLAB returns the expected
            % StructArray when the Valid nv-pair is supplied.

            import arrow.array.StructArray

            T = table([1 2]', ["A1" "A2"]', VariableNames=["Number" "String"]);
            array = StructArray.fromMATLAB(T, Valid=2);
            tc.verifyEqual(array.NumElements, int64(2));
            tc.verifyEqual(array.NumFields, int32(2));
            tc.verifyEqual(array.FieldNames, ["Number" "String"]);
            tc.verifyEqual(array.Valid, [false; true]);

            field1 = arrow.array([1 2]');
            field2 = arrow.array(["A1" "A2"]');

            tc.verifyEqual(field1, array.field(1));
            tc.verifyEqual(field2, array.field(2));
        end

        function FromMATLABZeroVariablesError(tc)
            % Verify StructArray.fromMATLAB throws an error when the input
            % table T has zero variables.
            import arrow.array.StructArray

            fcn = @() StructArray.fromMATLAB(table);
            tc.verifyError(fcn, "arrow:struct:ZeroVariables");
        end

        function FromMATLABWrongNumberFieldNames(tc)
            % Verify StructArray.fromMATLAB throws an error when the 
            % FieldNames nv-pair is provided and its number of elements
            % does not equal the number of variables in the input table T.

            import arrow.array.StructArray

            fcn = @() StructArray.fromMATLAB(table(1), FieldNames=["A" "B"]);
            tc.verifyError(fcn, "arrow:tabular:WrongNumberColumnNames");
        end

        function FromMATLABValidNVPairBadIndex(tc)
            % Verify StructArray.fromMATLAB throws an error when the 
            % Valid nv-pair is provided and it contains an invalid index.

            import arrow.array.StructArray

            fcn = @() StructArray.fromMATLAB(table(1), Valid=2);
            tc.verifyError(fcn, "MATLAB:notLessEqual");
        end
    end
end