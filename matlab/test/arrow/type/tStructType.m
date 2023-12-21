% TSTRUCTTYPE Unit tests for arrow.type.StructType

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

classdef tStructType < matlab.unittest.TestCase

    properties (Constant)
        Field1 = arrow.field("A", arrow.float64())
        Field2 = arrow.field("C", arrow.boolean())
        Field3 = arrow.field("B", arrow.timestamp(TimeUnit="Microsecond", TimeZone="America/New_York"));
    end

    methods (Test)
        function Basic(tc)
            % Verify arrow.struct() returns an arrow.type.StructType
            % object.
            type = arrow.struct(tc.Field1);
            className = string(class(type));
            tc.verifyEqual(className, "arrow.type.StructType");
            tc.verifyEqual(type.ID, arrow.type.ID.Struct);
        end

        function TooFewInputsError(tc)
            % Verify arrow.struct() errors if given zero input arguments.
            fcn = @() arrow.struct();
            tc.verifyError(fcn, "arrow:struct:TooFewInputs");
        end

        function InvalidInputTypeError(tc)
            % Verify arrow.struct() errors if any one of the input
            % arguments is not an arrow.type.Field object.
            fcn = @() arrow.struct(1);
            tc.verifyError(fcn, "MATLAB:validation:UnableToConvert");
        end

        function EmptyFieldError(tc)
            % Verify arrow.struct() errors if given an empty 
            % arrow.type.Field array as one of its inputs.
            fcn = @() arrow.struct(tc.Field1, arrow.type.Field.empty(0, 0));
            tc.verifyError(fcn, "MATLAB:validators:mustBeNonempty");
        end

        function NumFieldsGetter(tc)
            % Verify the NumFields getter returns the expected value.
            type = arrow.struct(tc.Field1);
            tc.verifyEqual(type.NumFields, int32(1));

            type = arrow.struct(tc.Field1, tc.Field2);
            tc.verifyEqual(type.NumFields, int32(2));

            type = arrow.struct(tc.Field1, tc.Field2, tc.Field3);
            tc.verifyEqual(type.NumFields, int32(3));
        end

        function NumFieldsNoSetter(tc)
            % Verify the NumFields property is not settable.
            type = arrow.struct(tc.Field1);
            fcn = @() setfield(type, "NumFields", 20);
            tc.verifyError(fcn, "MATLAB:class:SetProhibited");
        end

        function FieldsGetter(tc)
            % Verify the Fields getter returns the expected
            % arrow.type.Field array.
            type = arrow.struct(tc.Field1, tc.Field2, tc.Field3);
            actual = type.Fields;
            expected = [tc.Field1, tc.Field2, tc.Field3];
            tc.verifyEqual(actual, expected);
        end

        function FieldsNoSetter(tc)
            % Verify the Fields property is not settable.
            type = arrow.struct(tc.Field1, tc.Field2, tc.Field3);
            fcn = @() setfield(type, "Fields", tc.Field3);
            tc.verifyError(fcn, "MATLAB:class:SetProhibited");
        end

        function IDGetter(tc)
            % Verify the ID getter returns the expected enum value.
            type = arrow.struct(tc.Field1);
            actual = type.ID;
            expected = arrow.type.ID.Struct;
            tc.verifyEqual(actual, expected);
        end

        function IDNoSetter(tc)
            % Verify the ID property is not settable.
            type = arrow.struct(tc.Field1);
            fcn = @() setfield(type, "ID", arrow.type.ID.Boolean);
            tc.verifyError(fcn, "MATLAB:class:SetProhibited");
        end

        function FieldMethod(tc)
            % Verify the field method returns the expected arrow.type.Field
            % with respect to the index provided.
            type = arrow.struct(tc.Field1, tc.Field2, tc.Field3);
            
            % Extract the 1st field
            actual1 = type.field(1);
            expected1 = tc.Field1;
            tc.verifyEqual(actual1, expected1);

            % Extract the 2nd field
            actual2 = type.field(2);
            expected2 = tc.Field2;
            tc.verifyEqual(actual2, expected2);

            % Extract the 3rd field
            actual3 = type.field(3);
            expected3 = tc.Field3;
            tc.verifyEqual(actual3, expected3);
        end

        function FieldIndexOutOfRangeError(tc)
            % Verify field() throws an error if provided an index that
            % exceeds NumFields.
            type = arrow.struct(tc.Field1, tc.Field2, tc.Field3);
            fcn = @() type.field(100);
            tc.verifyError(fcn, "arrow:index:OutOfRange");
        end

        function FieldIndexNonScalarError(tc)
            % Verify field() throws an error if provided a nonscalar array
            % of indices.
            type = arrow.struct(tc.Field1, tc.Field2, tc.Field3);
            fcn = @() type.field([1 2]);
            tc.verifyError(fcn, "arrow:badsubscript:NonScalar");
        end

        function FieldIndexNonNumberError(tc)
            % Verify field() throws an error if not provided a number as 
            % the index.
  
            type = arrow.struct(tc.Field1, tc.Field2, tc.Field3);
            fcn = @() type.field("A");
            tc.verifyError(fcn, "arrow:badsubscript:NonNumeric");
        end

        function IsEqualTrue(tc)
            % Verify two StructTypes are considered equal if their Fields
            % properties are equal.

            type1 = arrow.struct(tc.Field1, tc.Field2, tc.Field3);
            type2 = arrow.struct(tc.Field1, tc.Field2, tc.Field3);

            tc.verifyTrue(isequal(type1, type2));
            tc.verifyTrue(isequal(type1, type2, type2, type1));

            % Non-scalar arrow.type.StructType arrays
            type3 = [type1 type2];
            type4 = [type1 type2];
            tc.verifyTrue(isequal(type3, type4));
        end

        function IsEqualFalse(tc)
            % Verify isequal returns false when expected.
            type1 = arrow.struct(tc.Field1, tc.Field2, tc.Field3);
            type2 = arrow.struct(tc.Field1, tc.Field2);
            type3 = arrow.struct(tc.Field1, tc.Field3, tc.Field2);

            % Fields properties have different lengths
            tc.verifyFalse(isequal(type1, type2));

            % The corresponding elements in the Fields arrays are not equal
            tc.verifyFalse(isequal(type1, type3));

            % Non-scalar arrow.type.StructType arrays
            type4 = [type1 type2];
            type5 = [type1; type2];
            type6 = [type1 type2];
            type7 = [type1 type3];
            tc.verifyFalse(isequal(type4, type5));
            tc.verifyFalse(isequal(type6, type7));

        end
    end
end