%TLISTARRAY Tests for arrow.array.ListArray

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

classdef tListArray < matlab.unittest.TestCase

    properties (Constant)
        Traits = arrow.type.traits.traits(arrow.type.ID.List)
        FromArraysFcn = @arrow.array.ListArray.fromArrays
        FromMatlabFcn = @arrow.array.ListArray.fromMATLAB
    end

    properties (TestParameter)
        TestArray
    end

    methods (TestParameterDefinition, Static)

        function TestArray = initializeTestArray()
            %% Empty list
            Type = arrow.list(arrow.float64());
            NumElements = int64(0);
            Valid = logical.empty(0, 1);
            Offsets = arrow.array(int32(0));
            Values = arrow.array([]);
            Array = tListArray.FromArraysFcn(Offsets, Values, Valid=Valid);

            TestArray.EmptyList = struct( ...
                Array=Array, ...
                Properties=struct(...
                    Type=Type, ...
                    NumElements=NumElements, ...
                    Valid=Valid, ...
                    Offsets=Offsets, ...
                    Values=Values ...
                ) ...
            );

            %% List with NULLs
            Type = arrow.list(arrow.string());
            NumElements = int64(4);
            Valid = [true, false, true, false];
            Offsets = arrow.array(int32([0, 1, 4, 6, 7]));
            Values = arrow.array(["A", missing, "C", "D", "E", missing, "G"]);
            Array = tListArray.FromArraysFcn(Offsets, Values, Valid=Valid);

            TestArray.NullList = struct( ...
                Array=Array, ...
                Properties=struct(...
                    Type=Type, ...
                    NumElements=NumElements, ...
                    Valid=Valid, ...
                    Offsets=Offsets, ...
                    Values=Values ...
                ) ...
            );

            %% Single-level list (List<Float64>)
            Type = arrow.list(arrow.float64());
            NumElements = int64(3);
            Valid = true(1, NumElements);
            Offsets = arrow.array(int32([0, 2, 5, 9]));
            Values = arrow.array([1, 2, 3, 4, 5, 6, 7, 8, 9]);
            Array = tListArray.FromArraysFcn(Offsets, Values, Valid=Valid);

            TestArray.SingleLevelList = struct( ...
                Array=Array, ...
                Properties=struct(...
                    Type=Type, ...
                    NumElements=NumElements, ...
                    Valid=Valid, ...
                    Offsets=Offsets, ...
                    Values=Values ...
                ) ...
            );

            %% Multi-level list (List<List<Float64>>)
            Type = arrow.list(arrow.list(arrow.float64()));
            NumElements = int64(2);
            Valid = true(1, NumElements);
            Offsets = arrow.array(int32([0, 1, 3]));
            Values = TestArray.SingleLevelList.Array;
            Array = tListArray.FromArraysFcn(Offsets, Values, Valid=Valid);

            TestArray.MultiLevelList = struct( ...
                Array=Array, ...
                Properties=struct(...
                    Type=Type, ...
                    NumElements=NumElements, ...
                    Valid=Valid, ...
                    Offsets=Offsets, ...
                    Values=Values ...
                ) ...
            );
        end

    end

    methods (Test)

        function TestClass(testCase, TestArray)
            % Verify that the arrow.array.Array has the expected concrete
            % subclass.
            testCase.verifyInstanceOf(TestArray.Array, testCase.Traits.ArrayClassName);
        end

        function TestProperties(testCase, TestArray)
            % Verify that all properties of the arrow.array.Array:
            %
            % 1. Return the expected value
            % 2. Cannot be modified (i.e. are read-only).
            %
            properties = string(fieldnames(TestArray.Properties));
            for ii = numel(properties)
                property = properties(ii);
                expected = TestArray.Properties.(property);
                actual = getfield(TestArray.Array, property);
                % Verify that the property returns the expected value.
                testCase.verifyEqual(actual, expected);
                fcn = @() setfield(TestArray.Array, property, "NewValue");
                % Verify that the property cannot be modified (i.e. that it
                % is read-only).
                testCase.verifyError(fcn, "MATLAB:class:SetProhibited");
            end
        end

    end

end
