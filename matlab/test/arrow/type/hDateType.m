% Test class that contains shared unit tests for classes that subclass
% arrow.type.DateType.

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

classdef hDateType < hFixedWidthType

    properties(Abstract)
        DefaultDateUnit(1, 1) arrow.type.DateUnit
        ConstructionFcn
        ClassConstructorFcn
    end

    methods (Test)
        function TestClass(testCase)
            % Verify ArrowType is an instance of the expected class type.
            name = string(class(testCase.ArrowType()));
            testCase.verifyEqual(name, testCase.ClassName);
        end

        function DateUnitNoSetter(testCase)
            % Verify an exception is thrown when attempting to modify
            % the DateUnit property.
            type = testCase.ConstructionFcn();
            testCase.verifyError(@() setfield(type, "DateUnit", "Millisecond"), "MATLAB:class:SetProhibited");
        end

        function InvalidProxy(testCase)
            % Verify that an exception is thrown when a Proxy of an 
            % unexpected type is passed to the DateType's constructor.
            array = arrow.array([1, 2, 3]);
            proxy = array.Proxy;
            testCase.verifyError(@() testCase.ClassConstructorFcn(proxy), "arrow:proxy:ProxyNameMismatch");
        end

        function IsEqualTrue(testCase)
            % Verifies the isequal method returns true if both conditions
            % listed below are satisfied:
            %
            % 1. All input arguments have the same class type (either
            %       arrow.type.Date32Type or arrow.type.Date64Type).
            % 2. All inputs have the same size.

            % Scalar arrays
            dateType1 = testCase.ConstructionFcn();
            dateType2 = testCase.ConstructionFcn();
            testCase.verifyTrue(isequal(dateType1, dateType2));

            % Non-scalar arrays
            typeArray1 = [dateType1 dateType1];
            typeArray2 = [dateType2 dateType2];
            testCase.verifyTrue(isequal(typeArray1, typeArray2));
        end

        function IsEqualFalse(testCase)
            % Verifies the isequal method returns false when at least of
            % these conditions is not satisfied:
            %
            % 1. All input arguments have the same class type (either
            %       arrow.type.Date32Type or arrow.type.Date64Type).
            % 2. All inputs have the same size.

            % Pass a different arrow.type.Type subclass to isequal.
            dateType = testCase.ConstructionFcn();
            int32Type = arrow.int32();
            testCase.verifyFalse(isequal(dateType, int32Type));
            testCase.verifyFalse(isequal([dateType dateType], [int32Type int32Type]));

            % The array sizes are not equal.
            typeArray1 = [dateType dateType];
            typeArray2 = [dateType dateType]';
            testCase.verifyFalse(isequal(typeArray1, typeArray2));
        end

    end

end