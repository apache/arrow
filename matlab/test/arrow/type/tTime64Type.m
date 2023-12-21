%TTIME64TYPE Test class for arrow.type.Time64Type and arrow.time64

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

classdef tTime64Type < hFixedWidthType

    properties
        ConstructionFcn = @arrow.time64
        ArrowType = arrow.time64
        TypeID = arrow.type.ID.Time64
        BitWidth = int32(64)
        ClassName = "arrow.type.Time64Type"
    end

    methods(Test)
        function TestClass(testCase)
            % Verify ArrowType is an object of the expected class type.
            name = string(class(testCase.ArrowType));
            testCase.verifyEqual(name, testCase.ClassName);
        end

        function DefaultTimeUnit(testCase)
            % Verify the default TimeUnit is Microsecond.
            type = testCase.ArrowType;
            actualUnit = type.TimeUnit;
            expectedUnit = arrow.type.TimeUnit.Microsecond;
            testCase.verifyEqual(actualUnit, expectedUnit);
        end

        function SupplyTimeUnitEnum(testCase)
            % Verify that TimeUnit can be specified as an enum value.
            import arrow.type.*
            expectedUnit = [TimeUnit.Microsecond, TimeUnit.Nanosecond];

            for unit = expectedUnit
                type = testCase.ConstructionFcn(TimeUnit=unit);
                testCase.verifyEqual(type.TimeUnit, unit);
            end
        end

        function SupplyTimeUnitString(testCase)
            % Supply TimeUnit as a string value. Verify TimeUnit is set to
            % the appropriate TimeUnit enum value.
            import arrow.type.*
            unitString = ["Microsecond", "Nanosecond"];
            expectedUnit = [TimeUnit.Microsecond, TimeUnit.Nanosecond];

            for ii = 1:numel(unitString)
                type = testCase.ConstructionFcn(TimeUnit=unitString(ii));
                testCase.verifyEqual(type.TimeUnit, expectedUnit(ii));
            end
        end

        function ErrorIfAmbiguousTimeUnit(testCase)
            % Verify that an error is thrown if an ambiguous value is
            % provided for the TimeUnit name-value pair.
            fcn = @() testCase.ConstructionFcn(TimeUnit="mi");
            testCase.verifyError(fcn, "MATLAB:validation:UnableToConvert");
        end

        function ErrorIfTimeUnitIsNonScalar(testCase)
            % Verify that an error is thrown if a nonscalar value is
            % provided for the TimeUnit name-value pair.
            units = [arrow.type.TimeUnit.Microsecond; arrow.type.TimeUnit.Nanosecond];
            fcn = @() testCase.ConstructionFcn(TimeUnit=units);
            testCase.verifyError(fcn, "MATLAB:validation:IncompatibleSize");

            units = ["Microsecond" "Nanosecond"];
            fcn = @() testCase.ConstructionFcn(TimeUnit=units);
            testCase.verifyError(fcn, "MATLAB:validation:IncompatibleSize");
        end

        function TimeUnitNoSetter(testCase)
            % Verify that an error is thrown when trying to set the value
            % of the TimeUnit property.
            type = arrow.time64(TimeUnit="Nanosecond");
            testCase.verifyError(@() setfield(type, "TimeUnit", "Microsecond"), "MATLAB:class:SetProhibited");
        end

        function InvalidProxy(testCase)
            % Verify that an error is thrown when a Proxy of an unexpected
            % type is passed to the arrow.type.Time64Type constructor.
            array = arrow.array([1, 2, 3]);
            proxy = array.Proxy;
            testCase.verifyError(@() arrow.type.Time64Type(proxy), "arrow:proxy:ProxyNameMismatch");
        end

        function IsEqualTrue(testCase)
            % Verifies isequal method of arrow.type.Time64Type returns true if
            % these conditions are met:
            %
            % 1. All input arguments have a class type arrow.type.Time64Type
            % 2. All inputs have the same size
            % 3. The TimeUnit values of elements at corresponding positions in the arrays are equal

            % Scalar Time64Type arrays
            time64Type1 = arrow.time64(TimeUnit="Microsecond");
            time64Type2 = arrow.time64(TimeUnit="Microsecond");
            time64Type3 = arrow.time64(TimeUnit="Nanosecond");
            time64Type4 = arrow.time64(TimeUnit="Nanosecond");
            testCase.verifyTrue(isequal(time64Type1, time64Type2));
            testCase.verifyTrue(isequal(time64Type3, time64Type4));

            % Non-scalar Time64Type arrays
            typeArray1 = [time64Type1 time64Type3];
            typeArray2 = [time64Type2 time64Type4];
            testCase.verifyTrue(isequal(typeArray1, typeArray2));
        end

        function IsEqualFalse(testCase)
            % Verify isequal returns false when expected.
            time64Type1 = arrow.time64(TimeUnit="Microsecond");
            time64Type2 = arrow.time64(TimeUnit="Nanosecond");
            int32Type = arrow.int32();
            testCase.verifyFalse(isequal(time64Type1, time64Type2));
            testCase.verifyFalse(isequal(time64Type1, int32Type));

            % arrays have different dimensions
            typeArray1 = [time64Type1 time64Type2];
            typeArray2 = [time64Type1 time64Type2]';
            testCase.verifyFalse(isequal(typeArray1, typeArray2));

            % Corresponding elements have different TimeUnit values
            typeArray3 = [time64Type2 time64Type1];
            typeArray4 = [time64Type1 time64Type2]';
            testCase.verifyFalse(isequal(typeArray3, typeArray4));

            % Compare a nonscalar Time64Type array with a nonscalar
            % Int32Type array.
            typeArray5 = [int32Type int32Type];
            testCase.verifyFalse(isequal(typeArray3, typeArray5));
        end

    end

end
