% Test class for arrow.type.Date64Type and arrow.date64

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

classdef tDate64Type < hFixedWidthType

    properties
        ConstructionFcn = @arrow.date64
        ArrowType = arrow.date64
        TypeID = arrow.type.ID.Date64
        BitWidth = int32(64)
        ClassName = "arrow.type.Date64Type"
    end

    methods(Test)
        function TestClass(testCase)
            % Verify ArrowType is an object of the expected class type.
            name = string(class(testCase.ArrowType));
            testCase.verifyEqual(name, testCase.ClassName);
        end

        function DefaultDateUnit(testCase)
            % Verify the default DateUnit is Millisecond.
            type = testCase.ArrowType;
            actualUnit = type.DateUnit;
            expectedUnit = arrow.type.DateUnit.Millisecond;
            testCase.verifyEqual(actualUnit, expectedUnit);
        end

        function DateUnitNoSetter(testCase)
            % Verify that an error is thrown when trying to set the value
            % of the DateUnit property.
            type = arrow.date64();
            testCase.verifyError(@() setfield(type, "DateUnit", "Day"), "MATLAB:class:SetProhibited");
        end

        function InvalidProxy(testCase)
            % Verify that an error is thrown when a Proxy of an unexpected
            % type is passed to the arrow.type.Date64Type constructor.
            array = arrow.array([1, 2, 3]);
            proxy = array.Proxy;
            testCase.verifyError(@() arrow.type.Date64Type(proxy), "arrow:proxy:ProxyNameMismatch");
        end

        function IsEqualTrue(testCase)
            % Verifies isequal method of arrow.type.Date64Type returns true if
            % these conditions are met:
            %
            % 1. All input arguments have a class type arrow.type.Date64Type
            % 2. All inputs have the same size

            % Scalar Date64Type arrays
            date64Type1 = arrow.date64();
            date64Type2 = arrow.date64();
            testCase.verifyTrue(isequal(date64Type1, date64Type2));

            % Non-scalar Date64Type arrays
            typeArray1 = [date64Type1 date64Type1];
            typeArray2 = [date64Type2 date64Type2];
            testCase.verifyTrue(isequal(typeArray1, typeArray2));
        end

        function IsEqualFalse(testCase)
            % Verifies the isequal method of arrow.type.Date64Type returns
            % false when expected.

            % Pass a different arrow.type.Type subclass to isequal
            date64Type = arrow.date64();
            int32Type = arrow.int32();
            testCase.verifyFalse(isequal(date64Type, int32Type));
            testCase.verifyFalse(isequal([date64Type date64Type], [int32Type int32Type]));

            % Date64Type arrays have different sizes
            typeArray1 = [date64Type date64Type];
            typeArray2 = [date64Type date64Type]';
            testCase.verifyFalse(isequal(typeArray1, typeArray2));
        end

    end

end
