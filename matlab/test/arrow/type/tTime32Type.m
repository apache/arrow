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

classdef tTime32Type < hFixedWidthType
% Test class for arrow.type.Time32Type and arrow.time32

    properties
        ConstructionFcn = @arrow.time32
        ArrowType = arrow.time32
        TypeID = arrow.type.ID.Time32
        BitWidth = int32(32)
        ClassName = "arrow.type.Time32Type"
    end

    methods(Test)
        function TestClass(testCase)
            % Verify ArrowType is an object of the expected class type.
            name = string(class(testCase.ArrowType));
            testCase.verifyEqual(name, testCase.ClassName);
        end

        function DefaultTimeUnit(testCase)
            % Verify the default TimeUnit is Second.
            type = testCase.ArrowType;
            actualUnit = type.TimeUnit;
            expectedUnit = arrow.type.TimeUnit.Second;
            testCase.verifyEqual(actualUnit, expectedUnit);
        end

        function SupplyTimeUnitEnum(testCase)
            % Verify that TimeUnit can be specified as an enum value.
            import arrow.type.*
            expectedUnit = [TimeUnit.Second, TimeUnit.Millisecond];

            for unit = expectedUnit
                type = testCase.ConstructionFcn(TimeUnit=unit);
                testCase.verifyEqual(type.TimeUnit, unit);
            end
        end

        function SupplyTimeUnitString(testCase)
            % Supply TimeUnit as a string value. Verify TimeUnit is set to
            % the appropriate TimeUnit enum value.
            import arrow.type.*
            unitString = ["second", "millisecond"];
            expectedUnit = [TimeUnit.Second, TimeUnit.Millisecond];

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
            units = [arrow.type.TimeUnit.Second; arrow.type.TimeUnit.Millisecond];
            fcn = @() testCase.ConstructionFcn(TimeUnit=units);
            testCase.verifyError(fcn, "MATLAB:validation:IncompatibleSize");

            units = ["second" "millisecond"];
            fcn = @() testCase.ConstructionFcn(TimeUnit=units);
            testCase.verifyError(fcn, "MATLAB:validation:IncompatibleSize");
        end

        function Display(testCase)
            % Verify the display of Time32Type objects.
            %
            % Example:
            %
            %  Time32Type with properties:
            %
            %          ID: Time32
            %    TimeUnit: Second
            %
            type = testCase.ConstructionFcn(TimeUnit="Second"); %#ok<NASGU>
            classnameLink = "<a href=""matlab:helpPopup arrow.type.Time32Type"" style=""font-weight:bold"">Time32Type</a>";
            header = "  " + classnameLink + " with properties:" + newline;
            body = strjust(pad(["ID:"; "TimeUnit:"]));
            body = body + " " + ["Time32"; "Second"];
            body = "    " + body;
            footer = string(newline);
            expectedDisplay = char(strjoin([header body' footer], newline));
            actualDisplay = evalc('disp(type)');
            testCase.verifyEqual(actualDisplay, expectedDisplay);
        end

        function TimeUnitNoSetter(testCase)
            % Verify that an error is thrown when trying to set the value
            % of the TimeUnit property.
            schema = arrow.time32(TimeUnit="Millisecond");
            testCase.verifyError(@() setfield(schema, "TimeUnit", "Second"), "MATLAB:class:SetProhibited");
        end

        function BitWidthNoSetter(testCase)
            % Verify that an error is thrown when trying to set the value
            % of the BitWidth property.
            schema = arrow.time32(TimeUnit="Millisecond");
            testCase.verifyError(@() setfield(schema, "BitWidth", 64), "MATLAB:class:SetProhibited");
        end

        function IDNoSetter(testCase)
            % Verify that an error is thrown when trying to set the value
            % of the ID property.
            schema = arrow.time32(TimeUnit="Millisecond");
            testCase.verifyError(@() setfield(schema, "ID", 15), "MATLAB:class:SetProhibited");
        end

        function NumFieldsNoSetter(testCase)
            % Verify that an error is thrown when trying to set the value
            % of the NumFields property.
            schema = arrow.time32(TimeUnit="Millisecond");
            testCase.verifyError(@() setfield(schema, "NumFields", 2), "MATLAB:class:SetProhibited");
        end

        function InvalidProxy(testCase)
            % Verify that an error is thrown when a Proxy of an unexpected
            % type is passed to the arrow.type.Time32Type constructor.
            array = arrow.array([1, 2, 3]);
            proxy = array.Proxy;
            testCase.verifyError(@() arrow.type.Time32Type(proxy), "arrow:proxy:ProxyNameMismatch");
        end

    end

end
