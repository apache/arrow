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

classdef tDate32Type < hFixedWidthType
% Test class for arrow.type.Date32Type and arrow.date32

    properties
        ConstructionFcn = @arrow.date32
        ArrowType = arrow.date32
        TypeID = arrow.type.ID.Date32
        BitWidth = int32(32)
        ClassName = "arrow.type.Date32Type"
    end

    methods(Test)
        function TestClass(testCase)
            % Verify ArrowType is an object of the expected class type.
            name = string(class(testCase.ArrowType));
            testCase.verifyEqual(name, testCase.ClassName);
        end

        function DefaultDateUnit(testCase)
            % Verify the default DateUnit is Day.
            type = testCase.ArrowType;
            actualUnit = type.DateUnit;
            expectedUnit = arrow.type.DateUnit.Day;
            testCase.verifyEqual(actualUnit, expectedUnit);
        end

        function Display(testCase)
            % Verify the display of Date32Type objects.
            %
            % Example:
            %
            %  Date32Type with properties:
            %
            %          ID: Date32
            %    DateUnit: Day
            %
            type = testCase.ConstructionFcn(); %#ok<NASGU>
            classnameLink = "<a href=""matlab:helpPopup arrow.type.Date32Type"" style=""font-weight:bold"">Date32Type</a>";
            header = "  " + classnameLink + " with properties:" + newline;
            body = strjust(pad(["ID:"; "DateUnit:"]));
            body = body + " " + ["Date32"; "Day"];
            body = "    " + body;
            footer = string(newline);
            expectedDisplay = char(strjoin([header body' footer], newline));
            actualDisplay = evalc('disp(type)');
            testCase.verifyEqual(actualDisplay, expectedDisplay);
        end

        function DateUnitNoSetter(testCase)
            % Verify that an error is thrown when trying to set the value
            % of the DateUnit property.
            type = arrow.date32();
            testCase.verifyError(@() setfield(type, "DateUnit", "Millisecond"), "MATLAB:class:SetProhibited");
        end

        function InvalidProxy(testCase)
            % Verify that an error is thrown when a Proxy of an unexpected
            % type is passed to the arrow.type.Date32Type constructor.
            array = arrow.array([1, 2, 3]);
            proxy = array.Proxy;
            testCase.verifyError(@() arrow.type.Date32Type(proxy), "arrow:proxy:ProxyNameMismatch");
        end

    end

end
