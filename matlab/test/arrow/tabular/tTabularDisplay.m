%TTABULARDISPLAY Unit tests verifying the display of arrow.tabular.Table
%and arrow.tabular.RecordBatch objects.

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

classdef tTabularDisplay < matlab.unittest.TestCase

    properties (TestParameter)
        TabularType
    end

    methods (TestParameterDefinition, Static)
        function TabularType = initializeTabularType()

            tableStruct = struct(FullClassName="arrow.tabular.Table", ...
                ClassName="Table", FromTableFcn = @arrow.table);
            
            recordBatchStruct = struct(FullClassName="arrow.tabular.RecordBatch", ...
                ClassName="RecordBatch", FromTableFcn=@arrow.recordBatch);

            TabularType = struct(Table=tableStruct, RecordBatch=recordBatchStruct);
        end

    end

    methods (Test)

        function ZeroRowsZeroColumns(testCase, TabularType)
            % Verify tabular object display when the object has zero rows
            % and zero columns.
            import arrow.internal.test.display.makeLinkString
            
            tabularObj = TabularType.FromTableFcn(table); %#ok<NASGU>
            
            classNameString = makeLinkString(FullClassName=TabularType.FullClassName, ...
                ClassName=TabularType.ClassName, BoldFont=true);
            zeroString = getNumString(0);
            header = compose("  Arrow %s with %s rows and %s columns", classNameString, zeroString, zeroString);
            expectedDisplay = char(header + newline + newline);
            actualDisplay = evalc('disp(tabularObj)');

            testCase.verifyEqual(actualDisplay, expectedDisplay);
        end

        function ZeroRowsOneColumn(testCase, TabularType)
            % Verify tabular object display when the object has zero rows
            % and one column.
            import arrow.internal.test.display.makeLinkString
            
            t = table(1, VariableNames="Number");
            tabularObj = TabularType.FromTableFcn(t(1:0, :)); %#ok<NASGU>
            
            classNameString = makeLinkString(FullClassName=TabularType.FullClassName, ...
                ClassName=TabularType.ClassName, BoldFont=true);
            header = compose("  Arrow %s with %s rows and %s column:", classNameString, getNumString(0), getNumString(1));
            
            fieldString = makeFieldString("Number", "Float64", "arrow.type.Float64Type");
            schema = join(["    Schema:" "        " + fieldString], [newline newline]);
            
            expectedDisplay = char(join([header schema + newline + newline], [newline newline]));
            actualDisplay = evalc('disp(tabularObj)');

            testCase.verifyEqual(actualDisplay, expectedDisplay);
        end

        function OneRowOneColumn(testCase, TabularType)
            % Verify tabular object display when the object has one row
            % and column.
            import arrow.internal.test.display.makeLinkString
            
            t = table(1, VariableNames="Number");
            tabularObj = TabularType.FromTableFcn(t); %#ok<NASGU>
            
            classNameString = makeLinkString(FullClassName=TabularType.FullClassName, ...
                ClassName=TabularType.ClassName, BoldFont=true);
            header = compose("  Arrow %s with %s row and %s column:", classNameString, getNumString(1), getNumString(1));
            
            fieldString = makeFieldString("Number", "Float64", "arrow.type.Float64Type");
            schema = join(["    Schema:" "        " + fieldString], [newline newline]);
            row = join(["    First Row:"  "        1"], [newline newline]);

            
            expectedDisplay = char(join([header schema row + newline + newline], [newline newline]));
            actualDisplay = evalc('disp(tabularObj)');

            testCase.verifyEqual(actualDisplay, expectedDisplay);
        end

        function ManyRowsAndColumns(testCase, TabularType)
            % Verify tabular object display when the object has many rows
            % and columns.
            import arrow.internal.test.display.makeLinkString
            
            t = table((1:2)', ["A"; "B"], true(2, 1), VariableNames=["Number", "Letter", "Logical"]);
            tabularObj = TabularType.FromTableFcn(t); %#ok<NASGU>
            
            classNameString = makeLinkString(FullClassName=TabularType.FullClassName, ...
                ClassName=TabularType.ClassName, BoldFont=true);
            header = compose("  Arrow %s with %s rows and %s columns:", classNameString, getNumString(2), getNumString(3));
            
            fieldOneString = makeFieldString("Number", "Float64", "arrow.type.Float64Type");
            fieldTwoString = makeFieldString("Letter", "String", "arrow.type.StringType");
            fieldThreeString = makeFieldString("Logical", "Boolean", "arrow.type.BooleanType");

            fields = join([fieldOneString fieldTwoString fieldThreeString], " | ");
            schema = join(["    Schema:" "        " + fields], [newline newline]);
            row = join(["    First Row:"  "        1 | ""A"" | true"], [newline newline]);
            
            expectedDisplay = char(join([header schema row + newline + newline], [newline newline]));
            actualDisplay = evalc('disp(tabularObj)');

            testCase.verifyEqual(actualDisplay, expectedDisplay);
        end
    end
end

function numString = getNumString(num)
    if usejava("desktop")
        numString = compose("<strong>%d</strong>", num);
    else
        numString = compose("%d", num);
    end
end

function str = makeFieldString(fieldName, classType, fullClassType)
    import arrow.internal.test.display.makeLinkString

    if usejava("desktop")
        name = compose("<strong>%s</strong>:", fieldName);
        typeStr = makeLinkString(FullClassName=fullClassType, ClassName=classType, BoldFont=true);
        str = name + " " + typeStr;
    else
        str = fieldName + ": " + classType;
    end
end