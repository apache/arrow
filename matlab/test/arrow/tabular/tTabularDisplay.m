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

        function ZeroRowsMultipleColumns(testCase, TabularType)
            % Verify that the display of a 0xN arrow tabular object displays
            % the schema but no rows because there are zero rows.
            import arrow.internal.test.display.makeLinkString
            t = table(Size=[0,6], ...
                      VariableTypes=["double" "single" "int8" "logical" "uint64" "string"], ...
                      VariableNames=["ratio = a / b" "number" "ID" "A very looooooooooooooong name" "Result" "侯磊"]);
            tabularObj = TabularType.FromTableFcn(t);
            actualDisplay = evalc('disp(tabularObj)');

            classNameString = makeLinkString(FullClassName=TabularType.FullClassName, ...
                ClassName=TabularType.ClassName, BoldFont=true);
            header = compose("  Arrow %s with %s rows and %s columns:", classNameString, getNumString(0), getNumString(6));
            var1Field = char(makeFieldString(                 "ratio = a / b", "Float64", "arrow.type.Float64Type"));
            var2Field = char(makeFieldString(                        "number", "Float32", "arrow.type.Float32Type"));
            var3Field = char(makeFieldString(                            "ID", "Int8"   , "arrow.type.Int8Type"));
            var4Field = char(makeFieldString("A very looooooooooooooong name", "Boolean", "arrow.type.BooleanType"));
            var5Field = char(makeFieldString(                        "Result", "UInt64" , "arrow.type.UInt64Type"));
            var6Field = char(makeFieldString(                          "侯磊", "String" , "arrow.type.StringType"));
            expectedDisplay = [char(header), newline, ...
                               newline, ...
                               '    Schema:', newline, ...
                               newline ...
                               '        ', var1Field, ' | ', var2Field, ' | ', var3Field, ' | ', var4Field, ' | ', var5Field, ' | ', var6Field, newline, ...
                               newline];

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

        function MultipleRowsAndColumns(testCase, TabularType)
            % Verify tabular object display when the object has mulitple rows
            % and columns. Only the first row is displayed. All columns are
            % displayed.
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

        function VeryWideTabular(testCase, TabularType)
            % Verify that all variables are displayed without any trucation 
            % even when the tabular object is wider than the MATLAB Command
            % Window.
            import arrow.internal.test.display.makeLinkString

            t = array2table([1:100;101:200],VariableNames="x"+(1:100));
            arrowTabularObj = TabularType.FromTableFcn(t);
            actualDisplay = evalc('disp(arrowTabularObj)');

            classNameString = makeLinkString(FullClassName=TabularType.FullClassName, ...
                ClassName=TabularType.ClassName, BoldFont=true);
            header = compose("  Arrow %s with %s rows and %s columns:", classNameString, getNumString(2), getNumString(100));
            schemaDisplay = ['    Schema:', newline, newline, '        '];
            dataDisplay   = ['    First Row:', newline, newline, '        '];
            for i = 1:width(t)
                if i < width(t)
                    schemaDisplay = [schemaDisplay, char(makeFieldString("x"+i, "Float64", "arrow.type.Float64Type")), ' | '];
                    dataDisplay = [dataDisplay, num2str(i), ' | '];
                else
                    schemaDisplay = [schemaDisplay, char(makeFieldString("x"+i, "Float64", "arrow.type.Float64Type"))];
                    dataDisplay = [dataDisplay, num2str(i)];
                end
            end
            expectedDisplay = [char(header), newline, ...
                               newline, ...
                               schemaDisplay, newline, ...
                               newline ...
                               dataDisplay, newline, ...
                               newline];
            testCase.verifyEqual(actualDisplay, expectedDisplay);
        end

        function DataContainsHyperlink(testCase, TabularType)
            % Verify that the data text containing hyperlink is always
            % displayed as expected no matter whether hotlinks is turned on
            % or off.
            import arrow.internal.test.display.makeLinkString

            hLink1 = string('<a href="http://www.foo.com">foo</a>');
            hLink2 = string('<a href="http://www.foo.com">another foo</a>');
            t = table(["a";"bcd";missing;""],[hLink1;hLink2;hLink1;hLink2],[NaN;1;2;3],'VariableNames',["Description", "Link_to_doc", "Result"]);
            arrowTabularObj = TabularType.FromTableFcn(t);

            % The display of schema and actual tabular data always contains 
            % hyperlinks no matter whether hotlinks is turned on or off
            var1Field = char(makeFieldString("Description", "String" , "arrow.type.StringType"));
            var2Field = char(makeFieldString("Link_to_doc", "String" , "arrow.type.StringType"));
            var3Field = char(makeFieldString(     "Result", "Float64", "arrow.type.Float64Type"));
            schemaDisplay = ['    Schema:', newline, newline, ...
                             '        ', var1Field, ' | ', var2Field, ' | ', var3Field];
            dataDisplay   = ['    First Row:', newline, newline, ...
                             '        ', '"a"', ' | ', '"', '<a href="http://www.foo.com">foo</a>', '"', ' | ', 'null'];
            expectedDisplayOfData = [newline, ...
                                     '    Schema:', newline, ...
                                     newline ...
                                     '        ', var1Field, ' | ', var2Field, ' | ', var3Field, newline, ...
                                     newline, ...
                                     '    First Row:', newline, ...
                                     newline, ...
                                     '        ', '"a"', ' | ', '"', '<a href="http://www.foo.com">foo</a>', '"', ' | ', 'null', newline, ...
                                     newline];

            % hotlinks is turned off
            actualDisplay = evalc('feature(''hotlinks'',''off'');disp(arrowTabularObj)');
            testCase.verifySubstring(actualDisplay, expectedDisplayOfData);

            % hotlinks is turned on
            actualDisplay = evalc('feature(''hotlinks'',''on'');disp(arrowTabularObj)');
            testCase.verifySubstring(actualDisplay, expectedDisplayOfData);
        end

        function DisplayClassNameWhenDataIsNotArray(testCase, TabularType)
            % Verify that the class name instead of the actual data will be
            % displayed when the datatype of a tabular variable is a nested
            % array type (e.g. StructArray or ListArray).
            import arrow.internal.test.display.makeLinkString

            t = table(datetime(2023,1,[1;2;3]),table([1;2;3],[4;5;6]),seconds([1;2;3]));
            arrowTabularObj = TabularType.FromTableFcn(t);
            actualDisplay = evalc('disp(arrowTabularObj)');

            classNameString = makeLinkString(FullClassName=TabularType.FullClassName, ...
                ClassName=TabularType.ClassName, BoldFont=true);
            header = compose("  Arrow %s with %s rows and %s columns:", classNameString, getNumString(3), getNumString(3));
            var1Field = char(makeFieldString("Var1", "Timestamp" , "arrow.type.TimestampType"));
            var2Field = char(makeFieldString("Var2", "Struct" , "arrow.type.StructType"));
            var3Field = char(makeFieldString("Var3", "Time64", "arrow.type.Time64Type"));
            expectedDisplay = [char(header), newline, ...
                               newline, ...
                               '    Schema:', newline, ...
                               newline, ...
                               '        ', var1Field, ' | ', var2Field, ' | ', var3Field, newline, ...
                               newline, ...
                               '    First Row:', newline, ...
                               newline, ...
                               '        ', '2023-01-01 00:00:00.000000 | <Struct> | 00:00:01.000000', newline,...
                               newline];

            testCase.verifyEqual(actualDisplay, expectedDisplay);
        end

        function DisplayInvalidData(testCase, TabularType)
            % Verify that "null" is displayed for invalid value.
            import arrow.internal.test.display.makeLinkString

            t = table(seconds([NaN;1]), string([missing;"a"]), string(["";"b"]),  [NaN;1], datetime(2023,1,[NaN;2]),...
                VariableNames=["durationVar", "stringVar1", "stringVar2", "doubleVar", "datetimeVar"]);
            arrowTabularObj = TabularType.FromTableFcn(t);
            actualDisplay = evalc('disp(arrowTabularObj)');

            classNameString = makeLinkString(FullClassName=TabularType.FullClassName, ...
                ClassName=TabularType.ClassName, BoldFont=true);
            header = compose("  Arrow %s with %s rows and %s columns:", classNameString, getNumString(2), getNumString(5));
            var1Field = char(makeFieldString("durationVar", "Time64" , "arrow.type.Time64Type"));
            var2Field = char(makeFieldString("stringVar1", "String" , "arrow.type.StringType"));
            var3Field = char(makeFieldString("stringVar2", "String", "arrow.type.StringType"));
            var4Field = char(makeFieldString("doubleVar", "Float64", "arrow.type.Float64Type"));
            var5Field = char(makeFieldString("datetimeVar", "Timestamp", "arrow.type.TimestampType"));
            expectedDisplay = [char(header), newline, ...
                               newline, ...
                               '    Schema:', newline, ...
                               newline, ...
                               '        ', var1Field, ' | ', var2Field, ' | ', var3Field, ' | ', var4Field, ' | ', var5Field, newline, ...
                               newline, ...
                               '    First Row:', newline, ...
                               newline, ...
                               '        ', 'null | null | "" | null | null', newline,...
                               newline];

            testCase.verifyEqual(actualDisplay, expectedDisplay);
        end

        function Unicode(testCase, TabularType)
            % Verify that unicode characters are displayed well. The 
            % current display doesn't align multiple rows vertically. 
            % Therefore, there is no alignment concern.
            import arrow.internal.test.display.makeLinkString

            t = table([0.1;0.2],string(char([26228 22825; 22810 20113])), ["Kevin"; "Lei"],...
                      VariableNames=["Number" "Weather" string(char([21517 23383]))]);
            arrowTabularObj = TabularType.FromTableFcn(t);
            actualDisplay = evalc('disp(arrowTabularObj)');

            classNameString = makeLinkString(FullClassName=TabularType.FullClassName, ...
                ClassName=TabularType.ClassName, BoldFont=true);
            header = compose("  Arrow %s with %s rows and %s columns:", classNameString, getNumString(2), getNumString(3));
            var1Field = char(makeFieldString(t.Properties.VariableNames{1}, "Float64" , "arrow.type.Float64Type"));
            var2Field = char(makeFieldString(t.Properties.VariableNames{2}, "String" , "arrow.type.StringType"));
            var3Field = char(makeFieldString(t.Properties.VariableNames{3}, "String", "arrow.type.StringType"));
            expectedDisplay = [char(header), newline, ...
                               newline, ...
                               '    Schema:', newline, ...
                               newline, ...
                               '        ', var1Field, ' | ', var2Field, ' | ', var3Field, newline, ...
                               newline, ...
                               '    First Row:', newline, ...
                               newline, ...
                               '        ', '0.1 | "', char([26228 22825]), '" | "Kevin"', newline,...
                               newline];

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