%HTABULAR Shared test class for tabular types.

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

classdef hTabular < matlab.unittest.TestCase

    properties (Abstract)
        % Must be a handle function to the fromArrays construction 
        % function (i.e. @arrow.tabular.Table.fromArrays)
        FromArraysFcn
        % Must be a handle function to the convenience construction
        % function. (i.e. @arrow.table)
        ConstructionFcn
        % Must be full class name (i.e. "arrow.tabular.Table").
        ClassName(1, 1) string
    end

    properties 
        ShortClassName(1, 1) string
        ErrorIdentifierPrefix(1, 1) string
    end

    methods (TestClassSetup)
        function initializeShortClassName(tc)
            dotPos = strfind(tc.ClassName, ".");
            tc.ShortClassName = extractAfter(tc.ClassName, dotPos(end));
        end

        function initializeErrorIdentifierPrefix(tc)
            tc.ErrorIdentifierPrefix = "arrow:tabular:" + lower(tc.ShortClassName) + ":";
        end

    end

    methods (Abstract)
        % Used by test methods to verify arrowTabularObj has the expected
        % value. Subclasses of hTabular are responsible for implementing
        % this method.
        %
        % columnNames is a string array containing the expected ColumnNames
        %    property value of arrowTabularObj.
        %
        % columnTraits is a cell array where each element is an
        %    arrow.type.traits.TypeTraits instance. Each TypeTraits instance
        %    corresponds to a column in arrowTabularObj.
        %
        % matlabTable is the expected output of calling the 
        %    toMATLAB() method on arrowTabularObj.
        verifyTabularObject(tc, arrowTabularObj, columnNames, columnTraits, matlabTable)
    
        % Used to verify the output of the arrow.tabular.Tabular/column() 
        % method. Subclasses of hTabular are responsible for implementing
        % this method.
        % 
        % This method must accept an arrow.array.Array as input and
        % convert it to an instance of the Arrow type that the column()
        % method returns.
        col = makeColumnFromArray(tc, array)
    end

    methods (Test, TestTags={'construction'})
        function ConstructionFunction(tc)
            % Verify the convenience construction function creates an
            % instance of the expected tabular type when provided a
            % MATLAB table as input.
            T = table([1, 2, 3]');
            obj = tc.ConstructionFcn(T);
            className = string(class(obj));
            tc.verifyEqual(className, tc.ClassName);
       end

       function ConstructionFunctionZeroArguments(tc)
            % Verify the convenience construction function creates an
            % instance of the expected tabular type when provided zero
            % input arguments.
            obj = tc.ConstructionFcn();
            className = string(class(obj));
            tc.verifyEqual(className, tc.ClassName);
            tc.verifyEqual(obj.NumRows, int64(0));
            tc.verifyEqual(obj.NumColumns, int32(0));
        end

        function EmptyTabularObject(tc)
            % Verify the convenience construction function creates a
            % tabular object (of the correct type) when provided an empty
            % MATLAB table as input.
            matlabTable = table.empty(0, 0);
            tabularObj = tc.ConstructionFcn(matlabTable);
            tc.verifyEqual(tabularObj.NumRows, int64(0));
            tc.verifyEqual(tabularObj.NumColumns, int32(0));
            tc.verifyEqual(tabularObj.ColumnNames, string.empty(1, 0));
            tc.verifyEqual(toMATLAB(tabularObj), matlabTable);

            matlabTable = table.empty(1, 0);
            tabularObj = tc.ConstructionFcn(matlabTable);
            tc.verifyEqual(tabularObj.NumRows, int64(0));
            tc.verifyEqual(tabularObj.NumColumns, int32(0));
            tc.verifyEqual(tabularObj.ColumnNames, string.empty(1, 0));

            matlabTable = table.empty(0, 1);
            tabularObj = tc.ConstructionFcn(matlabTable);
            tc.verifyEqual(tabularObj.NumRows, int64(0));
            tc.verifyEqual(tabularObj.NumColumns, int32(1));
            tc.verifyEqual(tabularObj.ColumnNames, "Var1");
        end

        function SupportedTypes(tc)
            % Verify the convenience construction function creates an 
            % instance of the expected tabular type when provided a MATLAB
            % table containing all MATLAB types that can be converted to
            % Arrow Arrays.
            import arrow.internal.test.tabular.createTableWithSupportedTypes
            import arrow.type.traits.traits

            matlabTable = createTableWithSupportedTypes();
            arrowTabularObj = tc.ConstructionFcn(matlabTable);
            expectedColumnNames = string(matlabTable.Properties.VariableNames);

            % For each variable in the input MATLAB table, look up the
            % corresponding Arrow Type using type traits.
            expectedColumnTraits = varfun(@(var) traits(string(class(var))), ...
                matlabTable, OutputFormat="cell");
            tc.verifyTabularObject(arrowTabularObj, expectedColumnNames, expectedColumnTraits, matlabTable);
        end

    end

    methods (Test, TestTags={'Properties'})

        function ColumnNames(tc)
            % Verify ColumnNames is set to the expected value.
            columnNames = ["A", "B", "C"];
            TOriginal = table(1, 2, 3, VariableNames=columnNames);
            tabularObj = tc.ConstructionFcn(TOriginal);
            tc.verifyEqual(tabularObj.ColumnNames, columnNames);
        end

        function UnicodeColumnNames(tc)
            % Verify column names can contain non-ascii characters.
            smiley = "😀";
            tree =  "🌲";
            mango = "🥭";
            columnNames = [smiley, tree, mango];
            TOriginal = table(1, 2, 3, VariableNames=columnNames);
            tabularObj = tc.ConstructionFcn(TOriginal);
            tc.verifyEqual(tabularObj.ColumnNames, columnNames);
            TConverted = tabularObj.toMATLAB();
            tc.verifyEqual(TOriginal, TConverted);
        end

        function ColumnNamesNoSetter(tc)
            % Verify ColumnNames is not settable.
            t = table([1; 2; 3]);
            tabularObj = tc.ConstructionFcn(t);
            tc.verifyError(@() setfield(tabularObj, "ColumnNames", "x"), ...
                "MATLAB:class:SetProhibited");
        end

        function NumRows(tc)
            % Verify the NumRows property is set to the expected value.
            numRows = int64([1, 5, 100]);
            for expectedNumRows = numRows
                matlabTable = array2table(ones(expectedNumRows, 1));
                arrowTable = arrow.table(matlabTable);
                tc.verifyEqual(arrowTable.NumRows, expectedNumRows);
            end
        end

        function NumRowsNoSetter(tc)
            % Verify NumRows is not settable.
            t = table([1; 2; 3]);
            tabularObj = tc.ConstructionFcn(t);
            tc.verifyError(@() setfield(tabularObj, "NumRows", 10), ...
                "MATLAB:class:SetProhibited");
        end

        function NumColumns(tc)
            % Verify the NumColumns property is set to the expected value.
            numColumns = int32([1, 5, 100]);
            for nc = numColumns
                T = array2table(ones(1, nc));
                tabularObj = tc.ConstructionFcn(T);
                tc.verifyEqual(tabularObj.NumColumns, nc);
            end
        end

        function NumColumnsNoSetter(tc)
            % Verify the NumColumns property is not settable.
            t = table([1; 2; 3]);
            tabularObj = tc.ConstructionFcn(t);
            tc.verifyError(@() setfield(tabularObj, "NumColumns", 10), ...
                "MATLAB:class:SetProhibited");
        end

        function Schema(tc)
            % Verify the Schema property is an instance of 
            % arrow.tabular.Schema and correctly configured.
            matlabTable = table(...
                ["A"; "B"; "C"], ...
                [1; 2; 3], ...
                [true; false; true], ...
                VariableNames=["A", "B", "C"] ...
            );
            tabularObj = tc.ConstructionFcn(matlabTable);
            schema = tabularObj.Schema;
            tc.verifyEqual(schema.NumFields, int32(3));
            tc.verifyEqual(schema.field(1).Type.ID, arrow.type.ID.String);
            tc.verifyEqual(schema.field(1).Name, "A");
            tc.verifyEqual(schema.field(2).Type.ID, arrow.type.ID.Float64);
            tc.verifyEqual(schema.field(2).Name, "B");
            tc.verifyEqual(schema.field(3).Type.ID, arrow.type.ID.Boolean);
            tc.verifyEqual(schema.field(3).Name, "C");
        end

        function SchemaNoSetter(tc)
            % Verify the Schema property is not settable.
            t = table([1; 2; 3]);
            tabularObj = tc.ConstructionFcn(t);
            tc.verifyError(@() setfield(tabularObj, "Schema", "Value"), ...
                "MATLAB:class:SetProhibited");
        end
    end

    methods (Test, TestTags={'Conversion functions'})

        function ToMATLAB(tc)
            % Verify the toMATLAB() method returns the expected MATLAB table.
            expectedMatlabTable = table([1, 2, 3]');
            arrowTabularObj = tc.ConstructionFcn(expectedMatlabTable);
            actualMatlabTable = arrowTabularObj.toMATLAB();
            tc.verifyEqual(actualMatlabTable, expectedMatlabTable);
        end

        function Table(tc)
            % Verify the table() method returns the expected MATLAB table.
            TOriginal = table([1, 2, 3]');
            arrowTable = tc.ConstructionFcn(TOriginal);
            TConverted = table(arrowTable);
            tc.verifyEqual(TOriginal, TConverted);
        end

    end

    methods (Test, TestTags={'fromArrays'})

        function FromArraysColumnNamesNotProvided(tc)
            % Verify the fromArrays function creates the expected
            % Tabular object when given a comma-separated list of 
            % arrow.array.Array instances.
            import arrow.type.traits.traits
            import arrow.internal.test.tabular.createAllSupportedArrayTypes

            [arrowArrays, matlabData] = createAllSupportedArrayTypes();
            TOriginal = table(matlabData{:});

            tabularObj = tc.FromArraysFcn(arrowArrays{:});
            columnNames = compose("Column%d", 1:width(TOriginal));
            TOriginal.Properties.VariableNames = columnNames;
            columnTraits = cellfun(@(array) traits(array.Type.ID), arrowArrays, UniformOutput=false);
            tc.verifyTabularObject(tabularObj, columnNames, columnTraits, TOriginal);
        end

        function FromArraysWithColumnNamesProvided(tc)
            % Verify the fromArrays function creates the expected
            % Tabular object when given a comma-separated list of 
            % arrow.array.Array instances and the ColumnNames nv-argument
            % is provided.
            import arrow.type.traits.traits
            import arrow.internal.test.tabular.createAllSupportedArrayTypes

            [arrowArrays, matlabData] = createAllSupportedArrayTypes();
            TOriginal = table(matlabData{:});

            columnNames = compose("MyVar%d", 1:numel(arrowArrays));
            tabularObj = tc.FromArraysFcn(arrowArrays{:}, ColumnNames=columnNames);
            TOriginal.Properties.VariableNames = columnNames;
            columnTraits = cellfun(@(array) traits(array.Type.ID), arrowArrays, UniformOutput=false);
            tc.verifyTabularObject(tabularObj, columnNames, columnTraits, TOriginal);
        end

        function FromArraysUnequalArrayLengthsError(tc)
            % Verify the fromArrays function throws an exception with the
            % identifier "arrow:tabular:UnequalArrayLengths" if the
            % arrays provided have different sizes.
            A1 = arrow.array([1, 2]);
            A2 = arrow.array(["A", "B", "C"]);
            fcn = @() tc.FromArraysFcn(A1, A2);
            tc.verifyError(fcn, "arrow:tabular:UnequalArrayLengths");
        end

        function FromArraysWrongNumberColumnNamesError(tc)
            % Verify the fromArrays function throws an exception with the
            % identifier "arrow:tabular:WrongNumberColumnNames" if the
            % size of ColumnNames does not match the number of arrays
            % provided.
            A1 = arrow.array([1, 2]);
            A2 = arrow.array(["A", "B"]);
            fcn = @() tc.FromArraysFcn(A1, A2, columnNames=["A", "B", "C"]);
            tc.verifyError(fcn, "arrow:tabular:WrongNumberColumnNames");
        end

        function FromArraysColumnNamesHasMissingString(tc)
            % Verify the fromArrays function throws an exception with the
            % identifier "MATLAB:validators:mustBeNonmissing" if 
            % ColumnNames is a string array with missing values.
            A1 = arrow.array([1, 2]);
            A2 = arrow.array(["A", "B"]);
            fcn = @() tc.FromArraysFcn(A1, A2, columnNames=["A", missing]);
            tc.verifyError(fcn, "MATLAB:validators:mustBeNonmissing");
        end
    
        function FromArraysNoInputs(tc)
            % Verify the fromArrays function constructs the expected
            % Tabular object when zero input arguments are provided.
            tabularObj = tc.FromArraysFcn();
            tc.verifyEqual(tabularObj.NumRows, int64(0));
            tc.verifyEqual(tabularObj.NumColumns, int32(0));
            tc.verifyEqual(tabularObj.ColumnNames, string.empty(1, 0));
        end

    end

    methods (Test, TestTags={'get.Column'})

        function GetColumnByName(tc)
            % Verify that columns can be accessed by name.
            matlabArray1 = [1; 2; 3];
            matlabArray2 = ["A"; "B"; "C"];
            matlabArray3 = [true; false; true];

            arrowArray1 = arrow.array(matlabArray1);
            arrowArray2 = arrow.array(matlabArray2);
            arrowArray3 = arrow.array(matlabArray3);

            tabularObj = tc.FromArraysFcn(...
                arrowArray1, ...
                arrowArray2, ...
                arrowArray3, ...
                ColumnNames=["A", "B", "C"] ...
            );

            actual = tabularObj.column("A");
            tc.verifyEqual(actual, tc.makeColumnFromArray(arrowArray1));

            actual = tabularObj.column("B");
            tc.verifyEqual(actual, tc.makeColumnFromArray(arrowArray2));

            actual = tabularObj.column("C");
            tc.verifyEqual(actual, tc.makeColumnFromArray(arrowArray3));
        end

        function GetColumnByNameWithEmptyString(tc)
            % Verify that a column whose name is the empty string ("")
            % can be accessed using the column() method.
            matlabArray1 = [1; 2; 3];
            matlabArray2 = ["A"; "B"; "C"];
            matlabArray3 = [true; false; true];

            arrowArray1 = arrow.array(matlabArray1);
            arrowArray2 = arrow.array(matlabArray2);
            arrowArray3 = arrow.array(matlabArray3);

            tabularObj = tc.FromArraysFcn(...
                arrowArray1, ...
                arrowArray2, ...
                arrowArray3, ...
                ColumnNames=["A", "", "C"] ...
            );

            column = tabularObj.column("");
            tc.verifyEqual(column, tc.makeColumnFromArray(arrowArray2));
        end

        function GetColumnByNameWithWhitespace(tc)
            % Verify that a column whose name contains only whitespace
            % characters can be accessed using the column() method.
            matlabArray1 = [1; 2; 3];
            matlabArray2 = ["A"; "B"; "C"];
            matlabArray3 = [true; false; true];

            arrowArray1 = arrow.array(matlabArray1);
            arrowArray2 = arrow.array(matlabArray2);
            arrowArray3 = arrow.array(matlabArray3);

            tabularObj = tc.FromArraysFcn(...
                arrowArray1, ...
                arrowArray2, ...
                arrowArray3, ...
                ColumnNames=[" ", "  ", "   "] ...
            );

            column = tabularObj.column(" ");
            tc.verifyEqual(column, tc.makeColumnFromArray(arrowArray1));

            column = tabularObj.column("  ");
            tc.verifyEqual(column, tc.makeColumnFromArray(arrowArray2));

            column = tabularObj.column("   ");
            tc.verifyEqual(column, tc.makeColumnFromArray(arrowArray3));
        end
    
        function GetColumnErrorIfColumnNameDoesNotExist(tc)
            % Verify that an error is thrown when trying to access a column
            % with a name that is not part of the Schema of the Tabular 
            % object.
            matlabArray1 = [1; 2; 3];
            matlabArray2 = ["A"; "B"; "C"];
            matlabArray3 = [true; false; true];

            arrowArray1 = arrow.array(matlabArray1);
            arrowArray2 = arrow.array(matlabArray2);
            arrowArray3 = arrow.array(matlabArray3);

            tabularObj = tc.FromArraysFcn(...
                arrowArray1, ...
                arrowArray2, ...
                arrowArray3, ...
                ColumnNames=["A", "B", "C"] ...
            );

            % Matching should be case-sensitive.
            name = "a";
            tc.verifyError(@() tabularObj.column(name), "arrow:tabular:schema:AmbiguousFieldName");

            name = "aA";
            tc.verifyError(@() tabularObj.column(name), "arrow:tabular:schema:AmbiguousFieldName");

            name = "D";
            tc.verifyError(@() tabularObj.column(name), "arrow:tabular:schema:AmbiguousFieldName");

            name = "";
            tc.verifyError(@() tabularObj.column(name), "arrow:tabular:schema:AmbiguousFieldName");

            name = " ";
            tc.verifyError(@() tabularObj.column(name), "arrow:tabular:schema:AmbiguousFieldName");
        end

        function GetColumnErrorIfAmbiguousColumnName(tc)
            % Verify that an error is thrown when trying to access a column
            % with a name that is ambiguous / occurs more than once in the
            % Schema of the Tabular object.
            tabularObj = tc.FromArraysFcn(...
                arrow.array([1, 2, 3]), ...
                arrow.array(["A", "B", "C"]), ...
                arrow.array([true, false, true]), ...
                arrow.array([days(1), days(2), days(3)]), ...
                ColumnNames=["A", "A", "B", "B"] ...
            );

            name = "A";
            tc.verifyError(@() tabularObj.column(name), "arrow:tabular:schema:AmbiguousFieldName");

            name = "B";
            tc.verifyError(@() tabularObj.column(name), "arrow:tabular:schema:AmbiguousFieldName");
        end
    
        function GetColumnByNameWithChar(tc)
            % Verify that the column method works when supplied a char
            % vector as input.
            matlabArray1 = [1; 2; 3];
            matlabArray2 = ["A"; "B"; "C"];
            matlabArray3 = [true; false; true];

            arrowArray1 = arrow.array(matlabArray1);
            arrowArray2 = arrow.array(matlabArray2);
            arrowArray3 = arrow.array(matlabArray3);

            tabularObj = tc.FromArraysFcn(...
                arrowArray1, ...
                arrowArray2, ...
                arrowArray3, ...
                ColumnNames=["", "B", "123"] ...
            );

            % Should match the first column whose name is the
            % empty string ("").
            name = char.empty(0, 0);
            column = tabularObj.column(name);
            tc.verifyEqual(column, tc.makeColumnFromArray(arrowArray1));
            
            name = char.empty(0, 1);
            column = tabularObj.column(name);
            tc.verifyEqual(column, tc.makeColumnFromArray(arrowArray1));

            name = char.empty(1, 0);
            column = tabularObj.column(name);
            tc.verifyEqual(column, tc.makeColumnFromArray(arrowArray1));

            % Should match the second column whose name is "B".
            name = 'B';
            column = tabularObj.column(name);
            tc.verifyEqual(column, tc.makeColumnFromArray(arrowArray2));

            % Should match the third column whose name is "123".
            name = '123';
            column = tabularObj.column(name);
            tc.verifyEqual(column, tc.makeColumnFromArray(arrowArray3));
        end

        function GetColumnErrorIfColumnNameIsNonScalar(tc)
            % Verify that an error is thrown if a nonscalar string array is
            % specified as a column name to the column method.
            tabularObj = tc.FromArraysFcn(...
                arrow.array([1, 2, 3]), ...
                arrow.array(["A", "B", "C"]), ...
                arrow.array([true, false, true]), ...
                ColumnNames=["A", "B", "C"] ...
            );

            name = ["A", "B", "C"];
            tc.verifyError(@() tabularObj.column(name), "arrow:badsubscript:NonScalar");

            name = ["A";  "B"; "C"];
            tc.verifyError(@() tabularObj.column(name), "arrow:badsubscript:NonScalar");
        end

        function GetColumnEmptyTabularObjectColumnIndexError(tc)
            % Verify an exception is thrown when the column() method is
            % invoked on a Tabular object with zero columns.
            TOriginal = table();
            tabularObj = tc.ConstructionFcn(TOriginal);
            fcn = @() tabularObj.column(1);
            id = tc.ErrorIdentifierPrefix + "NumericIndexWithEmpty" + tc.ShortClassName;
            tc.verifyError(fcn, id);
        end

        function GetColumnInvalidNumericIndexError(tc)
            % Verify an exception is thrown if the column method is called
            % with a numeric index value that is greater than the number of 
            % columns.
            TOriginal = table(1, 2, 3);
            tabularObj = tc.ConstructionFcn(TOriginal);
            fcn = @() tabularObj.column(4);
            tc.verifyError(fcn, tc.ErrorIdentifierPrefix + "InvalidNumericColumnIndex");
        end

        function GetColumnUnsupportedColumnIndexType(tc)
            % Verify an exception is thrown if the column method is called
            % with an invalid datatype as the column index.
            TOriginal = table(1, 2, 3);
            tabularObj = tc.ConstructionFcn(TOriginal);
            fcn = @() tabularObj.column(datetime(2022, 1, 3));
            tc.verifyError(fcn, "arrow:badsubscript:UnsupportedIndexType");
        end

        function GetColumnErrorIfIndexIsNonScalar(tc)
            % Verify an exception is thrown if the column method is called
            % with a non-scalar index value.
            TOriginal = table(1, 2, 3);
            tabularObj = tc.ConstructionFcn(TOriginal);
            fcn = @() tabularObj.column([1 2]);
            tc.verifyError(fcn, "arrow:badsubscript:NonScalar");
        end

        function GetColumnErrorIfIndexIsNonPositive(tc)
            % Verify an exception is thrown if the column method is called
            % with a non-positive index value.
            TOriginal = table(1, 2, 3);
            tabularObj = tc.ConstructionFcn(TOriginal);
            fcn = @() tabularObj.column(-1);
            tc.verifyError(fcn, "arrow:badsubscript:NonPositive");
        end

    end

    methods (Test, TestTags={'isequal'})

        function TestIsEqualTrue(tc)
            % Verify two tabular objects are considered equal if:
            %   1. They have the same schema
            %   2. Their corresponding columns are equal

            a1 = arrow.array([1 2 3]);
            a2 = arrow.array(["A" "B" "C"]);
            a3 = arrow.array([true true false]);

            tabularObj1 = tc.FromArraysFcn(a1, a2, a3, ...
                ColumnNames=["A", "B", "C"]);
            tabularObj2 = tc.FromArraysFcn(a1, a2, a3, ...
                ColumnNames=["A", "B", "C"]);
            tc.verifyTrue(isequal(tabularObj1, tabularObj2));

            % Compare tabular objects that have zero columns.
            tabularObj3 = tc.FromArraysFcn();
            tabularObj4 = tc.FromArraysFcn();
            tc.verifyTrue(isequal(tabularObj3, tabularObj4));

            % Compare tabular objects that have zero rows
            a4 = arrow.array([]);
            a5 = arrow.array(strings(0, 0));
            rb5 = tc.FromArraysFcn(a4, a5, ColumnNames=["D" "E"]);
            rb6 = tc.FromArraysFcn(a4, a5, ColumnNames=["D" "E"]);
            tc.verifyTrue(isequal(rb5, rb6));

            % Call isequal with more than two arguments
            tc.verifyTrue(isequal(tabularObj3, tabularObj4, tabularObj3, tabularObj4));
        end

        function TestIsEqualFalse(tc)
            % Verify isequal returns false when expected.
            a1 = arrow.array([1 2 3]);
            a2 = arrow.array(["A" "B" "C"]);
            a3 = arrow.array([true true false]);
            a4 = arrow.array(["A" missing "C"]); 
            a5 = arrow.array([1 2]);
            a6 = arrow.array(["A" "B"]);
            a7 = arrow.array([true true]);

            tabularObj1 = tc.FromArraysFcn(a1, a2, a3, ...
                ColumnNames=["A", "B", "C"]);
            tabularObj2 = tc.FromArraysFcn(a1, a2, a3, ...
                ColumnNames=["D", "E", "F"]);
            tabularObj3 = tc.FromArraysFcn(a1, a4, a3, ...
                ColumnNames=["A", "B", "C"]);
            tabularObj4 = tc.FromArraysFcn(a5, a6, a7, ...
                ColumnNames=["A", "B", "C"]);
            tabularObj5 = tc.FromArraysFcn(a1, a2, a3, a1, ...
                ColumnNames=["A", "B", "C", "D"]);

            % The column names are not equal
            tc.verifyFalse(isequal(tabularObj1, tabularObj2));

            % The columns are not equal
            tc.verifyFalse(isequal(tabularObj1, tabularObj3));

            % The number of rows are not equal
            tc.verifyFalse(isequal(tabularObj1, tabularObj4));

            % The number of columns are not equal
            tc.verifyFalse(isequal(tabularObj1, tabularObj5));

            % Call isequal with more than two arguments
            tc.verifyFalse(isequal(tabularObj1, tabularObj2, tabularObj3, tabularObj4));
        end
   
    end

end