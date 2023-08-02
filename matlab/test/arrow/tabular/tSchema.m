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

classdef tSchema < matlab.unittest.TestCase
% Tests for the arrow.tabular.Schema class and the associated arrow.schema
% construction function.

    methods(Test)

        function ErrorIfUnsupportedInputType(testCase)
            % Verify that an error is thrown by arrow.schema if an
            % unsupported input argument is supplied.
        end

        function ErrorIfUnsupportedConstructorInputs(testCase)
            % Verify that an error is thrown by the constructor of
            % arrow.tabular.Schema if unsupported arguments are passed to
            % the constructor.
        end

        function ErrorIfTooFewInputs(testCase)
            % Verify that an error is thrown by arrow.schema if too few
            % input arguments are supplied.
        end

        function ErrorIfTooManyInputs(testCase)
            % Verify that an error is thrown by arrow.schema if too many
            % input arguments are supplied.
        end

        function ClassType(testCase)
            % Verify that the class type of the object returned by a call
            % to arrow.schema is "arrow.tabular.Schema".
            schema = arrow.schema(arrow.field("A", arrow.uint8));
            testCase.verifyInstanceOf(schema, "arrow.tabular.Schema");
        end

        function ConstructSchemaFromProxy(testCase)
            % Verify that an arrow.tabular.Schema instance can be
            % constructred directly from an existing
            % arrow.tabular.proxy.Schema Proxy instance.
        end

        function Fields(testCase)
            % Verify that the Fields property returns an expected array of
            % Field objects.
        end

        function FieldNames(testCase)
            % Verify that the FieldNames property returns an expected
            % string array of field names.
        end

        function FieldNamesNoSetter(testCase)
            % Verify that an error is thrown when trying to set the value
            % of the FieldNames property.
        end

        function NumFieldsNoSetter(testCase)
            % Verify than an error is thrown when trying to set the value
            % of the NumFields property.
        end

        function FieldsNoSetter(testCase)
            % Verify that an error is thrown when trying to set the value
            % of the Fields property.
        end

        function NumFields(testCase)
            % Verify that the NumFields property returns an execpted number
            % of fields.
        end

        function ErrorIfInvalidNumericFieldIndex(testCase)
            % Verify that an error is thrown if an invalid numeric index is
            % supplied to the field method (e.g. -1.1, NaN, Inf, etc.).
        end

        function GetFieldByIndex(testCase)
            % Verify that Fields can be accessed using a numeric index.
        end

        function GetFieldByName(testCase)
            % Verify that Fields can be accessed using a field name.
        end

        function ErrorIfInvalidFieldIndex(testCase)
            % Verify that an error is thrown when trying to access a field
            % with an invalid numeric index (i.e. less than 1 or greater
            % than NumFields).
        end

        function ErrorIfInvalidFieldName(testCase)
            % Verify that an error is thrown when trying to access a field
            % with a name that is not part of the schema.
        end

        function ErrorIfAmbiguousFieldName(testCase)
            % Verify that an error is thrown when trying to access a field
            % with a name that is ambiguous / occurs more than once in the
            % schema
        end

        function SupportedFieldTypes(testCase)
            % Verify that a Schema can be created from Fields with any
            % supported Type.
        end

        function UnicodeFieldNames(testCase)
            % Verify that Field names containing Unicode characters are 
            % preserved with the FieldNames property.
            smiley = "😀";
            tree =  "🌲";
            mango = "🥭";
            expectedFieldNames = [smiley, tree, mango];
            f1 = arrow.field(smiley, arrow.uint8);
            f2 = arrow.field(tree, arrow.uint16);
            f3 = arrow.field(mango, arrow.uint32);
            fields = [f1, f2, f3];
            schema = arrow.schema(fields);
            actualFieldNames = schema.FieldNames;
            testCase.verifyEqual(actualFieldNames, expectedFieldNames);
        end

        function EmptyFieldNames(testCase)
            % Verify that Field names which are the empty string are 
            % preserved with the FieldNames property.
        end

        function EmptySchema(testCase)
            % Verify that a Schema with no Fields can be created.
            % TODO: Decide whether construction of Schema objects with no
            % Fields should be supported.
        end

    end

end