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
            testCase.verifyError(@() arrow.schema("test"), "MATLAB:validation:UnableToConvert");
        end

        function ErrorIfUnsupportedConstructorInputs(testCase)
            % Verify that an error is thrown by the constructor of
            % arrow.tabular.Schema if unsupported arguments are passed to
            % the constructor.
            testCase.verifyError(@() arrow.tabular.Schema("test"), "MATLAB:validation:UnableToConvert");
        end

        function ErrorIfTooFewInputs(testCase)
            % Verify that an error is thrown by arrow.schema if too few
            % input arguments are supplied.
            testCase.verifyError(@() arrow.schema(), "MATLAB:minrhs");
        end

        function ErrorIfTooManyInputs(testCase)
            % Verify that an error is thrown by arrow.schema if too many
            % input arguments are supplied.
            testCase.verifyError(@() arrow.schema("a", "b", "c"), "MATLAB:TooManyInputs");
        end

        function ClassType(testCase)
            % Verify that the class type of the object returned by a call
            % to arrow.schema is "arrow.tabular.Schema".
            schema = arrow.schema(arrow.field("A", arrow.uint8));
            testCase.verifyInstanceOf(schema, "arrow.tabular.Schema");
        end

        function ConstructSchemaFromProxy(testCase)
            % Verify that an arrow.tabular.Schema instance can be
            % constructed directly from an existing
            % arrow.tabular.proxy.Schema Proxy instance.
            schema1 = arrow.schema(arrow.field("a", arrow.uint8));
            % Construct an instance of arrow.tabular.Schema directly from a
            % Proxy of type "arrow.tabular.proxy.Schema".
            schema2 = arrow.tabular.Schema(schema1.Proxy);
            testCase.verifyEqual(schema1.FieldNames, schema2.FieldNames);
            testCase.verifyEqual(schema1.NumFields, schema2.NumFields);
        end

        function Fields(testCase)
            % Verify that the Fields property returns an expected array of
            % Field objects.
            f1 = arrow.field("A", arrow.uint8);
            f2 = arrow.field("B", arrow.uint16);
            f3 = arrow.field("C", arrow.uint32);
            expectedFields = [f1, f2, f3];
            schema = arrow.schema(expectedFields);

            actualFields = schema.Fields;

            testCase.verifyEqual(actualFields(1).Name, expectedFields(1).Name);
            testCase.verifyEqual(actualFields(1).Type.ID, expectedFields(1).Type.ID);
            testCase.verifyEqual(actualFields(2).Name, expectedFields(2).Name);
            testCase.verifyEqual(actualFields(2).Type.ID, expectedFields(2).Type.ID);
            testCase.verifyEqual(actualFields(3).Name, expectedFields(3).Name);
            testCase.verifyEqual(actualFields(3).Type.ID, expectedFields(3).Type.ID);
        end

        function FieldNames(testCase)
            % Verify that the FieldNames property returns an expected
            % string array of field names.
            expectedFieldNames = ["A"        , "B"          , "C"];
            schema = arrow.schema([...
                arrow.field(expectedFieldNames(1), arrow.uint8), ...
                arrow.field(expectedFieldNames(2), arrow.uint16), ...
                arrow.field(expectedFieldNames(3), arrow.uint32) ...
            ]);
            actualFieldNames = schema.FieldNames;
            testCase.verifyEqual(actualFieldNames, expectedFieldNames);
        end

        function FieldNamesNoSetter(testCase)
            % Verify that an error is thrown when trying to set the value
            % of the FieldNames property.
            schema = arrow.schema(arrow.field("A", arrow.uint8));
            testCase.verifyError(@() setfield(schema, "FieldNames", "B"), "MATLAB:class:SetProhibited");
        end

        function NumFieldsNoSetter(testCase)
            % Verify than an error is thrown when trying to set the value
            % of the NumFields property.
            schema = arrow.schema(arrow.field("A", arrow.uint8));
            testCase.verifyError(@() setfield(schema, "NumFields", 123), "MATLAB:class:SetProhibited");
        end

        function FieldsNoSetter(testCase)
            % Verify that an error is thrown when trying to set the value
            % of the Fields property.
            schema = arrow.schema(arrow.field("A", arrow.uint8));
            testCase.verifyError(@() setfield(schema, "Fields", arrow.field("B", arrow.uint8)), "MATLAB:class:SetProhibited");
        end

        function NumFields(testCase)
            % Verify that the NumFields property returns an expected number
            % of fields.
            schema = arrow.schema([...
                arrow.field("A", arrow.uint8), ...
                arrow.field("B", arrow.uint16), ...
                arrow.field("C", arrow.uint32) ...
            ]);
            expectedNumFields = int32(3);
            actualNumFields = schema.NumFields;
            testCase.verifyEqual(actualNumFields, expectedNumFields);
        end

        function ErrorIfUnsupportedFieldIndex(testCase)
            % Verify that an error is thrown if an invalid field index is
            % supplied to the field method (e.g. -1.1, NaN, {1}, etc.).
            schema = arrow.schema([...
                arrow.field("A", arrow.uint8), ...
                arrow.field("B", arrow.uint16), ...
                arrow.field("C", arrow.uint32) ...
            ]);

            index = [];
            testCase.verifyError(@() schema.field(index), "arrow:badsubscript:NonScalar");

            index = 0;
            testCase.verifyError(@() schema.field(index), "arrow:badsubscript:NonPositive");

            index = -1;
            testCase.verifyError(@() schema.field(index), "arrow:badsubscript:NonPositive");

            index = 1.23;
            testCase.verifyError(@() schema.field(index), "arrow:badsubscript:NonInteger");

            index = NaN;
            testCase.verifyError(@() schema.field(index), "arrow:badsubscript:NonInteger");

            index = {1};
            testCase.verifyError(@() schema.field(index), "arrow:badsubscript:UnsupportedIndexType");

            index = [1; 1];
            testCase.verifyError(@() schema.field(index), "arrow:badsubscript:NonScalar");
        end

        function GetFieldByIndex(testCase)
            % Verify that Fields can be accessed using a numeric index.
            schema = arrow.schema([...
                arrow.field("A", arrow.uint8), ...
                arrow.field("B", arrow.uint16), ...
                arrow.field("C", arrow.uint32) ...
            ]);

            field = schema.field(1);
            testCase.verifyEqual(field.Name, "A");
            testCase.verifyEqual(field.Type.ID, arrow.type.ID.UInt8);

            field = schema.field(2);
            testCase.verifyEqual(field.Name, "B");
            testCase.verifyEqual(field.Type.ID, arrow.type.ID.UInt16);

            field = schema.field(3);
            testCase.verifyEqual(field.Name, "C");
            testCase.verifyEqual(field.Type.ID, arrow.type.ID.UInt32);
        end

        function GetFieldByName(testCase)
            % Verify that Fields can be accessed using a field name.
            schema = arrow.schema([...
                arrow.field("A", arrow.uint8), ...
                arrow.field("B", arrow.uint16), ...
                arrow.field("C", arrow.uint32) ...
            ]);

            field = schema.field("A");
            testCase.verifyEqual(field.Name, "A");
            testCase.verifyEqual(field.Type.ID, arrow.type.ID.UInt8);

            field = schema.field("B");
            testCase.verifyEqual(field.Name, "B");
            testCase.verifyEqual(field.Type.ID, arrow.type.ID.UInt16);

            field = schema.field("C");
            testCase.verifyEqual(field.Name, "C");
            testCase.verifyEqual(field.Type.ID, arrow.type.ID.UInt32);
        end

        function GetFieldByNameWithEmptyString(testCase)
            % Verify that a Field whose name is the empty string ("")
            % can be accessed using the field() method.
            schema = arrow.schema([...
                arrow.field("A", arrow.uint8), ...
                arrow.field("", arrow.uint16), ...
                arrow.field("C", arrow.uint32) ...
            ]);

            field = schema.field("");

            testCase.verifyEqual(field.Name, "");
            testCase.verifyEqual(field.Type.ID, arrow.type.ID.UInt16);
        end

        function GetFieldByNameWithWhitespace(testCase)
            % Verify that a Field whose name contains only whitespace
            % characters can be accessed using the field() method.
            schema = arrow.schema([...
                arrow.field(" ", arrow.uint8), ...
                arrow.field("  ", arrow.uint16), ...
                arrow.field("   ", arrow.uint32) ...
            ]);

            field = schema.field(" ");
            testCase.verifyEqual(field.Name, " ");
            testCase.verifyEqual(field.Type.ID, arrow.type.ID.UInt8);

            field = schema.field("  ");
            testCase.verifyEqual(field.Name, "  ");
            testCase.verifyEqual(field.Type.ID, arrow.type.ID.UInt16);

            field = schema.field("   ");
            testCase.verifyEqual(field.Name, "   ");
            testCase.verifyEqual(field.Type.ID, arrow.type.ID.UInt32);
        end

        function ErrorIfIndexIsOutOfRange(testCase)
            % Verify that an error is thrown when trying to access a field
            % with an invalid numeric index (e.g. greater than NumFields).
            schema = arrow.schema([...
                arrow.field("A", arrow.uint8), ...
                arrow.field("B", arrow.uint16), ...
                arrow.field("C", arrow.uint32) ...
            ]);

            % Index is greater than NumFields.
            index = 100;
            testCase.verifyError(@() schema.field(index), "arrow:index:OutOfRange");
        end

        function ErrorIfFieldNameDoesNotExist(testCase)
            % Verify that an error is thrown when trying to access a field
            % with a name that is not part of the schema.
            schema = arrow.schema([...
                arrow.field("A", arrow.uint8), ...
                arrow.field("B", arrow.uint16), ...
                arrow.field("C", arrow.uint32) ...
            ]);

            % Matching should be case-sensitive.
            fieldName = "a";
            testCase.verifyError(@() schema.field(fieldName), "arrow:tabular:schema:AmbiguousFieldName");

            fieldName = "aA";
            testCase.verifyError(@() schema.field(fieldName), "arrow:tabular:schema:AmbiguousFieldName");

            fieldName = "D";
            testCase.verifyError(@() schema.field(fieldName), "arrow:tabular:schema:AmbiguousFieldName");

            fieldName = "";
            testCase.verifyError(@() schema.field(fieldName), "arrow:tabular:schema:AmbiguousFieldName");

            fieldName = " ";
            testCase.verifyError(@() schema.field(fieldName), "arrow:tabular:schema:AmbiguousFieldName");
        end

        function ErrorIfAmbiguousFieldName(testCase)
            % Verify that an error is thrown when trying to access a field
            % with a name that is ambiguous / occurs more than once in the
            % schema.
            schema = arrow.schema([...
                arrow.field("A", arrow.uint8), ...
                arrow.field("A", arrow.uint16), ...
                arrow.field("B", arrow.uint32), ...
                arrow.field("B", arrow.uint32)
            ]);

            fieldName = "A";
            testCase.verifyError(@() schema.field(fieldName), "arrow:tabular:schema:AmbiguousFieldName");

            fieldName = "B";
            testCase.verifyError(@() schema.field(fieldName), "arrow:tabular:schema:AmbiguousFieldName");
        end

        function SupportedFieldTypes(testCase)
            % Verify that a Schema can be created from Fields with any
            % supported Type.
            fields = [ ...
                arrow.field("A", arrow.uint8), ...
                arrow.field("B", arrow.uint16), ...
                arrow.field("C", arrow.uint32), ...
                arrow.field("D", arrow.uint64), ...
                arrow.field("E", arrow.int8), ...
                arrow.field("F", arrow.int16), ...
                arrow.field("G", arrow.int32), ...
                arrow.field("H", arrow.int64), ...
                arrow.field("I", arrow.float32), ...
                arrow.field("J", arrow.float64), ...
                arrow.field("K", arrow.boolean), ...
                arrow.field("L", arrow.string), ...
                arrow.field("M", arrow.timestamp), ...
            ];

            schema = arrow.schema(fields);

            testCase.verifyEqual(schema.field("A").Type.ID, arrow.type.ID.UInt8);
            testCase.verifyEqual(schema.field("B").Type.ID, arrow.type.ID.UInt16);
            testCase.verifyEqual(schema.field("C").Type.ID, arrow.type.ID.UInt32);
            testCase.verifyEqual(schema.field("D").Type.ID, arrow.type.ID.UInt64);
            testCase.verifyEqual(schema.field("E").Type.ID, arrow.type.ID.Int8);
            testCase.verifyEqual(schema.field("F").Type.ID, arrow.type.ID.Int16);
            testCase.verifyEqual(schema.field("G").Type.ID, arrow.type.ID.Int32);
            testCase.verifyEqual(schema.field("H").Type.ID, arrow.type.ID.Int64);
            testCase.verifyEqual(schema.field("I").Type.ID, arrow.type.ID.Float32);
            testCase.verifyEqual(schema.field("J").Type.ID, arrow.type.ID.Float64);
            testCase.verifyEqual(schema.field("K").Type.ID, arrow.type.ID.Boolean);
            testCase.verifyEqual(schema.field("L").Type.ID, arrow.type.ID.String);
            testCase.verifyEqual(schema.field("M").Type.ID, arrow.type.ID.Timestamp);
        end

        function UnicodeFieldNames(testCase)
            % Verify that Field names containing Unicode characters are 
            % preserved with the FieldNames property.
            smiley = "😀";
            tree =  "🌲";
            mango = "🥭";
            expectedFieldNames = [smiley, tree, mango];

            f1 = arrow.field(expectedFieldNames(1), arrow.uint8);
            f2 = arrow.field(expectedFieldNames(2), arrow.uint16);
            f3 = arrow.field(expectedFieldNames(3), arrow.uint32);
            fields = [f1, f2, f3];

            schema = arrow.schema(fields);

            actualFieldNames = schema.FieldNames;

            testCase.verifyEqual(actualFieldNames, expectedFieldNames);
        end

        function EmptyFieldNames(testCase)
            % Verify that Field names which are the empty string are 
            % preserved with the FieldNames property.
            expectedFieldNames = ["", "B", "C"];
            schema = arrow.schema([...
                arrow.field("", arrow.uint8), ...
                arrow.field("B", arrow.uint16), ...
                arrow.field("C", arrow.uint32)
            ]);
            actualFieldNames = schema.FieldNames;
            testCase.verifyEqual(actualFieldNames, expectedFieldNames);
        end

        function EmptySchema(testCase)
            % Verify that a Schema with no Fields can be created.

            % 0x0 empty Field array.
            fields = arrow.type.Field.empty(0, 0);
            schema = arrow.schema(fields);
            testCase.verifyEqual(schema.NumFields, int32(0));
            testCase.verifyEqual(schema.FieldNames, string.empty(1, 0));
            testCase.verifyEqual(schema.Fields, arrow.type.Field.empty(0, 0));
            testCase.verifyError(@() schema.field(0), "arrow:badsubscript:NonPositive");
            testCase.verifyError(@() schema.field(1), "arrow:index:EmptyContainer");

            % 0x1 empty Field array.
            fields = arrow.type.Field.empty(0, 1);
            schema = arrow.schema(fields);
            testCase.verifyEqual(schema.NumFields, int32(0));
            testCase.verifyEqual(schema.FieldNames, string.empty(1, 0));
            testCase.verifyEqual(schema.Fields, arrow.type.Field.empty(0, 0));
            testCase.verifyError(@() schema.field(0), "arrow:badsubscript:NonPositive");
            testCase.verifyError(@() schema.field(1), "arrow:index:EmptyContainer");

            % 1x0 empty Field array.
            fields = arrow.type.Field.empty(1, 0);
            schema = arrow.schema(fields);
            testCase.verifyEqual(schema.NumFields, int32(0));
            testCase.verifyEqual(schema.FieldNames, string.empty(1, 0));
            testCase.verifyEqual(schema.Fields, arrow.type.Field.empty(0, 0));
            testCase.verifyError(@() schema.field(0), "arrow:badsubscript:NonPositive");
            testCase.verifyError(@() schema.field(1), "arrow:index:EmptyContainer");
        end

        function GetFieldByNameWithChar(testCase)
            % Verify that the field method works when supplied a char
            % vector as input.
            schema = arrow.schema([...
                arrow.field("", arrow.uint8), ...
                arrow.field("B", arrow.uint16), ...
                arrow.field("123", arrow.uint32)
            ]);

            % Should match the first field whose name is the
            % empty string ("").
            fieldName = char.empty(0, 0);
            field = schema.field(fieldName);
            testCase.verifyEqual(field.Name, "");
            testCase.verifyEqual(field.Type.ID, arrow.type.ID.UInt8);

            fieldName = char.empty(0, 1);
            field = schema.field(fieldName);
            testCase.verifyEqual(field.Name, "");
            testCase.verifyEqual(field.Type.ID, arrow.type.ID.UInt8);

            fieldName = char.empty(1, 0);
            field = schema.field(fieldName);
            testCase.verifyEqual(field.Name, "");
            testCase.verifyEqual(field.Type.ID, arrow.type.ID.UInt8);

            % Should match the second field whose name is "B".
            fieldName = 'B';
            field = schema.field(fieldName);
            testCase.verifyEqual(field.Name, "B");
            testCase.verifyEqual(field.Type.ID, arrow.type.ID.UInt16);

            % Should match the third field whose name is "123".
            fieldName = '123';
            field = schema.field(fieldName);
            testCase.verifyEqual(field.Name, "123");
            testCase.verifyEqual(field.Type.ID, arrow.type.ID.UInt32);
        end

        function ErrorIfNumericIndexIsNonScalar(testCase)
            % Verify that an error is thrown if a nonscalar numeric index
            % is supplied to the field method.
            schema = arrow.schema([...
                arrow.field("", arrow.uint8), ...
                arrow.field("B", arrow.uint16), ...
                arrow.field("123", arrow.uint32)
            ]);

            fieldName = [1, 2, 3];
            testCase.verifyError(@() schema.field(fieldName), "arrow:badsubscript:NonScalar");

            fieldName = [1; 2; 3];
            testCase.verifyError(@() schema.field(fieldName), "arrow:badsubscript:NonScalar");
        end

        function ErrorIfFieldNameIsNonScalar(testCase)
            % Verify that an error is thrown if a nonscalar string array is
            % specified as a field name to the field method.
            schema = arrow.schema([...
                arrow.field("", arrow.uint8), ...
                arrow.field("B", arrow.uint16), ...
                arrow.field("123", arrow.uint32)
            ]);

            fieldName = ["A", "B", "C"];
            testCase.verifyError(@() schema.field(fieldName), "arrow:badsubscript:NonScalar");

            fieldName = ["A";  "B"; "C"];
            testCase.verifyError(@() schema.field(fieldName), "arrow:badsubscript:NonScalar");
        end

        function TestIsEqualTrue(testCase)
            % Schema objects are considered equal if:
            %  1. They have the same number of fields
            %  2. Their corresponding Fields properties are equal

            schema1 = arrow.schema([...
                arrow.field("A", arrow.uint8), ...
                arrow.field("B", arrow.uint16), ...
                arrow.field("123", arrow.uint32)
            ]);
            schema2 = arrow.schema([...
                arrow.field("A", arrow.uint8), ...
                arrow.field("B", arrow.uint16), ...
                arrow.field("123", arrow.uint32)
            ]);

            % Create a Schema with zero fields
            schema3 = arrow.recordBatch(table).Schema;
            schema4 = arrow.recordBatch(table).Schema;
            
            testCase.verifyTrue(isequal(schema1, schema2));
            testCase.verifyTrue(isequal(schema3, schema4));
        end

        function TestIsEqualFalse(testCase)
            % Verify isequal returns false when expected.

            schema1 = arrow.schema([...
                arrow.field("A", arrow.uint8), ...
                arrow.field("B", arrow.uint16), ...
                arrow.field("123", arrow.uint32)
            ]);
            schema2 = arrow.schema([...
                arrow.field("A", arrow.uint8), ...
                arrow.field("B", arrow.uint16), ...
            ]);
            schema3 = arrow.schema([...
                arrow.field("A", arrow.float32), ...
                arrow.field("B", arrow.uint16), ...
            ]);
            schema4 = arrow.schema([...
                arrow.field("C", arrow.uint8), ...
                arrow.field("B", arrow.uint16), ...
            ]);

            % Create a Schema with zero fields
            schema5 = arrow.recordBatch(table).Schema;
            
            % Have different number of fields
            testCase.verifyFalse(isequal(schema1, schema2));

            % Fields properties are not equal
            testCase.verifyFalse(isequal(schema2, schema3));
            testCase.verifyFalse(isequal(schema2, schema4));
            testCase.verifyFalse(isequal(schema4, schema5));

            % Compare schema to double
            testCase.verifyFalse(isequal(schema4, 5));
        end

        function TestDisplaySchemaZeroFields(testCase)
            import arrow.internal.test.display.makeLinkString

            schema = arrow.schema(arrow.type.Field.empty(0, 0)); %#ok<NASGU>
            classnameLink = makeLinkString(FullClassName="arrow.tabular.Schema",...
                                            ClassName="Schema", BoldFont=true);
            expectedDisplay = "  Arrow " + classnameLink + " with 0 fields" + newline;
            expectedDisplay = char(expectedDisplay + newline);
            actualDisplay = evalc('disp(schema)');
            testCase.verifyEqual(actualDisplay, char(expectedDisplay));
        end

        function TestDisplaySchemaOneField(testCase)
            import arrow.internal.test.display.makeLinkString

            schema = arrow.schema(arrow.field("TestField", arrow.boolean())); %#ok<NASGU>
            classnameLink = makeLinkString(FullClassName="arrow.tabular.Schema",...
                                            ClassName="Schema", BoldFont=true);
            header = "  Arrow " + classnameLink + " with 1 field:" + newline;
            indent = "    ";

            if usejava("desktop")
                type = makeLinkString(FullClassName="arrow.type.BooleanType", ...
                                      ClassName="Boolean", BoldFont=true);
                name = "<strong>TestField</strong>: ";
                fieldLine = indent + name + type + newline;
            else
                fieldLine = indent + "TestField: Boolean" + newline;
            end
            expectedDisplay = join([header, fieldLine], newline);
            expectedDisplay = char(expectedDisplay + newline);
            actualDisplay = evalc('disp(schema)');
            testCase.verifyEqual(actualDisplay, char(expectedDisplay));
        end

        function TestDisplaySchemaField(testCase)
            import arrow.internal.test.display.makeLinkString

            field1 = arrow.field("Field1", arrow.timestamp());
            field2 = arrow.field("Field2", arrow.string());
            schema = arrow.schema([field1, field2]); %#ok<NASGU>
            classnameLink = makeLinkString(FullClassName="arrow.tabular.Schema",...
                                            ClassName="Schema", BoldFont=true);
            header = "  Arrow " + classnameLink + " with 2 fields:" + newline;

            indent = "    ";
            if usejava("desktop")
                type1 = makeLinkString(FullClassName="arrow.type.TimestampType", ...
                                       ClassName="Timestamp", BoldFont=true);
                field1String = "<strong>Field1</strong>: " + type1;
                type2 = makeLinkString(FullClassName="arrow.type.StringType", ...
                                      ClassName="String", BoldFont=true);
                field2String = "<strong>Field2</strong>: " + type2;
                fieldLine = indent + field1String + " | " + field2String + newline;
            else
                fieldLine = indent + "Field1: Timestamp | Field2: String" + newline;
            end

            expectedDisplay = join([header, fieldLine], newline);
            expectedDisplay = char(expectedDisplay + newline);
            actualDisplay = evalc('disp(schema)');
            testCase.verifyEqual(actualDisplay, char(expectedDisplay));
        end

    end

end
