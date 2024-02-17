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

classdef tField < matlab.unittest.TestCase
% Test class for arrow.type.Field and arrow.field.

    methods(Test)
        function TestBasic(testCase)
            name = "A";
            type = arrow.uint64;
            field = arrow.field(name, type);

            testCase.verifyEqual(field.Name, name);
            testCase.verifyEqual(field.Type.ID, type.ID);
        end

        function TestSupportedTypes(testCase)
            name = "name";
            supportedTypes = { ...
                                 arrow.uint8, ...
                                 arrow.uint16, ...
                                 arrow.uint32, ...
                                 arrow.uint64, ...
                                 arrow.int8, ...
                                 arrow.int16, ...
                                 arrow.int32, ...
                                 arrow.int64, ...
                                 arrow.boolean, ...
                                 arrow.float32, ...
                                 arrow.float64, ...
                                 arrow.string, ...
                                 arrow.timestamp, ...
                                 arrow.list(arrow.uint64()), ...
                                 arrow.struct(arrow.field("A", arrow.float32()))
                             };
            for ii = 1:numel(supportedTypes)
                supportedType = supportedTypes{ii};
                field = arrow.field(name, supportedType);
                testCase.verifyEqual(field.Name, name);
                testCase.verifyEqual(field.Type.ID, supportedType.ID);
            end
        end

        function TestNameUnicode(testCase)
            smiley = "😀";
            tree =  "🌲";
            mango = "🥭";

            type = arrow.uint64;
            field = arrow.field(smiley, type);

            testCase.verifyEqual(field.Name, smiley);
            testCase.verifyEqual(field.Type.ID, type.ID);

            field = arrow.field(tree, type);

            testCase.verifyEqual(field.Name, tree);
            testCase.verifyEqual(field.Type.ID, type.ID);

            field = arrow.field(mango, type);

            testCase.verifyEqual(field.Name, mango);
            testCase.verifyEqual(field.Type.ID, type.ID);
        end

        function TestErrorIfNameStringMissing(testCase)
            name = string(missing);
            type = arrow.uint64;
            testCase.verifyError(@() arrow.field(name, type), "MATLAB:validators:mustBeNonmissing");
        end

        function TestNameEmptyString(testCase)
            name = "";
            type = arrow.uint64;
            field = arrow.field(name, type);

            testCase.verifyEqual(field.Name, name);
            testCase.verifyEqual(field.Type.ID, type.ID);
        end

        function TestNameCharVector(testCase)
            name = 'ABC';
            type = arrow.uint64;
            field = arrow.field(name, type);

            testCase.verifyEqual(field.Name, string(name));
            testCase.verifyEqual(field.Type.ID, type.ID);
        end

        function TestNameNumber(testCase)
            name = 123;
            type = arrow.uint64;
            field = arrow.field(name, type);

            testCase.verifyEqual(field.Name, string(123));
            testCase.verifyEqual(field.Type.ID, type.ID);
        end

        function TestArrowTypeUnsupportedInput(testCase)
            name = "A";
            type = { 123 };
            testCase.verifyError(@() arrow.field(name, type), "MATLAB:validation:UnableToConvert");
        end

        function TestNameUnsupportedInput(testCase)
            name = table();
            type = arrow.uint64;
            testCase.verifyError(@() arrow.field(name, type), "MATLAB:validation:UnableToConvert");
        end

        function TestImmutableProperties(testCase)
            name = "A";
            type = arrow.uint64;
            field = arrow.field(name, type);

            testCase.verifyError(@() setfield(field, "Name", "NewValue"), "MATLAB:class:noSetMethod")
            testCase.verifyError(@() setfield(field, "Type", arrow.boolean), "MATLAB:class:noSetMethod")
        end

        function TestIsEqualScalarTrue(testCase)
            % Two scalar arrow.type.Field objects are equal if:
            %
            %  1. Their Name properties are equal
            %  2. Their Type properties are equal

            f1 = arrow.field("A", arrow.timestamp(TimeZone="America/New_York", TimeUnit="Second"));
            f2 = arrow.field("A", arrow.timestamp(TimeZone="America/New_York", TimeUnit="Second"));
            testCase.verifyTrue(isequal(f1, f2));
        end

        function TestIsEqualScalarFalse(testCase)
            % Verifies isequal returns false when expected. 
            f1 = arrow.field("A", arrow.timestamp(TimeZone="America/New_York", TimeUnit="Second"));
            f2 = arrow.field("B", arrow.timestamp(TimeZone="America/New_York", TimeUnit="Second"));
            f3 = arrow.field("A", arrow.timestamp(TimeZone="America/New_York", TimeUnit="Millisecond"));
            f4 = arrow.field("A", arrow.time32());

            % Name properties are not equal
            testCase.verifyFalse(isequal(f1, f2));

            % Type properties are not equal
            testCase.verifyFalse(isequal(f1, f3));

            % Type properties have different class types
            testCase.verifyFalse(isequal(f1, f4));

            % Compare arrow.type.Field and a string
            testCase.verifyFalse(isequal(f1, "A"));
        end

        function TestIsEqualNonScalarTrue(testCase)
            % Two nonscalar arrow.type.Field arrays are equal if:
            %  1. They have the same shape
            %  2. Their corresponding Field elements are equal
            
            f1 = arrow.field("A", arrow.timestamp(TimeZone="America/New_York", TimeUnit="Second"));
            f2 = arrow.field("B", arrow.string());
            fieldArray1 = [f1 f2 f1; f2 f1 f1];
            fieldArray2 = [f1 f2 f1; f2 f1 f1];

            testCase.verifyTrue(isequal(fieldArray1, fieldArray2));
            testCase.verifyTrue(isequal(fieldArray1, fieldArray2, fieldArray1));
        end

        function TestIsEqualEmptyFields(testCase)
            % Verify isequal returns the expected value when at least one
            % of the inputs is empty.
            
            f1 = arrow.type.Field.empty(1, 0);
            f2 = arrow.type.Field.empty(0, 1);
            f3 = arrow.type.Field.empty(0, 0);
            f4 = arrow.field("B", arrow.string());

            % Compare two 1x0 Field arrays
            testCase.verifyTrue(isequal(f1, f1));
            
            % Compare two 0x1 Field arrays
            testCase.verifyTrue(isequal(f2, f2));

            % Compare two 0x0 Field arrays
            testCase.verifyTrue(isequal(f3, f3));

            % Compare 1x0 and 0x1 Field arrays
            testCase.verifyFalse(isequal(f1, f2));

            % Compare 1x0 and 0x0 Field arrays
            testCase.verifyFalse(isequal(f1, f3));

            % Compare 0x1 and 0x0 Field arrays
            testCase.verifyFalse(isequal(f2, f3));

            % Compare 1x0 and 1x1 Field arrays
            testCase.verifyFalse(isequal(f1, f4));
        end

        function TestIsEqualNonScalarFalse(testCase)
            % Verifies isequal returns false when expected for non-scalar
            % Field arrays.
            
            f1 = arrow.field("A", arrow.timestamp(TimeZone="America/New_York", TimeUnit="Second"));
            f2 = arrow.field("B", arrow.string());
            f3 = arrow.field("B", arrow.date32());
            f4 = arrow.field("C", arrow.timestamp(TimeZone="America/New_York", TimeUnit="Second"));

            fieldArray1 = [f1 f2 f3; f4 f1 f2];
            fieldArray2 = reshape(fieldArray1, [3 2]);
            fieldArray3 = [f1 f3 f3; f4 f1 f2];
            fieldArray4 = [f4 f2 f3; f4 f1 f2];
            
            % They have different shapes
            testCase.verifyFalse(isequal(fieldArray1, fieldArray2));
            testCase.verifyFalse(isequal(fieldArray1, fieldArray2, fieldArray1));

            % Their corresponding elements are not equal - type mismatch
            testCase.verifyFalse(isequal(fieldArray1, fieldArray3));

            % Their corresponding elements are not equal - name mismatch
            testCase.verifyFalse(isequal(fieldArray1, fieldArray4));

            % Compare arrow.type.Field array and a string array
            testCase.verifyFalse(isequal(f1, strings(size(f1))));
        end

        function TestDisplay(testCase)
            % Verify the display of Field objects.
            %
            % Example:
            %
            %  Field with properties:
            %
            %        Name: FieldA
            %        Type: [1x2 arrow.type.Int32Type]

            import arrow.internal.test.display.verify
            import arrow.internal.test.display.makeLinkString
            import arrow.internal.test.display.makeDimensionString

            field = arrow.field("B", arrow.timestamp(TimeZone="America/Anchorage")); %#ok<NASGU>
            classnameLink = makeLinkString(FullClassName="arrow.type.Field", ClassName="Field", BoldFont=true);
            header = "  " + classnameLink + " with properties:" + newline;
            body = strjust(pad(["Name:"; "Type:"]));
            dimensionString = makeDimensionString([1 1]);
            fieldString = compose("[%s %s]", dimensionString, "arrow.type.TimestampType");
            body = body + " " + ["""B"""; fieldString];
            body = "    " + body;
            footer = string(newline);
            expectedDisplay = char(strjoin([header body' footer], newline));
            actualDisplay = evalc('disp(field)');
            verify(testCase, actualDisplay, expectedDisplay);
        end
    end
end
