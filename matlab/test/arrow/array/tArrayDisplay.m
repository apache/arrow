%TARRAYDISPLAY Unit tests for array display.

classdef tArrayDisplay < matlab.unittest.TestCase
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

    properties (TestParameter)
        EmptyArray
        ArrayWithOneElement
        ArrayWithMultipleElements
        ArrayWithOneNull
        ArrayWithMultipleNulls
    end

    methods(TestParameterDefinition, Static)
        function EmptyArray = initializeEmptyArray()
            EmptyArray.Boolean = arrow.array(logical.empty(0, 0));
            EmptyArray.UInt8 = arrow.array(uint8.empty(0, 0));
            EmptyArray.Int64 = arrow.array(int64.empty(0, 0));
            EmptyArray.Time64 = arrow.array(duration.empty(0, 0));
            EmptyArray.Timestamp = arrow.array(datetime.empty(0, 0));
            EmptyArray.String = arrow.array(string.empty(0, 0));
        end

        function ArrayWithOneElement = initializeArrayWithOneElement()
            boolStruct = struct(Array=arrow.array(true), String="true");
            ArrayWithOneElement.Boolean = boolStruct;
            
            uint8Struct = struct(Array=arrow.array(uint8(1)), String="1");
            ArrayWithOneElement.UInt8 = uint8Struct; 

            int64Struct = struct(Array=arrow.array(int64(2)), String="2");
            ArrayWithOneElement.Int64 = int64Struct;

            time64Struct = struct(Array=arrow.array(seconds(1)), String="00:00:01.000000");
            ArrayWithOneElement.Time64 = time64Struct; 

            timestampStruct = struct(Array=arrow.array(datetime(2023, 10, 20), TimeUnit="Second"), String="2023-10-20 00:00:00");
            ArrayWithOneElement.Timestamp = timestampStruct;

            stringStruct = struct(Array=arrow.array("A"), String="""A""");
            ArrayWithOneElement.String = stringStruct;
        end

        function ArrayWithMultipleElements = initializeArrayWithMultipleElements()
            boolStruct = struct(Array=arrow.array([true false true]), ...
                String="true | false | true");
            ArrayWithMultipleElements.Boolean = boolStruct;
            
            uint8Struct = struct(Array=arrow.array(uint8([1 10 100])), ...
                String="1 | 10 | 100");
            ArrayWithMultipleElements.UInt8 = uint8Struct; 

            int64Struct = struct(Array=arrow.array(int64([2 1 20])), ...
                String="2 | 1 | 20");
            ArrayWithMultipleElements.Int64 = int64Struct;

            time64Struct = struct(Array=arrow.array(seconds([1 2 3])), ...
                String="00:00:01.000000 | 00:00:02.000000 | 00:00:03.000000");
            ArrayWithMultipleElements.Time64 = time64Struct; 

            timestampStruct = struct(Array=arrow.array(datetime(2023, 10, 20) + days(0:2) , TimeUnit="Second"), ...
                String="2023-10-20 00:00:00 | 2023-10-21 00:00:00 | 2023-10-22 00:00:00");
            ArrayWithMultipleElements.Timestamp = timestampStruct;

            stringStruct = struct(Array=arrow.array(["A" "ABC" "ABCDE"]), ...
                String="""A"" | ""ABC"" | ""ABCDE""");
            ArrayWithMultipleElements.String = stringStruct;
        end

        function ArrayWithOneNull = initializeArrayWithOneNull()
            boolStruct = struct(Array=arrow.array([true false true], Valid=[1 3]), ...
                String="true | null | true");
            ArrayWithOneNull.Boolean = boolStruct;
            
            uint8Struct = struct(Array=arrow.array(uint8([1 10 100]), Valid=[2 3]), ...
                String="null | 10 | 100");
            ArrayWithOneNull.UInt8 = uint8Struct; 

            int64Struct = struct(Array=arrow.array(int64([2 1 20]), Valid=[1 2]), ...
                String="2 | 1 | null");
            ArrayWithOneNull.Int64 = int64Struct;

            time64Struct = struct(Array=arrow.array(seconds([1 NaN 3])), ...
                String="00:00:01.000000 | null | 00:00:03.000000");
            ArrayWithOneNull.Time64 = time64Struct; 

            timestampStruct = struct(Array=arrow.array([datetime(2023, 10, 20) + days(0:1) NaT] , TimeUnit="Second"), ...
                String="2023-10-20 00:00:00 | 2023-10-21 00:00:00 | null");
            ArrayWithOneNull.Timestamp = timestampStruct;

            stringStruct = struct(Array=arrow.array([missing "ABC" "ABCDE"]), ...
                String="null | ""ABC"" | ""ABCDE""");
            ArrayWithOneNull.String = stringStruct;
        end

        function ArrayWithMultipleNulls = initializeArrayWithMultipleNulls()
            boolStruct = struct(Array=arrow.array([true false true], Valid=1), ...
                String="true | null | null");
            ArrayWithMultipleNulls.Boolean = boolStruct;
            
            uint8Struct = struct(Array=arrow.array(uint8([1 10 100]), Valid=2), ...
                String="null | 10 | null");
            ArrayWithMultipleNulls.UInt8 = uint8Struct; 

            int64Struct = struct(Array=arrow.array(int64([2 1 20]), Valid=3), ...
                String="null | null | 20");
            ArrayWithMultipleNulls.Int64 = int64Struct;

            time64Struct = struct(Array=arrow.array(seconds([1 NaN NaN])), ...
                String="00:00:01.000000 | null | null");
            ArrayWithMultipleNulls.Time64 = time64Struct; 

            timestampStruct = struct(Array=arrow.array([datetime(2023, 10, 20) NaT NaT] , TimeUnit="Second"), ...
                String="2023-10-20 00:00:00 | null | null");
            ArrayWithMultipleNulls.Timestamp = timestampStruct;

            stringStruct = struct(Array=arrow.array([missing "ABC" missing]), ...
                String="null | ""ABC"" | null");
            ArrayWithMultipleNulls.String = stringStruct;
        end
    end

    methods (Test)

        function DisplayEmptyArray(testCase, EmptyArray)
            import arrow.internal.test.display.makeLinkString

            fullClassName = string(class(EmptyArray)); 
            displayName = extractAfter(fullClassName, "arrow.array.");
            classNameLink = makeLinkString(FullClassName=fullClassName, ...
                                           ClassName=displayName, ...
                                           BoldFont=true);
            zeroString = getNumString(0);
            header = compose("  %s with %s elements and %s null values" + newline, ...
                classNameLink, zeroString, zeroString);

            expectedDisplay = char(header + newline);
            actualDisplay = evalc('disp(EmptyArray)');
            testCase.verifyEqual(actualDisplay, expectedDisplay);
        end

        function DisplayArrayWithOneElement(testCase, ArrayWithOneElement)
            import arrow.internal.test.display.makeLinkString

            array = ArrayWithOneElement.Array;
            fullClassName = string(class(array)); 
            displayName = extractAfter(fullClassName, "arrow.array.");
            classNameLink = makeLinkString(FullClassName=fullClassName, ...
                                           ClassName=displayName, ...
                                           BoldFont=true);

            numElementString = getNumString(1);
            numNullString = getNumString(0);
            header = compose("  %s with %s element and %s null values:" + newline, ...
                classNameLink, numElementString, numNullString);

            body = "    " + ArrayWithOneElement.String + newline + newline;
            expectedDisplay = char(strjoin([header body], newline));
            actualDisplay = evalc('disp(array)');
            testCase.verifyEqual(actualDisplay, expectedDisplay);
        end

        function DisplayArrayWithMultipleElements(testCase, ArrayWithMultipleElements)
            import arrow.internal.test.display.makeLinkString

            array = ArrayWithMultipleElements.Array;
            fullClassName = string(class(array)); 
            displayName = extractAfter(fullClassName, "arrow.array.");
            classNameLink = makeLinkString(FullClassName=fullClassName, ...
                                           ClassName=displayName, ...
                                           BoldFont=true);

            numElementString = getNumString(3);
            numNullString = getNumString(0);
            header = compose("  %s with %s elements and %s null values:" + newline, ...
                classNameLink, numElementString, numNullString);

            body = "    " + ArrayWithMultipleElements.String + newline + newline;
            expectedDisplay = char(strjoin([header body], newline));
            actualDisplay = evalc('disp(array)');
            testCase.verifyEqual(actualDisplay, expectedDisplay);
        end

        function DisplayArrayWithOneNull(testCase, ArrayWithOneNull)
            import arrow.internal.test.display.makeLinkString

            array = ArrayWithOneNull.Array;
            fullClassName = string(class(array)); 
            displayName = extractAfter(fullClassName, "arrow.array.");
            classNameLink = makeLinkString(FullClassName=fullClassName, ...
                                           ClassName=displayName, ...
                                           BoldFont=true);

            numElementString = getNumString(3);
            numNullString = getNumString(1);
            header = compose("  %s with %s elements and %s null value:" + newline, ...
                classNameLink, numElementString, numNullString);

            body = "    " + ArrayWithOneNull.String + newline + newline;
            expectedDisplay = char(strjoin([header body], newline));
            actualDisplay = evalc('disp(array)');
            testCase.verifyEqual(actualDisplay, expectedDisplay);
        end

        function DisplayArrayWithMultipleNulls(testCase, ArrayWithMultipleNulls)
            import arrow.internal.test.display.makeLinkString

            array = ArrayWithMultipleNulls.Array;
            fullClassName = string(class(array)); 
            displayName = extractAfter(fullClassName, "arrow.array.");
            classNameLink = makeLinkString(FullClassName=fullClassName, ...
                                           ClassName=displayName, ...
                                           BoldFont=true);

            numElementString = getNumString(3);
            numNullString = getNumString(2);
            header = compose("  %s with %s elements and %s null values:" + newline, ...
                classNameLink, numElementString, numNullString);


            body = "    " + ArrayWithMultipleNulls.String + newline + newline;
            expectedDisplay = char(strjoin([header body], newline));
            actualDisplay = evalc('disp(array)');
            testCase.verifyEqual(actualDisplay, expectedDisplay);
        end

        function DisplayWindowSizeThree(testCase)
            import arrow.internal.test.display.makeLinkString
            array = arrow.array(1:8);
            fullClassName = string(class(array)); 
            displayName = extractAfter(fullClassName, "arrow.array.");
            classNameLink = makeLinkString(FullClassName=fullClassName, ...
                                           ClassName=displayName, ...
                                           BoldFont=true);

            numElementString = getNumString(8);
            numNullString = getNumString(0);
            header = compose("  %s with %s elements and %s null values:" + newline, ...
                classNameLink, numElementString, numNullString);

            body = "    1 | 2 | 3 | ... | 6 | 7 | 8" + newline + newline;
            expectedDisplay = char(strjoin([header body], newline));
            actualDisplay = evalc('disp(array)');
            testCase.verifyEqual(actualDisplay, expectedDisplay);

        end

        function DisplayStructArray(testCase)
            import arrow.internal.test.display.makeLinkString

            t = table((1:2)', ["A"; "B"], VariableNames=["Number", "Text"]);
            structArray = arrow.array(t); %#ok<NASGU>

            classNameLink = makeLinkString(FullClassName="arrow.array.StructArray", ...
                                           ClassName="StructArray", ...
                                           BoldFont=true);

            numElementString = getNumString(2);
            numNullString = getNumString(0);
            header = compose("  %s with %s elements and %s null values:" + newline, ...
                classNameLink, numElementString, numNullString);

            body =  "    -- is_valid: all not null" + newline + ...
                    "    -- child 0 type: double" + newline + ...
                    "        [" + newline + ...
                    "            1," + newline + ...
                    "            2" + newline + ...
                    "        ]" + newline + ...
                    "    -- child 1 type: string" + newline + ...
                    "        [" + newline + ...
                    "            ""A""," + newline + ...
                    "            ""B""" + newline + ...
                    "        ]" + newline + newline;
            expectedDisplay = char(strjoin([header body], newline));
            actualDisplay = evalc('disp(structArray)');
            testCase.verifyEqual(actualDisplay, expectedDisplay);
        end

        function DisplayListArray(testCase)
            import arrow.internal.test.display.makeLinkString

            Offsets = arrow.array(int32([0, 2, 3]));
            Values = arrow.array([1, 2, 3]);

            listArray = arrow.array.ListArray.fromArrays(Offsets, Values); %#ok<NASGU>

            classNameLink = makeLinkString(FullClassName="arrow.array.ListArray", ...
                                           ClassName="ListArray", ...
                                           BoldFont=true);

            numElementString = getNumString(2);
            numNullString = getNumString(0);
            header = compose("  %s with %s elements and %s null values:" + newline, ...
                classNameLink, numElementString, numNullString);

            body =  "    [" + newline + ... 
                    "        [" + newline + ...
                    "            1," + newline + ...
                    "            2" + newline + ...
                    "        ]," + newline + ...
                    "        [" + newline + ...
                    "            3" + newline + ...
                    "        ]" + newline + ...
                    "    ]" + newline + newline;
            
            expectedDisplay = char(strjoin([header body], newline));
            actualDisplay = evalc('disp(listArray)');
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