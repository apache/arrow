%TTYPEDISPLAY Unit tests verifying the display of all classes within the
%arrow.type.Type class hierarchy.

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

classdef tTypeDisplay < matlab.unittest.TestCase

    properties(TestParameter)
        TypeDisplaysOnlyID = {arrow.boolean(), ...
                              arrow.uint8(), ...
                              arrow.uint16(), ...
                              arrow.uint32(), ...
                              arrow.uint64(), ...
                              arrow.int8(), ...
                              arrow.int16(), ...
                              arrow.int32(), ...
                              arrow.int64(), ...
                              arrow.float32(), ...
                              arrow.float64(), ...
                              arrow.float64(), ...
                              arrow.string()}

        TimeType = {arrow.time32(TimeUnit="Second"), ...
                   arrow.time64(TimeUnit="Nanosecond")};

        DateType = {arrow.date32(), ...
                    arrow.date64()};
    end

    methods (Test)
        function EmptyTypeDisplay(testCase)
            % Verify the display of an empty arrow.type.Type instance.
            %
            % Example:
            %
            %  0x1 Type array with properties:
            %
            %    ID

            import arrow.internal.test.display.verify
            import arrow.internal.test.display.makeLinkString
            import arrow.internal.test.display.makeDimensionString

            type = arrow.type.Type.empty(0, 1);
            typeLink = makeLinkString(FullClassName="arrow.type.Type", ClassName="Type", BoldFont=true);
            dimensionString = makeDimensionString(size(type));
            header = "  " + dimensionString + " " + typeLink + " array with properties:" + newline;
            body = strjust(pad("ID"));
            body = "    " + body;
            footer = string(newline);
            expectedDisplay = char(strjoin([header body' footer], newline));
            actualDisplay = evalc('disp(type)');
            verify(testCase, actualDisplay, expectedDisplay);
        end

        function NonScalarArrayDifferentTypes(testCase)
            % Verify the display of a nonscalar, heterogeneous arrow.type.Type array:
            % 
            % Example:
            %
            %  1×2 heterogeneous FixedWidthType (Float32Type, TimestampType) array with properties:
            %
            %    ID

            import arrow.internal.test.display.verify
            import arrow.internal.test.display.makeLinkString
            import arrow.internal.test.display.makeDimensionString

            float32Type = arrow.float32();
            timestampType = arrow.timestamp();
            typeArray = [float32Type timestampType];

            heterogeneousLink =  makeLinkString(FullClassName="matlab.mixin.Heterogeneous", ClassName="heterogeneous",  BoldFont=false);
            fixedWidthLink    =  makeLinkString(FullClassName="arrow.type.FixedWidthType",  ClassName="FixedWidthType", BoldFont=true);
            timestampLink     = makeLinkString(FullClassName="arrow.type.TimestampType",   ClassName="TimestampType",   BoldFont=false);
            float32Link       = makeLinkString(FullClassName="arrow.type.Float32Type",     ClassName="Float32Type",     BoldFont=false);

            dimensionString = makeDimensionString(size(typeArray));
            
            header = "  " + dimensionString + " " + heterogeneousLink + " " + fixedWidthLink + ...
                " (" +  float32Link + ", " + timestampLink + ") array with properties:" + newline;
            body = "    " + "ID";
            footer = string(newline);
            expectedDisplay = char(strjoin([header body' footer], newline));
            actualDisplay = evalc('disp(typeArray)');
            verify(testCase, actualDisplay, expectedDisplay);
        end

        function NonScalarArraySameTypes(testCase)
            % Verify the display of a scalar, homogeneous arrow.type.Type array:
            % 
            % Example:
            %
            %  1×2 TimestampType array with properties:
            %
            %    ID
            %    TimeUnit
            %    TimeZone 

            import arrow.internal.test.display.verify
            import arrow.internal.test.display.makeLinkString
            import arrow.internal.test.display.makeDimensionString

            timestampType1 = arrow.timestamp(TimeZone="Pacific/Fiji");
            timestampType2 = arrow.timestamp(TimeUnit="Second");
            typeArray = [timestampType1 timestampType2];

            timestampLink = makeLinkString(FullClassName="arrow.type.TimestampType", ClassName="TimestampType", BoldFont=true);
            dimensionString = makeDimensionString(size(typeArray));
            header = "  " + dimensionString + " " + timestampLink + " array with properties:" + newline;
            body = strjust(["ID"; "TimeUnit"; "TimeZone"], "left");
            body = "    " + body;
            footer = string(newline);
            expectedDisplay = char(strjoin([header body' footer], newline));
            actualDisplay = evalc('disp(typeArray)');
            verify(testCase, actualDisplay, expectedDisplay);
        end

        function TestTypeDisplaysOnlyID(testCase, TypeDisplaysOnlyID)
            % Verify the display of arrow.type.Type subclasses that only
            % display the ID property.
            %
            % Example:
            %
            %  BooleanType with properties:
            %
            %          ID: Boolean

            import arrow.internal.test.display.verify
            import arrow.internal.test.display.makeLinkString

            type = TypeDisplaysOnlyID;
            fullClassName = string(class(type));
            className = reverse(extractBefore(reverse(fullClassName), "."));
            typeLink = makeLinkString(FullClassName=fullClassName, ClassName=className, BoldFont=true);
            header = "  " + typeLink + " with properties:" + newline;
            body = "    ID: " + string(type.ID);
            footer = string(newline);
            expectedDisplay = char(strjoin([header body' footer], newline));
            actualDisplay = evalc('disp(type)');
            verify(testCase, actualDisplay, expectedDisplay);
        end

        function TestTimeType(testCase, TimeType)
            % Verify the display of TimeType objects.
            %
            % Example:
            %
            %  Time32Type with properties:
            %
            %          ID: Time32
            %    TimeUnit: Second

            import arrow.internal.test.display.verify
            import arrow.internal.test.display.makeLinkString

            type = TimeType;
            fullClassName = string(class(type));
            className = reverse(extractBefore(reverse(fullClassName), "."));
            typeLink = makeLinkString(FullClassName=fullClassName, ClassName=className, BoldFont=true);

            header = "  " + typeLink + " with properties:" + newline;
            body = strjust(pad(["ID:"; "TimeUnit:"]));
            body = body + " " + [string(type.ID); string(type.TimeUnit)];
            body = "    " + body;
            footer = string(newline);
            expectedDisplay = char(strjoin([header body' footer], newline));
            actualDisplay = evalc('disp(type)');
            verify(testCase, actualDisplay, expectedDisplay);
        end

        function TestDateType(testCase, DateType)
            % Verify the display of DateType objects.
            %
            % Example:
            %
            %  Date32Type with properties:
            %
            %          ID: Date32
            %    DateUnit: Day

            import arrow.internal.test.display.verify
            import arrow.internal.test.display.makeLinkString

            type = DateType;
            fullClassName = string(class(type));
            className = reverse(extractBefore(reverse(fullClassName), "."));
            typeLink = makeLinkString(FullClassName=fullClassName, ClassName=className, BoldFont=true);

            header = "  " + typeLink + " with properties:" + newline;
            body = strjust(pad(["ID:"; "DateUnit:"]));
            body = body + " " + [string(type.ID); string(type.DateUnit)];
            body = "    " + body;
            footer = string(newline);
            expectedDisplay = char(strjoin([header body' footer], newline));
            actualDisplay = evalc('disp(type)');
            verify(testCase, actualDisplay, expectedDisplay);
        end

        function TimestampTypeDisplay(testCase)
            % Verify the display of TimestampType objects.
            %
            % Example:
            %
            %  TimestampType with properties:
            %
            %          ID: Timestamp
            %    TimeUnit: Second
            %    TimeZone: "America/Anchorage"

            import arrow.internal.test.display.verify
            import arrow.internal.test.display.makeLinkString

            type = arrow.timestamp(TimeUnit="Second", TimeZone="America/Anchorage"); %#ok<NASGU>
            classnameLink = makeLinkString(FullClassName="arrow.type.TimestampType", ClassName="TimestampType", BoldFont=true);
            header = "  " + classnameLink + " with properties:" + newline;
            body = strjust(pad(["ID:"; "TimeUnit:"; "TimeZone:"]));
            body = body + " " + ["Timestamp"; "Second"; """America/Anchorage"""];
            body = "    " + body;
            footer = string(newline);
            expectedDisplay = char(strjoin([header body' footer], newline));
            actualDisplay = evalc('disp(type)');
            verify(testCase, actualDisplay, expectedDisplay);
        end

        function StructTypeDisplay(testCase)
            % Verify the display of StructType objects.
            %
            % Example:
            %
            %  StructType with properties:
            %
            %          ID: Struct
            %      Fields: [1x2 arrow.type.Field]

            import arrow.internal.test.display.verify
            import arrow.internal.test.display.makeLinkString
            import arrow.internal.test.display.makeDimensionString

            fieldA = arrow.field("A", arrow.int32());
            fieldB = arrow.field("B", arrow.timestamp(TimeZone="America/Anchorage"));
            type = arrow.struct(fieldA, fieldB); %#ok<NASGU>
            classnameLink = makeLinkString(FullClassName="arrow.type.StructType", ClassName="StructType", BoldFont=true);
            header = "  " + classnameLink + " with properties:" + newline;
            body = strjust(pad(["ID:"; "Fields:"]));
            dimensionString = makeDimensionString([1 2]);
            fieldString = compose("[%s %s]", dimensionString, "arrow.type.Field");
            body = body + " " + ["Struct"; fieldString];
            body = "    " + body;
            footer = string(newline);
            expectedDisplay = char(strjoin([header body' footer], newline));
            actualDisplay = evalc('disp(type)');
            verify(testCase, actualDisplay, expectedDisplay);
        end

        function ListTypeDisplay(testCase)
            % Verify the display of ListType objects.
            %
            % Example:
            %
            %  ListType with properties:
            %
            %          ID: Struct
            %   ValueType: [1x1 arrow.type.StringType]

            import arrow.internal.test.display.verify
            import arrow.internal.test.display.makeLinkString
            import arrow.internal.test.display.makeDimensionString

            valueType = arrow.string();
            type = arrow.list(valueType); %#ok<NASGU>
            classnameLink = makeLinkString(FullClassName="arrow.type.ListType", ClassName="ListType", BoldFont=true);
            header = "  " + classnameLink + " with properties:" + newline;
            body = strjust(pad(["ID:"; "ValueType:"]));
            dimensionString = makeDimensionString([1 1]);
            typeString = compose("[%s %s]", dimensionString, string(class(valueType)));
            body = body + " " + ["List"; typeString];
            body = "    " + body;
            footer = string(newline);
            expectedDisplay = char(strjoin([header body' footer], newline));
            actualDisplay = evalc('disp(type)');
            verify(testCase, actualDisplay, expectedDisplay);
        end
    end
end
