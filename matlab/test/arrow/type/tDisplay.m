%TDISPLAY Unit tests verifying the display of all classes within the
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

classdef tDisplay < matlab.unittest.TestCase

    methods (Test)
        function EmptyTypeDisplay(testCase)
            % Verify the display of an empty arrow.type.Type instance.
            %
            % Example:
            %
            %  0x1 Type array with properties:
            %
            %    ID

            type = arrow.type.Type.empty(0, 1);
            typeLink = makeLinkString(FullClassName="arrow.type.Type", ClassName="Type", BoldFont=true);
            dimensionString = makeDimensionString(size(type));
            header = "  " + dimensionString + " " + typeLink + " array with properties:" + newline;
            body = strjust(pad("ID"));
            body = "    " + body;
            footer = string(newline);
            expectedDisplay = char(strjoin([header body' footer], newline));
            actualDisplay = evalc('disp(type)');
            testCase.verifyDisplay(actualDisplay, expectedDisplay);
        end

        function NonScalarArrayDifferentTypes(testCase)
            % Verify the display of a nonscalar, heterogeneous arrow.type.Type array:
            % 
            % Example:
            %
            %  1×2 heterogeneous FixedWidthType (Float32Type, TimestampType) array with properties:
            %
            %    ID

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
            testCase.verifyDisplay(actualDisplay, expectedDisplay);
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
            testCase.verifyDisplay(actualDisplay, expectedDisplay);
        end


        function BooleanType(testCase)
            % Verify the display of BooleanType objects.
            %
            % Example:
            %
            %  BooleanType with properties:
            %
            %          ID: Boolean

            type = arrow.boolean(); %#ok<NASGU>
            booleanLink = makeLinkString(FullClassName="arrow.type.BooleanType", ClassName="BooleanType", BoldFont=true);
            header = "  " + booleanLink + " with properties:" + newline;
            body = "    ID: Boolean";
            footer = string(newline);
            expectedDisplay = char(strjoin([header body' footer], newline));
            actualDisplay = evalc('disp(type)');
            testCase.verifyDisplay(actualDisplay, expectedDisplay);
        end
    end

    methods
        function verifyDisplay(testCase, actualDisplay, expectedDisplay)
            % When the MATLAB GUI is running, '×' (char(215)) is used as
            % the delimiter between dimension values. However, when the 
            % GUI is not running, 'x' (char(120)) is used as the delimiter.
            % To account for this discrepancy, check if actualDisplay 
            % contains char(215). If not, replace all instances of
            % char(215) in expectedDisplay with char(120).

            tf = contains(actualDisplay, char(215));
            if ~tf
                idx = strfind(expectedDisplay, char(215));
                expectedDisplay(idx) = char(120);
            end
            testCase.verifyEqual(actualDisplay, expectedDisplay);
        end
    end
end

function link = makeLinkString(opts)
    arguments
        opts.FullClassName(1, 1) string
        opts.ClassName(1, 1) string
        % When displaying heterogeneous arrays, only the name of the 
        % closest shared anscestor class is displayed in bold. All other
        % class names are not bolded.
        opts.BoldFont(1, 1) logical
    end

    if opts.BoldFont
        link = compose("<a href=""matlab:helpPopup %s"" style=""font-weight:bold"">%s</a>", ...
            opts.FullClassName, opts.ClassName);
    else
        link = compose("<a href=""matlab:helpPopup %s"">%s</a>", opts.FullClassName, opts.ClassName);
    end
end

function sizeString = makeDimensionString(arraySize)
    sizeString = string(arraySize);
    sizeString = join(sizeString, char(215));
end