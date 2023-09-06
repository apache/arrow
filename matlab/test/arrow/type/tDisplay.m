%TDISPLAY Unit tests verifying the display of arrow.type.Type arrays.

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

            type = arrow.type.Type.empty(0, 1); %#ok<NASGU>
            classnameLink = "<a href=""matlab:helpPopup arrow.type.Type"" style=""font-weight:bold"">Type</a>";
            sizeString = "0" + char(215) + "1";
            header = "  " + sizeString + " " + classnameLink + " array with properties:" + newline;
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

            heterogeneousLink =  makeDisplayLink(FullClassName="matlab.mixin.Heterogeneous", ClassName="heterogeneous",  BoldFont=false);
            fixedWidthLink    =  makeDisplayLink(FullClassName="arrow.type.FixedWidthType",  ClassName="FixedWidthType", BoldFont=true);
            timestampLink     = makeDisplayLink(FullClassName="arrow.type.TimestampType",   ClassName="TimestampType",   BoldFont=false);
            float32Link       = makeDisplayLink(FullClassName="arrow.type.Float32Type",     ClassName="Float32Type",     BoldFont=false);

            sizeString = makeSizeString(size(typeArray));
            
            header = "  " + sizeString + " " + heterogeneousLink + " " + fixedWidthLink + ...
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

            timestampLink = makeDisplayLink(FullClassName="arrow.type.TimestampType", ClassName="TimestampType", BoldFont=true);
            sizeString = makeSizeString(size(typeArray));
            header = "  " + sizeString + " " + timestampLink + " array with properties:" + newline;
            body = strjust(["ID"; "TimeUnit"; "TimeZone"], "left");
            body = "    " + body;
            footer = string(newline);
            expectedDisplay = char(strjoin([header body' footer], newline));
            actualDisplay = evalc('disp(typeArray)');
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

function link = makeDisplayLink(opts)
    arguments
        opts.FullClassName(1, 1) string
        opts.ClassName(1, 1) string
        opts.BoldFont(1, 1) logical
    end

    if opts.BoldFont
        link = compose("<a href=""matlab:helpPopup %s"" style=""font-weight:bold"">%s</a>", ...
            opts.FullClassName, opts.ClassName);
    else
        link = compose("<a href=""matlab:helpPopup %s"">%s</a>", opts.FullClassName, opts.ClassName);
    end
end

function sizeString = makeSizeString(arraySize)
    sizeString = string(arraySize);
    sizeString = join(sizeString, char(215));
end