%DISPLAY Generates display text for tabular-like objects.

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

function text = display(names, data)
%DISPLAY Summary of this function goes here
%   Detailed explanation goes here
    arguments
        names(1, :) string {mustBeNonempty}
        data(:, :) string {mustBeNonempty}
    end

    % names and data must have the same number of columns
    assert(size(names, 2) == size(data, 2));

    % Determine the maximum text length per column 
    namesLength = strlength(names);
    dataLength = strlength(data);
    columnWidths = max([namesLength; dataLength]);

    % Create the "_" dividers for each column
    dividers = arrayfun(@(width) string(repmat('_', [1 width])), columnWidths);

    extraNumPadding = 0;
    if usejava("desktop")
        names = compose("<strong>%s</strong>", names);
        % To account for the extra characters required to bold the names,
        % we must add 17 to the minimum column width when padding the
        % names.
        extraNumPadding = 17;
    end

    % Pad and center align the strings
    numColumns = numel(names);
    for ii = 1:numColumns
        names(ii) = strjust(pad(names(ii), columnWidths(ii) + extraNumPadding), "center");
        data(:, ii) = strjust(pad(data(:, ii), columnWidths(ii)), "center");
    end

    indent = "    ";
    names = indent + join(names, indent);
    dividers = indent + join(dividers, indent);
    data = indent + join(data, indent);
    text = names + newline + dividers + newline + newline + data;
end
