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

function text = displayHeader(schema)
    fields = schema.Fields;
    names = [fields.Name];
    types = [fields.Type];
    typeIDs = string([types.ID]);

    nameLength = strlength(names);
    idx = nameLength > 15;
    numCutOff = nameLength(idx) - 15;
    names(idx) = extractBefore(names(idx), 16) + compose("... (+%d chars)", numCutOff);

    boldFont = usejava("desktop");

    if boldFont
       names = compose(" <strong>Name:</strong> %s ", names);
       typeIDs = compose(" <strong>Type:</strong> %s ", typeIDs);
    else
        names = compose(" Name: %s ", names);
        typeIDs = compose(" Type: %s ", typeIDs);
    end

    numColumns = numel(typeIDs);
    columnWidth = max([strlength(names); strlength(typeIDs)]);

    body = [names; typeIDs];
    for ii = 1:numColumns
        body(:, ii) = pad(body(:, ii), columnWidth(ii));
    end
        
    body = "│" + join(body, "│", 2) + "│";
    body = join(body, newline);
    
    if boldFont
       columnWidth = columnWidth - 17;
    end
    
    top = getBorderRow(ColumnWidth=columnWidth, LeftCorner="┌", ...
        RightCorner="┐", Divider="┬");
    bottom = getBorderRow(ColumnWidth=columnWidth, LeftCorner="└", ...
        RightCorner="┘", Divider="┴");    
    text = join([top; body; bottom], newline);

end

function borderRow = getBorderRow(opts)
    arguments
        opts.ColumnWidth
        opts.LeftCorner
        opts.RightCorner
        opts.Divider
    end

    numDividers = numel(opts.ColumnWidth) - 1;
    numHorizontalSections = numel(opts.ColumnWidth);
    borderRow = strings([1 numDividers + numHorizontalSections]);
    
    index = 1;
    for ii = 1:numHorizontalSections
        borderRow(index) = repmat('─', [1 opts.ColumnWidth(ii)]);
        index = index + 2;
    end
    borderRow(2:2:end-1) = opts.Divider;
    borderRow = opts.LeftCorner + strjoin(borderRow, "") + opts.RightCorner;
end