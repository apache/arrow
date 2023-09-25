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
    
    maxNameWidth = max(strlength(names));
    maxTypeWidth = max(strlength(typeIDs));
    maxWidth = max([maxNameWidth maxTypeWidth]);
        
    body = pad([names ; typeIDs], maxWidth);
    body = "│" + join(body, "│", 2) + "│";
    body = join(body, newline);
    
    if boldFont
       maxWidth = maxWidth - 17;
    end
    
    top =  getTopRow(numel(typeIDs), maxWidth);
    bottom = getBottomRow(numel(typeIDs), maxWidth);
    
    text = join([top; body; bottom], newline);

end

function topRow = getTopRow(numFields, maxWidth)
    horizontalBars = string(repmat('─', [1 maxWidth]));
    topRow = strings([1 3 + numFields - 1]);
    topRow(1) = "┌";
    topRow(2) = horizontalBars;
    topRow(3:end-1) = "┬" + horizontalBars;
    topRow(end) = "┐";
    topRow = strjoin(topRow, "");
end

function bottomRow = getBottomRow(numFields, maxWidth)
    horizontalBars = string(repmat('─', [1 maxWidth]));
    bottomRow = strings([1 3 + numFields - 1]);
    bottomRow(1) = "└";
    bottomRow(2) = horizontalBars;
    bottomRow(3:end-1) = "┴" + horizontalBars;
    bottomRow(end) = "┘";
    bottomRow = strjoin(bottomRow, "");
end