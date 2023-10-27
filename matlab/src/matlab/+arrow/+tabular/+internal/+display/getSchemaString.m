%GETSCHEMASTRING Generates a string representation of an 
% arrow.tabular.Schema object.

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

function text = getSchemaString(schema)
    fields = schema.Fields;
    names = [fields.Name];
    types = [fields.Type];
    typeIDs = string([types.ID]);

    % Use <empty> as the sentinel for field names with zero characters.
    idx = strlength(names) == 0;
    names(idx) = "<empty>";

    if usejava("desktop")
        % When in desktop mode, the Command Window can interpret HTML tags
        % to display bold font and hyperlinks.
        names = compose("<strong>%s</strong>", names);
        classNames = arrayfun(@(type) string(class(type)), types);

        % Creates a string array with the following form:
        %  
        % ["arrow.type.BooleanType" "Boolean" "arrow.type.StringType" "String" ...]
        %
        % This string array is passed to the compose call below. The 
        % format specifier operator supplied to compose contains two 
        % formatting operators (%s), so compose uses two elements from the
        % string array (classNameAndIDs) at a time.
        classNameAndIDs = strings([1 numel(typeIDs) * 2]);
        classNameAndIDs(1:2:end-1) = classNames;
        classNameAndIDs(2:2:end) = typeIDs;
        typeIDs = compose("<a href=""matlab:helpPopup %s"" style=""font-weight:bold"">%s</a>", classNameAndIDs);
    end

    text = names + ": " + typeIDs;
    text = strjoin(text, " | ");
end