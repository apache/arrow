%GETTABULARHEADER Generates the display header for arrow.tabular.Table and
% arrow.tabular.RecordBatch.

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

function header = getTabularHeader(className, numRows, numColumns)
    import arrow.internal.display.boldFontIfPossible
    import arrow.internal.display.pluralizeStringIfNeeded

    numRowsString = boldFontIfPossible(numRows);
    numColsString = boldFontIfPossible(numColumns);
    rowWordString = pluralizeStringIfNeeded(numRows, "row");
    colWordString = pluralizeStringIfNeeded(numColumns, "column");
    formatSpec = "  Arrow %s with %s %s and %s %s";
    if numColumns > 0
        formatSpec = formatSpec + ":";
    end
    header = compose(formatSpec,className, numRowsString, rowWordString, numColsString, colWordString);
end