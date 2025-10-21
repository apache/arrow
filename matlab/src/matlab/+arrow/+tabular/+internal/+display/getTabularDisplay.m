%GETTABULARDISPLAY Generates the display for arrow.tabular.Table and
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

function tabularDisplay = getTabularDisplay(tabularObj, className)
    import arrow.tabular.internal.display.getSchemaString
    import arrow.tabular.internal.display.getTabularHeader

    numRows = tabularObj.NumRows;
    numColumns = tabularObj.NumColumns;
    tabularDisplay = getTabularHeader(className, numRows, numColumns);

    if numColumns > 0
        twoNewLines = string([newline newline]);
        fourSpaces = string(repmat(' ', 1, 4));
        eightSpaces = string(repmat(' ', 1, 8));

        schemaHeader = fourSpaces + "Schema:";
        schemaBody =   eightSpaces + getSchemaString(tabularObj.Schema);
        schemaDisplay = schemaHeader + twoNewLines + schemaBody;
        tabularDisplay = tabularDisplay + twoNewLines + schemaDisplay;
    
        if numRows > 0
            rowHeader = fourSpaces + "First Row:";
            rowBody = eightSpaces + tabularObj.Proxy.getRowAsString(struct(Index=int64(1)));
            rowDisplay = rowHeader + twoNewLines + rowBody;
            tabularDisplay = tabularDisplay + twoNewLines + rowDisplay;
        end
    end
end
