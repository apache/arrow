function [variables, metadata] = table2mlarrow(t)
%TABLE2MLARROW Converts a MATLAB table into a form
%   suitable for passing to the mlarrow C++ MEX layer.
%
%   [VARIABLES, METADATA] = TABLE2MLARROW(T)
%   Takes a MATLAB table T and returns struct array equivalents
%   which are suitable for passing to the mlarrow C++ MEX layer.
%
%   VARIABLES is an 1xN struct array representing the the table variables.
%
%   VARIABLES contains the following fields:
%
%   Field Name     Class        Description
%   ------------   -------      ----------------------------------------------
%   Name           char         Variable's name
%   Type           char         Variable's MATLAB datatype
%   Data           numeric      Variable's data
%   Valid          logical      0 = invalid (null), 1 = valid (non-null) value
%
%   METADATA is a 1x1 struct array with the following fields:
%
%   METADATA contains the following fields:
%
%   Field Name    Class         Description
%   ------------  -------       ----------------------------------------------
%   Description   char          Table description (T.Properties.Description)
%   NumRows       double        Number of table rows (height(T))
%   NumVariables  double        Number of table variables (width(T))
%
%   See also FEATHERREAD, FEATHERWRITE.

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

import mlarrow.util.*;

% Struct array representing the underlying data of each variable
% in the given table.
variables = repmat(createVariableStruct('', [], [], ''), 1, width(t));

% Struct representing table-level metadata.
metadata = createMetadataStruct(t.Properties.Description, height(t), width(t));

% Iterate over each variable in the given table,
% extracting the underlying array data.
for ii = 1:width(t)
    data = t.(ii);
    % Multi-column table variables are unsupported.
    if ~isvector(data)
        error('MATLAB:arrow:MultiColumnVariablesUnsupported', ...
              'Multi-column table variables are unsupported by featherwrite.');
    end
    % Get the datatype of the current variable's underlying array.
    variables(ii).Type = class(data);
    % Break the datatype down into its constituent components, if appropriate.
    switch variables(ii).Type
        % For numeric variables, the underlying array data can
        % be passed to the C++ layer directly.
        case {'uint8', 'uint16', 'uint32', 'uint64', ...
              'int8', 'int16', 'int32', 'int64', ...
              'single', 'double'}
            variables(ii).Data = data;
        otherwise
            error('MATLAB:arrow:UnsupportedVariableType', ...
                 ['Type ' variables(ii).Type ' is unsupported by featherwrite.']);
    end
    variables(ii).Valid = ~ismissing(data);
    variables(ii).Name = t.Properties.VariableNames{ii};
end

end
