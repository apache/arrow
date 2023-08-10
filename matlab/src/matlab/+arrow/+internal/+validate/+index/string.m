%NUMERIC Validates the numeric index value.

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
function string(index)

    % This function assumes index is string
    assert(isstring(index));

    if any(ismissing(index))
        errid = "arrow:badSubscript:MissingString";
        msg = "String indices must be nonmissing";
        error(errid, msg);
    elseif any(strlength(index) == 0)
        errid = "arrow:badSubscript:ZeroLengthText";
        msg = "String indices must not be zero length text values.";
        error(errid, msg);
    end
end
