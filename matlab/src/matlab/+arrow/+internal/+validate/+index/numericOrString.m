%NUMERICORSTRING Validates index is a valid numeric or string index.

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

function idx = numericOrString(idx, numericIndexType, opts)
    arguments
        idx
        numericIndexType(1, 1) string
        opts.AllowNonScalar(1, 1) logical = true
    end

    import arrow.internal.validate.*

    opts = namedargs2cell(opts);
    idx = convertCharsToStrings(idx);
    if isnumeric(idx)
        idx = index.numeric(idx, numericIndexType, opts{:});
    elseif isstring(idx)
        idx = index.string(idx, opts{:});
    else
        errid = "arrow:badsubscript:UnsupportedIndexType";
        msg = "Indices must be positive integers or nonmissing strings.";
        error(errid, msg);
    end
end
