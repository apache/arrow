%MAKELINKSTRING Utility function for creating hyperlinks.

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

function link = makeLinkString(opts)
    arguments
        opts.FullClassName(1, 1) string
        opts.ClassName(1, 1) string
        % When displaying heterogeneous arrays, only the name of the 
        % closest shared ancestor class is displayed in bold. All other
        % class names are not bolded.
        opts.BoldFont(1, 1) logical
    end
        
    if opts.BoldFont
        link = compose("<a href=""matlab:helpPopup %s""" + ...
            " style=""font-weight:bold"">%s</a>", ...
            opts.FullClassName, opts.ClassName);
    else
        link = compose("<a href=""matlab:helpPopup %s"">%s</a>", ...
            opts.FullClassName, opts.ClassName);
    end
end