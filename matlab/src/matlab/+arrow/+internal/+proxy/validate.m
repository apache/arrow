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

function validate(proxy, expectedName)
%VALIDATE Throws an arrow:matlab:ProxyNameMismatch error if
% proxy.Name and expectedName are not equal.
    arguments
        proxy(1, 1) libmexclass.proxy.Proxy
        expectedName(1, 1) string {mustBeNonmissing, mustBeNonzeroLengthText}
    end

    if proxy.Name ~= expectedName
        errid = "arrow:proxy:ProxyNameMismatch";
        msg = compose("The Name property of the Proxy provided is ""%s"", " + ...
            "but expected it to be ""%s"".", proxy.Name, expectedName);
        error(errid, msg);
    end
end
