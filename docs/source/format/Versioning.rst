.. Licensed to the Apache Software Foundation (ASF) under one
.. or more contributor license agreements.  See the NOTICE file
.. distributed with this work for additional information
.. regarding copyright ownership.  The ASF licenses this file
.. to you under the Apache License, Version 2.0 (the
.. "License"); you may not use this file except in compliance
.. with the License.  You may obtain a copy of the License at

..   http://www.apache.org/licenses/LICENSE-2.0

.. Unless required by applicable law or agreed to in writing,
.. software distributed under the License is distributed on an
.. "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
.. KIND, either express or implied.  See the License for the
.. specific language governing permissions and limitations
.. under the License.

Format Versioning and Stability
===============================

Starting with version 1.0.0 (not yet released), Apache Arrow utilizes
**two versions** to describe each release of the project. These are
the **Format Version** and the **Library Version**. Each Library
Version has a corresponding Format Version, and multiple versions of
the library may have the same format version. For example, library
versions 2.0.0 and 3.0.0 may both track format version 1.0.0.

For library versions prior to 1.0.0, major releases may contain API
changes. From 1.0.0 onward, we follow `Semantic Versioning
<https://semver.org/>`_ with regards to communicating API changes. We
expect most releases to be major library releases.

Backward Compatibility
----------------------

A newer versioned client library will be able to read any data and
metadata produced by an older client library.

So long as the **major** format version is not changed, a newer
library is backward compatible with an older library.

Forward Compatibility
---------------------

An older client library must be able to either read data generated
from a new client library or detect that it cannot properly read the
data.

An increase in the **minor** version of the format version, such as
1.0.0 to 1.1.0, indicates that 1.1.0 contains new features not
available in 1.0.0. So long as these features are not used (such as a
new logical data type), forward compatibility is preserved.

Long-Term Stability
-------------------

A change in the format major version (e.g. from 1.0.0 to 2.0.0)
indicates a disruption to these compatibility guarantees in some way.
We **do not expect** this to be a frequent occurrence starting with
the 1.0.0 library and format release. This would be an exceptional
event and, should this come to pass, we would exercise caution in
ensuring that production applications are not harmed.

Pre-1.0.0 Versions
------------------

We have made no forward or backward compatibility guarantees for
versions prior to 1.0.0. However, we are making every effort to ensure
that new clients can read serialized data produced by library version
0.8.0 and onward.
