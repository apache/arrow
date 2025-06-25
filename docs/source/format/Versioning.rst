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

*******************************
Format Versioning and Stability
*******************************

Starting with version 1.0.0, Apache Arrow uses
**two versions** to describe each release of the project:
the **Format Version** and the **Library Version**. Each Library
Version has a corresponding Format Version, and multiple versions of
the library may have the same format version. For example, library
versions 2.0.0 and 3.0.0 may both track format version 1.0.0. See
:doc:`../status` for details about supported features in each library.

For library versions prior to 1.0.0, major releases may contain API
changes. From 1.0.0 onward, we follow `Semantic Versioning
<https://semver.org/>`_ with regards to communicating API changes. We
expect most releases to be major library releases.

Backward Compatibility
======================

A newer versioned client library will be able to read any data and
metadata produced by an older client library.

So long as the **major** format version is not changed, a newer
library is backward compatible with an older library.

Forward Compatibility
=====================

An older client library must be able to either read data generated
from a new client library or detect that it cannot properly read the
data.

An increase in the **minor** version of the format version, such as
1.0.0 to 1.1.0, indicates that 1.1.0 contains new features not
available in 1.0.0. So long as these features are not used (such as a
new data type), forward compatibility is preserved.

Long-Term Stability
===================

A change in the format major version (e.g. from 1.0.0 to 2.0.0)
indicates a disruption to these compatibility guarantees in some way.
We **do not expect** this to be a frequent occurrence.
This would be an exceptional
event and, should this come to pass, we would exercise caution in
ensuring that production applications are not harmed.

Pre-1.0.0 Versions
==================

We made no forward or backward compatibility guarantees for
versions prior to 1.0.0. However, we made every effort to ensure
that new clients can read serialized data produced by library version
0.8.0 and onward.

.. _post-1-0-0-format-versions:

Post-1.0.0 Format Versions
==========================

Since version 1.0.0, there have been five new minor versions and zero new
major versions of the Arrow format. Each new minor version added new features.
When these new features are not used, the new minor format versions are
compatible with format version 1.0.0. The new features added in each minor
format version since 1.0.0 are as follows:

Version 1.1
-----------

* Added 256-bit Decimal type.

Version 1.2
-----------

* Added MonthDayNano interval type.

Version 1.3
-----------

* Added :ref:`run-end-encoded-layout`.

Version 1.4
-----------

* Added :ref:`variable-size-binary-view-layout` and the associated BinaryView
  and Utf8View types.
* Added :ref:`listview-layout` and the associated ListView and LargeListView
  types.
* Added :ref:`variadic-buffers`.

Version 1.5
-----------

* Expanded Decimal type bit widths to allow 32-bit and 64-bit types.
