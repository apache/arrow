<!--
PCG Random Number Generation for C++

Copyright 2014-2019 Melissa O'Neill <oneill@pcg-random.org>,
                    and the PCG Project contributors.

SPDX-License-Identifier: (Apache-2.0 OR MIT)

Licensed under the Apache License, Version 2.0 (provided in
LICENSE-APACHE.txt and at http://www.apache.org/licenses/LICENSE-2.0)
or under the MIT license (provided in LICENSE-MIT.txt and at
http://opensource.org/licenses/MIT), at your option. This file may not
be copied, modified, or distributed except according to those terms.

Distributed on an "AS IS" BASIS, WITHOUT WARRANTY OF ANY KIND, either
express or implied.  See your chosen license for details.

For additional information about the PCG random number generation scheme,
visit http://www.pcg-random.org/.
-->

Sources are taken from git changeset ffd522e7188bef30a00c74dc7eb9de5faff90092
(https://github.com/imneme/pcg-cpp).

Changes:
- enclosed in `arrow_vendored` namespace
- remove `struct arbitrary_seed` definition because of https://github.com/apache/arrow/issues/35596

