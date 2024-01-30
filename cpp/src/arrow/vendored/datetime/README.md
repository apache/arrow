<!--
The MIT License (MIT)

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.
-->

# Utilities for supporting date time functions

Sources for datetime are adapted from Howard Hinnant's date library
(https://github.com/HowardHinnant/date).

Sources are taken from changeset cc4685a21e4a4fdae707ad1233c61bbaff241f93
of the above project.

The following changes are made:
- fix internal inclusion paths (from "date/xxx.h" to simply "xxx.h")
- enclose the `date` namespace inside the `arrow_vendored` namespace
- include a custom "visibility.h" header from "tz.cpp" for proper DLL
  exports on Windows
- disable curl-based database downloading in "tz.h"
