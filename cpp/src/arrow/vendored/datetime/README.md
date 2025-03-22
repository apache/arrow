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

Sources are taken from changeset 5bdb7e6f31fac909c090a46dbd9fea27b6e609a4
of the above project.

The following changes are made:
- fix internal inclusion paths (from "date/xxx.h" to simply "xxx.h")
- enclose the `date` namespace inside the `arrow_vendored` namespace
- fix 4 declarations like `CONSTCD11 date::day  operator "" _d(unsigned long long d) NOEXCEPT;`
  to not have offending whitespace for modern clang:
  `CONSTCD11 date::day  operator ""_d(unsigned long long d) NOEXCEPT;` 

## How to update

```console
$ cd cpp/src/arrow/vendored/datetime
$ ./update.sh 3.0.3
```
