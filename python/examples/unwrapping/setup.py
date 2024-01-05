from setuptools import setup
from Cython.Build import cythonize

import os
import numpy as np
import pyarrow as pa


ext_modules = cythonize("example.pyx")

for ext in ext_modules:
    # The Numpy C headers are currently required
    ext.include_dirs.append(np.get_include())
    ext.include_dirs.append(pa.get_include())
    ext.libraries.extend(pa.get_libraries())
    ext.library_dirs.extend(pa.get_library_dirs())

    if os.name == 'posix':
        ext.extra_compile_args.append('-std=c++17')

setup(ext_modules=ext_modules)
