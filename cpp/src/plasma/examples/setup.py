import numpy as np
from distutils.core import setup
from Cython.Build import cythonize

setup(
    name="distsort",
    extra_compile_args=["-O3", "-mtune=native", "-march=native"],
    ext_modules=cythonize("multimerge.pyx"),
    include_dirs=[np.get_include()],
)
