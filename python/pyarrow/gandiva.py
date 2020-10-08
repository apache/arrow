try:
    from pyarrow_gandiva import *
except ImportError:
    raise ImportError('Gandiva extension not installed. You can install it '
                      'with pip install pyarrow_gandiva=={version} or conda install pyarrow_gandiva=={version}.')