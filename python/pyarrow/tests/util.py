import random
import contextlib


def randsign():
    """Randomly choose from 1 and -1.

    Returns
    -------
    sign : int
    """
    return random.choice((-1, 1))


@contextlib.contextmanager
def random_seed(seed):
    original_state = random.getstate()
    random.seed(seed)
    try:
        yield
    finally:
        random.setstate(original_state)
