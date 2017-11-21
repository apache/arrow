import random
import contextlib


def randsign():
    return random.choice((-1, 1))


@contextlib.contextmanager
def random_seed(seed):
    original_state = random.getstate()
    random.seed(seed)
    try:
        yield
    finally:
        random.setstate(original_state)
