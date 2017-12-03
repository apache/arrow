import decimal
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


def randdecimal(precision, scale):
    abs_scale = abs(scale)
    max_whole_value = 10 ** (precision - abs_scale) - 1
    whole = random.randint(-max_whole_value, max_whole_value)

    if not scale:
        return decimal.Decimal(whole)

    max_fractional_value = 10 ** abs_scale - 1
    fractional = random.randint(0, max_fractional_value)

    return decimal.Decimal(
        '{}.{}'.format(whole, str(fractional).rjust(abs_scale, '0'))
    )
