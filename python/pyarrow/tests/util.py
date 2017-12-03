"""
Utility functions for testing
"""

import decimal
import random
import contextlib


def randsign():
    """Randomly choose either 1 or -1.

    Returns
    -------
    sign : int
    """
    return random.choice((-1, 1))


@contextlib.contextmanager
def random_seed(seed):
    """Set the random seed inside of a context manager.

    Parameters
    ----------
    seed : int
        The seed to set

    Notes
    -----
    This function is useful when you want to set a random seed but not affect
    the random state of other functions using the random module.
    """
    original_state = random.getstate()
    random.seed(seed)
    try:
        yield
    finally:
        random.setstate(original_state)


def randdecimal(precision, scale):
    """Generate a random decimal value with specified precision and scale.

    Parameters
    ----------
    precision : int
        The maximum number of digits to generate. Must be an integer between 1
        and 38 inclusive.
    scale : int
        The maximum number of digits following the decimal point.  Must be an
        integer greater than or equal to 0.

    Returns
    -------
    decimal_value : decimal.Decimal
        A random decimal.Decimal object with the specifed precision and scale.
    """
    assert 1 <= precision <= 38, 'precision must be between 1 and 38 inclusive'
    if scale < 0:
        raise ValueError(
            'randdecimal does not yet support generating decimals with '
            'negative scale'
        )
    max_whole_value = 10 ** (precision - scale) - 1
    whole = random.randint(-max_whole_value, max_whole_value)

    if not scale:
        return decimal.Decimal(whole)

    max_fractional_value = 10 ** scale - 1
    fractional = random.randint(0, max_fractional_value)

    return decimal.Decimal(
        '{}.{}'.format(whole, str(fractional).rjust(scale, '0'))
    )
