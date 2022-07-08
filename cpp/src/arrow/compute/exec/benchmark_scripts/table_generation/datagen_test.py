from datagen import *
import numpy as np
import pytest

class ArrayUniformDriftGenerator(ArrayUniformDriftDistribution):
    def __init__(self, rng, arr, drift_opts):
        super().__init__(arr, drift_opts)
        self.rng = rng

    def __call__(self, block_size, num_blocks):
        return super().__call__(self.rng, block_size, num_blocks)

def make_dist(arr, seed, **kwargs):
    drift_opts = DriftOptions()
    for k, v in kwargs.items():
        setattr(drift_opts, k, v)
    rng = np.random.default_rng(seed)
    return ArrayUniformDriftGenerator(rng, arr, drift_opts)

@pytest.mark.parametrize(
    "seed,block_size,num_blocks,data,expected",
    [
        (1, 7, 3, ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"], [
            'd', 'e', 'g', 'h', 'a', 'b', 'g',
            'd', 'd', 'b', 'a', 'f', 'a', 'b',
            'c', 'h', 'd', 'c', 'a', 'd', 'f',
        ]),
        (2, 6, 4, ["a", "b", "c", "d", "e", "f", "g"], [
            'c', 'c', 'd', 'e', 'a', 'a',
            'e', 'a', 'c', 'a', 'c', 'e',
            'e', 'c', 'd', 'a', 'c', 'e',
            'g', 'g', 'g', 'a', 'e', 'g',
        ]),
        (3, 8, 2, ["a", "b", "c", "d", "e"], [
            'b', 'c', 'd', 'd', 'a', 'a', 'd', 'd',
            'd', 'd', 'b', 'c', 'd', 'b', 'b', 'b',
        ]),
    ]
)
def test_uniform_dist_expected_output(seed, block_size, num_blocks, data, expected):
    arr = np.array(data)
    result = make_dist(arr, 1)(block_size, num_blocks)
    assert (expected == result).all()

@pytest.mark.parametrize(
    "seed,block_size,num_blocks,data",
    [
        (1, 7, 3, ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"]),
        (2, 6, 4, ["a", "b", "c", "d", "e", "f", "g"]),
        (3, 8, 2, ["a", "b", "c", "d", "e"]),
    ]
)
def test_uniform_dist_one_block_at_a_time(seed, block_size, num_blocks, data):
    arr = np.array(data)
    res1 = make_dist(arr, 1)(block_size, num_blocks)
    dist = make_dist(arr, 1)
    res2 = np.concatenate([dist(block_size, 1) for i in range(num_blocks)])
    assert (res1 == res2).all()


@pytest.mark.parametrize(
    "seed,block_size,num_blocks,data",
    [
        (1, 7, 3, ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"]),
        (2, 6, 4, ["a", "b", "c", "d", "e", "f", "g"]),
        (3, 8, 2, ["a", "b", "c", "d", "e"]),
    ]
)
def test_uniform_dist_drift_blocks(seed, block_size, num_blocks, data):
    arr = np.array(data)
    dist1 = make_dist(arr, 1)
    res1 = dist1(block_size, num_blocks)
    flag1 = dist1._flag
    drift_blocks = max(1, num_blocks - 1)
    dist2 = make_dist(arr, 1, drift_blocks=drift_blocks, prob_drift=1.0)
    res2 = dist2(block_size, num_blocks)
    flag2 = dist2._flag
    assert (res1[:block_size] == res2[:block_size]).all()
    if drift_blocks == 1:
        assert (res1[block_size:] == res2[block_size:]).all()
    else:
        assert (res1[block_size:] != res2[block_size:]).any()
    assert ((flag1 & flag2) == flag1).all()
