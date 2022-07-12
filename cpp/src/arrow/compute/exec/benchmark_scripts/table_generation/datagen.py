import copy
from datetime import date, datetime, timedelta
from pandas.tseries.frequencies import to_offset
import pyarrow as pa
import numpy as np

_DEFAULT_BATCH_SIZE = 100

_DTYPE_IND = np.int32
_DTYPE_ID = np.int32
# strings in detail-arrays should be no longer than 300
_DTYPE_STRING = np.dtype("<U300")

_DEFAULT_NUM_IDS = 5000
_DEFAULT_NUM_FEATURE_CATEGORIES = 100

# the arrays below provide mock details
# an array shorter than the number of ids is used in round-robin fashion
_DEFAULT_DETAIL_NAME = np.array(
    ["Alice", "Bob"], dtype=_DTYPE_STRING)
_DEFAULT_DETAIL_IDS = np.array([10203040, 50607080], dtype=_DTYPE_ID)

def _arrow_type(dtype):
    if dtype == np.int8:
        return pa.int8()
    elif dtype == np.int16:
        return pa.int16()
    elif dtype == np.int32:
        return pa.int32()
    elif dtype == np.int64:
        return pa.int64()
    elif dtype == np.uint8:
        return pa.uint8()
    elif dtype == np.uint16:
        return pa.uint16()
    elif dtype == np.uint32:
        return pa.uint32()
    elif dtype == np.uint64:
        return pa.uint64()
    elif dtype == np.float16:
        return pa.float16()
    elif dtype == np.float32:
        return pa.float32()
    elif dtype == np.float64:
        return pa.float64()
    elif dtype == _DTYPE_STRING:
        return pa.string()
    else:
        raise RuntimeError("unknown dtype %r" % dtype)

def _doc(*args, **kwargs):
    def _doc_wrap(f):
        f.__doc__ = f.__doc__.format(*args, **kwargs)
        return f
    return _doc_wrap

def _max_freq(freqs):
    offsets = [to_offset(freq) for freq in freqs]
    return freqs[offsets.index(max(offsets))]

class SimpleMidnightCalendar:
    """
    A simple midnight calendar. All days are business days, and business hours
    are midnight only, irrespective of holidays or time zone.
    """

    def is_business_day(self, curr_date):
        """
        Returns True
        """
        return True

    def business_begin_ns(self, curr_date):
        """
        Returns midnight
        """
        return 0

    def business_end_ns(self, curr_date):
        """
        Returns midnight
        """
        return 0

class SimpleBusinessCalendar:
    """
    A simple business calendar. Business days are Monday through Friday, and business
    hours are 9:30 to 14:00, irrespective of holidays or time zone.
    """

    def is_business_day(self, curr_date):
        """
        Returns True for a date on Monday through Friday
        """
        return curr_date.weekday() < 5

    def business_begin_ns(self, curr_date):
        """
        Returns nanoseconds-since-midnight at 9:30
        """
        return 9 * 3600 * 1000000000

    def trading_end_ns(self, curr_date):
        """
        Returns nanoseconds-since-midnight at 16:00
        """
        return 16 * 3600 * 1000000000

class SimpleDailyBusinessCalendar:
    """
    A simple daily business calendar. Business days are Monday through Friday, and
    business hours are midnight only, irrespective of holidays or time zone.
    """

    def is_business_day(self, curr_date):
        """
        Returns True for a date on Monday through Friday
        """
        return curr_date.weekday() < 5

    def business_begin_ns(self, curr_date):
        """
        Returns midnight
        """
        return 0

    def trading_end_ns(self, curr_date):
        """
        Returns midnight
        """
        return 0

class ArrayDistribution:
    def __init__(self, dtype):
        self.dtype = dtype

    def __call__(self, rng, block_size, num_blocks):
        raise NotImplementedError

class ArrayAllDistribution(ArrayDistribution):
    """
    Samples in order all elements of a given array.
    """
    def __init__(self, arr):
        super().__init__(arr.dtype)
        self._arr = arr

    def __call__(self, rng, block_size, num_blocks):
        ind = np.arange(block_size, dtype=_DTYPE_ID)
        return self._arr[np.tile(ind % len(self._arr), num_blocks)]

class ArrayUniformDistribution(ArrayDistribution):
    """
    Samples uniformly the elements of a given array.
    """
    def __init__(self, arr):
        super().__init__(arr.dtype)
        self._arr = arr

    def __call__(self, rng, block_size, num_blocks):
        ind = rng.integers(low=0, high=len(self._arr),
                size=block_size*num_blocks, dtype=_DTYPE_IND)
        return self._arr[ind]

class DriftOptions:
    drift_blocks: int = 1 # number of blocks between drift chances
    prob_drift: float = 1 # probability of each drift chance
    prob_add: float = 0.1 # probability of adding an item on drift cycle
    prob_del: float = 0.1 # probability of deleting an item on drift cycle
    frac_init: float = 0.8 # initial fraction of items inside
    frac_min: float = 0.05 # minimum fraction of items inside
    frac_max: float = 1.0 # maximum fraction of items inside

class ArrayUniformDriftDistribution(ArrayDistribution):
    """
    Samples uniformly from a drifting set of elements of a given array.

    The sampler works in units of blocks, defined by the block_size parameter
    of the call operator, and is controlled by DriftOptions. For a fixed seed,
    it generates a fixed sequence of blocks, regardless of how many blocks are
    generated in each invocation of the call operator.

    The sampler maintains a subset of elements of the array from which to sample
    a block. This subset is initially a fraction DriftOptions.frac_init of the
    elements of the array. After each block, there is a probability
    DriftOptions.prob_drift that a next subset of elements is created by adding
    elements with probability DriftOptions.prob_add and deleting elements with
    probability DriftOptions.prob_del subject to this subset being a fraction
    between DriftOptions.frac_min and DriftOptions.frac_max of the elements of
    the array. The set of elements used for sampling is updated only each
    DriftOptions.drift_blocks blocks to the union of these next sets of elements
    created over these blocks. Thus, two samplers that differ only in their
    DriftOptions.drift_blocks setting will sample from synchronized sets of
    elements. For example, one sampler generates a block every hour using a set
    of elements associated with one hour while another generates every 24 hours
    using the union of the sets of elements associated with each of 24 hours.
    """
    def __init__(self, arr, drift_opts = None):
        super().__init__(arr.dtype)
        self._arr = arr
        self._drift_opts = copy.deepcopy(drift_opts) or DriftOptions()
        # countdown in blocks until next drift chance
        self._driftb = self._drift_opts.drift_blocks
        # constant range of indices
        self._ind = np.arange(len(arr), dtype=_DTYPE_IND)
        # permutation of indices into self._arr
        self._ptr = np.arange(len(arr), dtype=_DTYPE_IND)
        # temporary buffer
        self._buf = np.empty_like(arr)
        # selects inside-indices of self._ind
        self._flag = np.empty_like(arr, dtype=bool)
        # selects inside-indices of self._ind
        self._draw = np.empty_like(arr, dtype=bool)
        # probability of item to be added or deleted
        self._prob = np.empty_like(arr, dtype=np.float64)
        # inside/outside part below/at-or-above self._n
        self._n = int(len(arr) * self._drift_opts.frac_init)
        # minimum allowed value of n
        self._n0 = int(len(arr) * self._drift_opts.frac_min)
        # maximum allowed value of n
        self._n1 = int(len(arr) * self._drift_opts.frac_max)
        # initially, select inside-indices as range(n)
        self._flag[:] = False
        self._draw[self._ptr[:self._n]] = True
        self._draw[self._ptr[self._n:]] = False

    def __call__(self, rng, block_size, num_blocks):
        # prepare result indices into self._arr
        result_size = block_size * num_blocks
        result = np.empty((result_size,), dtype=_DTYPE_IND)
        # one block at a time, so two sequences of calls with fixed block_size
        # produce the same draws from rng regardless of how num_blocks varies
        for i in range(0, result_size, block_size):
            result[i:i+block_size] = self._do_draw(rng, block_size)
            # drift not yet for drawing
            if rng.random() < self._drift_opts.prob_drift:
                self._do_drift(rng)
                self._flag[self._ptr[:self._n]] = True
            # countdown to next drift chance
            self._driftb -= 1
            if self._driftb == 0:
                self._driftb = self._drift_opts.drift_blocks
                # drift for drawing
                self._draw[:] = self._flag[:]
                # reselect inside-indices
                self._flag[:] = False
        return self._arr[result]

    def _do_draw(self, rng, block_size):
        # uniform draw from the set of selected indices into current block
        ind1 = self._ind[self._draw]
        ind2 = rng.integers(low=0, high=len(ind1),
                size=block_size, dtype=_DTYPE_IND)
        return ind1[ind2]

    def _do_drift(self, rng):
        n = self._n
        # shuffle inside and outside parts
        rng.shuffle(self._ptr[:n])
        rng.shuffle(self._ptr[n:])
        # repartition
        rng.random(size=len(self._prob), out=self._prob)
        n_add = sum(self._prob[n:] < self._drift_opts.prob_add)
        n_del = sum(self._prob[:n] < self._drift_opts.prob_del)
        n_adj = n + n_add - n_del
        n_add -= max(0, n_adj - self._n1)
        n_del -= max(0, self._n0 - n_adj)
        # update partition
        na, nb, nn = n - n_del, n + n_add, n + n_add - n_del
        self._buf[na:nb] = self._ptr[na:nb] # save del and add parts
        self._ptr[na:nn] = self._buf[n:nb]  # place add part inside
        self._ptr[nn:nb] = self._buf[na:n]  # place del part outside
        self._n = nn


class NormalDistribution(ArrayDistribution):
    """
    Samples from a normal distribution.
    """
    def __init__(self, loc=0.0, scale=1.0):
        super().__init__(np.float64)
        self.loc = loc
        self.scale = scale

    def __call__(self, rng, block_size, num_blocks):
        return rng.normal(
            loc=self.loc, scale=self.scale, size=block_size*num_blocks)

class SquaredNormalDistribution(ArrayDistribution):
    """
    Samples from a squared normal distribution.
    """
    def __init__(self, loc=0.0, scale=1.0):
        super().__init__(np.float64)
        self.loc = loc
        self.scale = scale

    def __call__(self, rng, block_size, num_blocks):
        x = rng.normal(
            loc=self.loc, scale=self.scale, size=block_size*num_blocks)
        return x * x

def _get_array(ids):
    if isinstance(ids, np.ndarray):
        return ids
    elif type(ids) in [type(0), type([])]:
        return np.arange(ids, dtype=_DTYPE_ID)
    raise ValueError("invalid ids " + str(ids))

def _get_dist(dist):
    if type(dist) == type(""):
        if dist == "all":
            return ArrayAllDistribution
        elif dist == "uniform":
            return ArrayUniformDistribution
        elif dist == "uniform-drift":
            return ArrayUniformDriftDistribution
    elif isinstance(dist, Distribution):
        return dist
    raise ValueError("invalid dist " + str(dist))

def _get_id_dist(ids=_DEFAULT_NUM_IDS, dist="all"):
    return _get_dist(dist)(_get_array(ids))

def _get_category_id_dist(ids=_DEFAULT_NUM_FEATURE_CATEGORIES, dist="all"):
    return _get_dist(dist)(_get_array(ids))

class DataGenerator:
    """
    A data generator. See the generate(...) method for details.
    """

    def _normalize_date(self, date_value, default_date):
        """
        Normalizes to a date. If the date value is a string, it is parsed in ISO
        format and returned. If it is already a date, it is returned as is. If
        it is None, it defaults to the given default value.
        """
        if isinstance(date_value, str):
            return date.fromisoformat(date_value)
        elif isinstance(date_value, date):
            return date_value
        elif date_value is None:
            return default_date
        else:
            raise ValueError("invalid date: " + str(date_value))

    def _date_to_midnight_ns(self, curr_date):
        """
        Returns a datetime at midnight on the given date.
        """
        midnight = datetime.combine(curr_date, datetime.min.time()).timestamp()
        return int(midnight * 1000000000)

    def generate(self, begin_date=None, end_date=None, seed=None, calendar=None,
            freq="200ms", time_dist="constant", dists=None, num_ids=1,
            batch_size=_DEFAULT_BATCH_SIZE):
        """
        Generates market data using these paremeters:

            begin_date  the date on which to begin generating.
                        Defaults to the minimum date.
            end_date    the (inclusive) date on which to end generating.
                        Defaults to the maximum date.
            seed        the seed to use for pseudorandom generation.
                        Defaults to a random seed.
            calendar    the calendar defining trading dates and hours.
                        Defaults to SimpleMidnightCalendar().
            freq        the time frequency of periods to generate with.
                        Defaults to "200ms".
            time_dist   the distribution of times within a period.
                        Defaults to "constant". Also accepts "uniform".
            dists       list of (name, distribution) tuples.
            num_ids     the number of ids to generate per period.
            batch_size  the maximum number of time points to generate per batch.
                        Defaults to 100. Taken as at least 1.

        Returns an iterator of RecordBatch instances, each with a schema like:

            "time" timestamp
            name0 dtype0
            ...

        The BatchRecord may have fewer time points than requested in batch_size
        when the date range or business date ends.

        Example:

>>> dgen = DataGenerator()
>>> for batch in dgen.generate("2020-01-01", "2020-01-01", seed=1,
...         freq="10h",
...         dists=[
...             ("id", ArrayAllDistribution(np.arange(3, dtype=np.int32))),
...             ("value", NormalDistribution())
...         ],
...         num_ids=3, batch_size=2):
...     for item in batch.to_pydict().items():
...         print(item)
... 
('time', [Timestamp('2020-01-01 05:00:00+0000', tz='UTC'), Timestamp('2020-01-01 05:00:00+0000', tz='UTC'), Timestamp('2020-01-01 05:00:00+0000', tz='UTC')])
('id', [0, 1, 2])
('value', [0.345584192064786, 0.8216181435011584, 0.33043707618338714])
        """

        begin_date = self._normalize_date(begin_date, date.min)
        end_date = self._normalize_date(end_date, date.max)
        if calendar is None:
            calendar = SimpleMidnightCalendar()
        offset_ns = to_offset(freq).nanos
        if time_dist not in ["constant", "uniform"]:
            raise ValueError("invalid time_dist: " + str(time_dist))
        if dists is None:
            dists = []
        batch_size = max(1, batch_size)

        schema = pa.schema(
                [("time", pa.timestamp('ns', 'UTC'))] +
                [(name, _arrow_type(dist.dtype)) for name, dist in dists]
        )

        uniform_time_dist = (time_dist == "uniform")
        rng = np.random.default_rng(seed=seed)
        one_day = timedelta(days=1)
        curr_date = begin_date
        curr_date_ns = self._date_to_midnight_ns(curr_date)
        while True:
            if calendar.is_business_day(curr_date):
                begin_time_ns = (
                        curr_date_ns + calendar.business_begin_ns(curr_date))
                end_time_ns = (
                        curr_date_ns + calendar.trading_end_ns(curr_date))
                curr_offset_ns = min(offset_ns, end_time_ns - begin_time_ns + 1)
                while begin_time_ns <= end_time_ns:
                    times_num = 1 + (
                            (end_time_ns - begin_time_ns) // curr_offset_ns)
                    batch_num = min(times_num, batch_size)
                    curr_array_size = (num_ids * batch_num,)
                    batch_end_time_ns = (
                            begin_time_ns + (batch_num - 1) * curr_offset_ns)

                    time_ns_value_array = np.arange(
                            begin_time_ns, batch_end_time_ns + 1,
                            curr_offset_ns, dtype=np.uint64)
                    time_ns_array = np.repeat(
                        time_ns_value_array, num_ids).astype('datetime64[ns]')
                    if uniform_time_dist:
                        dist_offset_ns = min(curr_offset_ns,
                                end_time_ns - begin_time_ns)
                        time_ns_array += rng.integers(low=0,
                                high=dist_offset_ns, size=curr_array_size,
                                dtype=np.uint64)
                        time_ns_array = np.sort(time_ns_array)
                    arrays = ([time_ns_array] +
                        [dist(rng, num_ids, batch_num) for _, dist in dists])
                    parrays = [pa.array(a) for a in arrays]

                    batch = pa.RecordBatch.from_arrays(parrays, schema=schema)
                    yield batch

                    begin_time_ns += batch_num * curr_offset_ns
            if curr_date == end_date:
                break
            curr_date += one_day
            curr_date_ns = self._date_to_midnight_ns(curr_date)

class BasicDataGenerator:
    def __init__(self):
        self.datagen = DataGenerator()

    """
    A basic data generator. See the generate(...) method for details.
    """

    @_doc(ids_default=_DEFAULT_NUM_IDS)
    def generate(self, begin_date=None, end_date=None, seed=None, calendar=None,
            freq="200ms", time_dist="constant", ids=_DEFAULT_NUM_IDS,
            ids_dist="all", cols=20, batch_size=_DEFAULT_BATCH_SIZE):
        """
        Generates market data using these paremeters:

            begin_date  the date on which to begin generating.
                        Defaults to the minimum date.
            end_date    the (inclusive) date on which to end generating.
                        Defaults to the maximum date.
            seed        the seed to use for pseudorandom generation.
                        Defaults to a random seed.
            calendar    the calendar defining trading dates and hours.
                        Defaults to SimpleBusinessCalendar().
            freq        the time frequency of periods to generate with.
                        Defaults to "200ms". Minimum is "100ms".
            time_dist   the distribution of times within a period.
                        Defaults to "constant". Also accepts "uniform".
            ids         the number or list of ids to generate each time.
                        Defaults to {ids_default}.
                        Taken as at least 1.
            ids_dist    the distribution of ids within a period.
                        Defaults to "all". Also accepts "uniform" and
                        "uniform-drift".
            cols        the number of list of column names to generate.
                        Defaults to 20. Taken as at least 1.
            batch_size  the maximum number of time points to generate per batch.
                        Defaults to 100. Taken as at least 1.

        Returns an iterator of RecordBatch instances, each with this schema:

            "time_ns" uint64
            "id" int32
            column1 float64
            ...
            columnN float64

        The column names default to ["column" + str(i) for i in range(N)] where
        N is the number of columns. The BatchRecord will have fewer time points
        than requested in batch_size when the date range or trading date ends.

        Example (takes a couple of minutes):

            >>> num_batches = total_rows = total_bytes = 0
            >>> mdgen = BasicDataGenerator()
            >>> for batch in mdgen.generate("2020-01-01", "2020-01-01", seed=1):
            ...     num_batches += 1
            ...     total_rows += batch.num_rows
            ...     total_bytes += batch.nbytes
            ... 
            >>> (num_batches, total_rows, total_bytes)
            (1261, 630005000, 108360860000)
        """
        if calendar is None:
            calendar = SimpleBusinessCalendar()
        freq = _max_freq([freq, "100ms"])
        if not isinstance(ids, int):
            raise ValueError("invalid ids " + str(ids))
        if isinstance(cols, int):
            cols = ["column_" + str(i) for i in range(max(1, cols))]
        else:
            cols = list(cols)

        dists = (
            [("id", _get_id_dist(max(1, ids), ids_dist))] +
            [(col, NormalDistribution()) for col in cols]
        )

        return self.datagen.generate(begin_date=begin_date, end_date=end_date,
            seed=seed, calendar=calendar, freq=freq, time_dist=time_dist,
            dists=dists, num_ids=ids, batch_size=batch_size)

class StateDataGenerator:
    def __init__(self):
        self.dgen = DataGenerator()

    def generate(self, begin_date=None, end_date=None, seed=None,
            freq="30T", batch_size=_DEFAULT_BATCH_SIZE):
        """
        Generates basic data drawn from various distributions using these paremeters:

            begin_date  the date on which to begin generating.
                        Defaults to the minimum date.
            end_date    the (inclusive) date on which to end generating.
                        Defaults to the maximum date.
            seed        the seed to use for pseudorandom generation.
                        Defaults to a random seed.
            freq        the time frequency of periods to generate with.
                        Defaults to "30T". Minimum is "30T".
            batch_size  the maximum number of time points to generate per batch.
                        Defaults to 100. Taken as at least 1.

        Example:

        >>> sdg = StateDataGenerator()
        >>> for batch in sdg.generate("2020-01-01", "2020-01-01", seed=1):
        ...     for item in batch.to_pydict().items():
        ...         print((item[0], item[1][:1]))
        ...     break
        ... 
        ('time', [Timestamp('2020-01-01 05:00:00+0000', tz='UTC')])
        ('id', [0])
        ('c0', [0.345584192064786])
        ('c1', [0.599476654534323])
        ('c2', [0.338346476063111])
        ('c3', [484732503.5147238])
        ('c4', [-0.43637535848759307])
        ('c5', [0])
        ('c6', [-0.14101551119463715])
        """
        freq = _max_freq([freq, "30T"])
        dists = [
            ("id", _get_id_dist()),
            ("c0", NormalDistribution()),
            ("c1", NormalDistribution()),
            ("c2", SquaredNormalDistribution()),
            ("c3", SquaredNormalDistribution(0.0, 20000.0)),
            ("c4", NormalDistribution()),
            ("c5", _get_category_id_dist()),
            ("c6", NormalDistribution()),
        ]
        return self.dgen.generate(begin_date=begin_date, end_date=end_date,
            seed=seed, calendar=SimpleDailyBusinessCalendar(), freq=freq,
            time_dist="constant", dists=dists, num_ids=_DEFAULT_NUM_IDS,
            batch_size=batch_size)

class CompleteDataGenerator:
    def __init__(self):
        self.mdgen = BasicDataGenerator()

    def generate(self, begin_date=None, end_date=None, seed=None,
            freq="24h", batch_size=_DEFAULT_BATCH_SIZE):
        """
        Generates basic data with all ids using these parameters:

            begin_date  the date on which to begin generating.
                        Defaults to the minimum date.
            end_date    the (inclusive) date on which to end generating.
                        Defaults to the maximum date.
            seed        the seed to use for pseudorandom generation.
                        Defaults to a random seed.
            freq        the time frequency of periods to generate with.
                        Defaults to "24h". Minimum is "24h".
            batch_size  the maximum number of time points to generate per batch.
                        Defaults to 100. Taken as at least 1.

            batch_size=None):

        Example:

        >>> cdgen = CompleteDataGenerator()
        >>> for batch in cdgen.generate("2020-01-01", "2020-01-01", seed=1):
        ...     for item in list(batch.to_pydict().items())[-10:]:
        ...         print((item[0], item[1][:1]))
        ...     break
        ... 
        ('column_131', [2.749462981735764])
        ('column_132', [1.1768217718084364])
        ('column_133', [1.0369319084482733])
        ('column_134', [-0.9942706167664396])
        ('column_135', [0.9809507712347781])
        ('column_136', [-1.1456103231734929])
        ('column_137', [-0.8939909449630671])
        ('column_138', [-0.28543920874737394])
        ('column_139', [1.3280087243059246])
        ('column_140', [-1.1114318678329866])
        """
        freq = _max_freq([freq, "24h"])
        return self.mdgen.generate(begin_date=begin_date, end_date=end_date,
            seed=seed, calendar=SimpleDailyBusinessCalendar(), freq=freq,
            time_dist="constant", ids=_DEFAULT_NUM_IDS, ids_dist="all",
            cols=141, batch_size=batch_size)

class CompleteDailyDataGenerator:
    def __init__(self):
        self.mdgen = BasicDataGenerator()

    def generate(self, begin_date=None, end_date=None, seed=None,
            freq="24h", batch_size=_DEFAULT_BATCH_SIZE):
        """
        Generates basic data over all ids at least once a day using these paremeters:

            begin_date  the date on which to begin generating.
                        Defaults to the minimum date.
            end_date    the (inclusive) date on which to end generating.
                        Defaults to the maximum date.
            seed        the seed to use for pseudorandom generation.
                        Defaults to a random seed.
            freq        the time frequency of periods to generate with.
                        Defaults to "24h". Minimum is "24h".
            batch_size  the maximum number of time points to generate per batch.
                        Defaults to 100. Taken as at least 1.

            batch_size=None):

        Example:

        >>> cddg = CompleteDailyDataGenerator()
        >>> for batch in cddg.generate("2020-01-01", "2020-01-01", seed=1):
        ...     for item in list(batch.to_pydict().items())[-10:]:
        ...         print((item[0], item[1][:1]))
        ...     break
        ... 
        ('column_75', [1.3668288118895462])
        ('column_76', [0.09800763059526184])
        ('column_77', [0.30368723055036645])
        ('column_78', [0.7207469586450851])
        ('column_79', [-0.8567123247414715])
        ('column_80', [1.3452920309743779])
        ('column_81', [2.136508741914908])
        ('column_82', [-0.6624693897167523])
        ('column_83', [0.6089530774546572])
        ('column_84', [-1.5991155529278167])
        """
        freq = _max_freq([freq, "24h"])
        return self.mdgen.generate(begin_date=begin_date, end_date=end_date,
            seed=seed, calendar=SimpleDailyBusinessCalendar(), freq="24h",
            time_dist="constant", ids=_DEFAULT_NUM_IDS, ids_dist="all",
            cols=85, batch_size=batch_size)

class CompleteMonthlyDataGenerator:
    def __init__(self):
        self.mdgen = BasicDataGenerator()

    def generate(self, begin_date=None, end_date=None, seed=None,
            freq="30T", batch_size=_DEFAULT_BATCH_SIZE):
        """
        Generates forecast data using these paremeters:

            begin_date  the date on which to begin generating.
                        Defaults to the minimum date.
            end_date    the (inclusive) date on which to end generating.
                        Defaults to the maximum date.
            seed        the seed to use for pseudorandom generation.
                        Defaults to a random seed.
            freq        the time frequency of periods to generate with.
                        Defaults to "30T". Minimum is "30T".
            batch_size  the maximum number of time points to generate per batch.
                        Defaults to 100. Taken as at least 1.

            batch_size=None):

        Example:

        >>> cmdgen = CompleteMonthlyDataGenerator()
        >>> for batch in cmdgen.generate("2020-01-01", "2020-01-01", seed=1):
        ...     for item in batch.to_pydict().items():
        ...         print((item[0], item[1][:1]))
        ...     break
        ... 
        ('time', [Timestamp('2020-01-01 14:00:00+0000', tz='UTC')])
        ('id', [0])
        ('data', [0.345584192064786])
        """
        freq = _max_freq([freq, "30T"])
        return self.mdgen.generate(begin_date=begin_date, end_date=end_date,
            seed=seed, calendar=SimpleBusinessCalendar(), freq=freq,
            time_dist="constant", ids=_DEFAULT_NUM_IDS, ids_dist="all",
            cols=["data"], batch_size=batch_size)

class FeatureDataGenerator:
    def __init__(self):
        self.dgen = DataGenerator()

    def generate(self, begin_date=None, end_date=None, seed=None,
            freq="24h", batch_size=_DEFAULT_BATCH_SIZE):
        """
        Generates features for ids data using these paremeters:

            begin_date  the date on which to begin generating.
                        Defaults to the minimum date.
            end_date    the (inclusive) date on which to end generating.
                        Defaults to the maximum date.
            seed        the seed to use for pseudorandom generation.
                        Defaults to a random seed.
            freq        the time frequency of periods to generate with.
                        Defaults to "24h". Minimum is "24h".
            batch_size  the maximum number of time points to generate per batch.
                        Defaults to 100. Taken as at least 1.

            batch_size=None):

        Example:

        >>> fdgen = FeatureDataGenerator()
        >>> for batch in fdgen.generate("2020-01-01", "2020-01-01", seed=1):
        ...     for item in batch.to_pydict().items():
        ...         print((item[0], item[1][:1]))
        ...     break
        ... 
        ('time', [Timestamp('2020-01-01 05:00:00+0000', tz='UTC')])
        ('id', [0])
        ('f0', [0.345584192064786])
        ('f1', ['Alice'])
        ('f2', ['Bob'])
        ('f3', ['Alice'])
        ('f4', ['Bob'])
        ('f5', [10203040])
        ('f6', [10203040])
        ('f7', [10203040])
        ('f8', [10203040])
        """
        freq = _max_freq([freq, "24h"])
        dists = [
            ("id", _get_id_dist()),
            ("f0", NormalDistribution()),
            ("f1",
                _get_id_dist(_DEFAULT_DETAIL_NAME)),
            ("f2",
                _get_id_dist(_DEFAULT_DETAIL_NAME)),
            ("f3",
                _get_id_dist(_DEFAULT_DETAIL_NAME)),
            ("f4",
                _get_id_dist(_DEFAULT_DETAIL_NAME)),
            ("f5",
                _get_id_dist(_DEFAULT_DETAIL_IDS)),
            ("f6",
                _get_id_dist(_DEFAULT_DETAIL_IDS)),
            ("f7",
                _get_id_dist(_DEFAULT_DETAIL_IDS)),
            ("f8",
                _get_id_dist(_DEFAULT_DETAIL_IDS)),
        ]
        return self.dgen.generate(begin_date=begin_date, end_date=end_date,
            seed=seed, calendar=SimpleDailyBusinessCalendar(), freq=freq,
            time_dist="constant", dists=dists, num_ids=_DEFAULT_NUM_IDS,
            batch_size=batch_size)

class DefaultDataGenerator:
    def __init__(self):
        self.dgen = DataGenerator()

    def generate(self, begin_date=None, end_date=None, seed=None,
            freq="24h", batch_size=_DEFAULT_BATCH_SIZE):
        """
        Generates conditioner data using these paremeters:

            begin_date  the date on which to begin generating.
                        Defaults to the minimum date.
            end_date    the (inclusive) date on which to end generating.
                        Defaults to the maximum date.
            seed        the seed to use for pseudorandom generation.
                        Defaults to a random seed.
            freq        the time frequency of periods to generate with.
                        Defaults to "24h". Minimum is "24h".
            batch_size  the maximum number of time points to generate per batch.
                        Defaults to 100. Taken as at least 1.

            batch_size=None):

        Example:

        >>> ddgen = DefaultDataGenerator()
        >>> for batch in ddgen.generate("2020-01-01", "2020-01-01", seed=1):
        ...     for item in batch.to_pydict().items():
        ...         print((item[0], item[1][:1]))
        ...     break
        ... 
        ('time', [Timestamp('2020-01-01 05:00:00+0000', tz='UTC')])
        """
        freq = _max_freq([freq, "24h"])
        dists = [
            ("id", _get_id_dist()),
        ]
        return self.dgen.generate(begin_date=begin_date, end_date=end_date,
            seed=seed, calendar=SimpleDailyBusinessCalendar(), freq=freq,
            time_dist="constant", dists=dists, num_ids=_DEFAULT_NUM_IDS,
            batch_size=batch_size)
