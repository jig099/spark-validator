
from functools import reduce
from itertools import groupby
from operator import (itemgetter,add)


def reduceByKey(func, iterable):
    """Reduce by key.
    Equivalent to the Spark counterpart
    Inspired by http://stackoverflow.com/q/33648581/554319
    1. Sort by key
    2. Group by key yielding (key, grouper)
    3. For each pair yield (key, reduce(func, last element of each grouper))
    """
    # iterable.groupBy(_._1).map(l => (l._1, l._2.map(_._2).reduce(func)))
    return ( (k, reduce(func, (x[1] for x in xs)))
                 for (k,xs) in groupby(sorted(iterable, key=itemgetter(0)), itemgetter(0))
        )

if __name__ == '__main__':
    # Example from https://www.safaribooksonline.com/library/view/learning-spark/9781449359034/ch04.html#idp7549488
    data = [(1, 2), (3, 4), (3, 6)]
    expected_result = [(1, 2), (3, 10)]
    assert list(reduceByKey(add, data)) == expected_result