import os
import time
from pathlib import Path
from typing import Callable

flat_map = lambda f, xs: [y for ys in xs for y in f(ys)]

DATA_PATH = Path(os.getcwd()).parent / 'data.csv'


def simple_bench(fn: Callable, warm_up: int = 10, test_iterations: int = 100, verbose: bool = False):
    for i in range(warm_up):
        start_time = time.time()
        fn()
        end_time = time.time() - start_time
        if verbose:
            print(f"warm up #{i} [{end_time}sec]")

    times: list[float] = []
    for i in range(test_iterations):
        start_time = time.time()
        fn()
        end_time = time.time() - start_time
        times.append(end_time)
        if verbose:
            print(f"test #{i} [{end_time}sec]")

    print(float(sum(times)) / len(times))
