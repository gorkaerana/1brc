from contextlib import contextmanager
from operator import itemgetter
from itertools import count
from concurrent.futures import ProcessPoolExecutor
import os
from pathlib import Path
from time import perf_counter_ns
from typing import Iterator, TypeAlias


PathLike: TypeAlias = os.PathLike | str
Accumulator: TypeAlias = dict[bytes, list[float | int]]


def pretty_print_solution(d: Accumulator):
    formatted_strings = (
        f"{k.decode('utf-8')}={v[0]}/{v[1]}/{v[2]}"
        for k, v in sorted(d.items(), key=itemgetter(0))
    )
    print("{", end="")
    print(", ".join(formatted_strings), end="")
    print("}")


def update_in_place(line: bytes, d: Accumulator):
    station, temperature = line.split(b";")
    temp = float(temperature.decode("utf-8"))
    if station in d:
        old_min, old_avg, old_max, old_count = old = d[station]
        if temp < old_min:
            old[0] = temp
        old[1] = old_avg + ((temp - old_avg) / old_count)
        if temp > old_max:
            old[2] = temp
        old[3] += 1
    else:
        d[station] = [float("+inf"), temp, float("-inf"), 1]


def batch_indices(filepath: PathLike, n: int) -> tuple[int, ...]:
    """Returns the byte indices that divide `filepath` in `n` almost-equal chunks.
    We say "almost" cause for easiness downstream we find the closest newline after
    the actual chunking index byte.
    """
    n_bytes = os.path.getsize(filepath)
    chunk_size = n_bytes // (n - 1)
    indices = [0]
    with open(filepath, "rb") as fp:
        for i in range(chunk_size, n_bytes, chunk_size):
            for j in count(i):
                fp.seek(j)
                c = fp.read(1)
                if c == b"\n":
                    break
            indices.append(j)
    indices.append(n_bytes)
    return tuple(indices)


def process_batch(batch: bytes) -> Accumulator:
    d: Accumulator = {}
    for line in batch.splitlines():
        if not line:
            continue
        update_in_place(line, d)
    return d


def process_batch_from_indices(*args):
    filepath: PathLike
    start: int
    end: int
    (filepath, start, end), *_ = args
    with open(filepath, "rb") as fp:
        fp.seek(start)
        print(f"Processing {end - start} bytes")
        return process_batch(fp.read(end - start))


def consolidate_accumulators(accumulators: Iterator[Accumulator]) -> Accumulator:
    iter_accs = iter(accumulators)
    consolidated: Accumulator = next(iter_accs)
    for accumulator in iter_accs:
        common_keys = set(consolidated) & set(accumulator)
        for k, v in accumulator.items():
            if k not in common_keys:
                consolidated[k] = v
                continue
            old_min, old_avg, old_max, old_count = old = consolidated[k]
            new_min, new_avg, new_max, new_count = accumulator[k]
            if new_min < old_min:
                old[0] = new_min
            old[1] = (1 / (old_count + new_count)) * (
                (old_count * old_avg) + (new_count * new_avg)
            )
            if new_max > old_max:
                old[2] = new_max
            old[3] += new_count
    return consolidated


if __name__ == "__main__":
    from itertools import islice

    from rich.progress import track


    here = Path(__file__).resolve().parent
    data_dir = here.parent.parent.parent / "data"
    n_lines = 10_000
    data_path = data_dir / f"measurements_{n_lines}.txt"
    data_path = data_dir / "measurements.txt"

    def batched(iterable, n):
        # batched('ABCDEFG', 3) --> ABC DEF G
        if n < 1:
            raise ValueError('n must be at least one')
        it = iter(iterable)
        while batch := tuple(islice(it, n)):
            yield batch

    indices = batch_indices(data_path, 1250)
    index_tuples = zip(indices, indices[1:])
    from pprint import pprint
    results = []
    n_workers = 8
    with ProcessPoolExecutor(max_workers=n_workers) as executor:
        for i, index_batch in track(enumerate(batched(index_tuples, n_workers))):
            # if i > 0: break
            result = executor.map(
                process_batch_from_indices,
                [(data_path, start, end) for start, end in index_batch],
            )
            results.extend(result)
    final_result = consolidate_accumulators(results)
    pretty_print_solution(final_result)
