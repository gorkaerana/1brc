import mmap
from operator import itemgetter
from itertools import count
from concurrent.futures import ProcessPoolExecutor
import os
from pathlib import Path
from typing import Iterable, TypeAlias


PathLike: TypeAlias = os.PathLike | str
# station name -> [min, sum, max, count]
Accumulator: TypeAlias = dict[bytes, list[float | int]]


def pretty_print_solution(d: Accumulator):
    formatted_strings = (
        f"{k.decode('utf-8')}={v[0]}/{v[1] / v[3]}/{v[2]}"
        for k, v in sorted(d.items(), key=itemgetter(0))
    )
    print("{", end="")
    print(", ".join(formatted_strings), end="")
    print("}")


def update_in_place(line: bytes, d: Accumulator):
    station, temperature = line.split(b";")
    temp = float(temperature.decode("utf-8"))
    if station not in d:
        d[station] = [float("+inf"), 0, float("-inf"), 0]
    old_min, _, old_max, _ = old = d[station]
    if temp < old_min:
        old[0] = temp
    old[1] += temp
    if temp > old_max:
        old[2] = temp
    old[3] += 1


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
        return process_batch(fp.read(end - start))


def process_batch_from_indices_mmap(*args):
    mm: mmap.mmap
    start: int
    end: int
    (mm, start, end), *_ = args
    return process_batch(mm[start : (end - start)])


def consolidate_accumulators(accumulators: Iterable[Accumulator]) -> Accumulator:
    iter_accs = iter(accumulators)
    consolidated: Accumulator = next(iter_accs)
    for accumulator in iter_accs:
        for k, v in accumulator.items():
            if k not in consolidated:
                consolidated[k] = v
                continue
            old_min, old_avg, old_max, _ = old = consolidated[k]
            new_min, new_avg, new_max, new_count = accumulator[k]
            if new_min < old_min:
                old[0] = new_min
            old[1] = old_avg + new_avg
            if new_max > old_max:
                old[2] = new_max
            old[3] += new_count
    return consolidated


if __name__ == "__main__":
    from itertools import islice

    here = Path(__file__).resolve().parent
    data_dir = here.parent.parent.parent / "data"
    n_lines = 10_000
    data_path = data_dir / f"measurements_{n_lines}.txt"
    # data_path = data_dir / "measurements.txt"

    def batched(iterable, n):
        # batched('ABCDEFG', 3) --> ABC DEF G
        if n < 1:
            raise ValueError("n must be at least one")
        it = iter(iterable)
        while batch := tuple(islice(it, n)):
            yield batch

    indices = batch_indices(data_path, 1500)
    index_tuples = zip(indices, indices[1:])
    results: list[Accumulator] = []
    n_workers = 10
    with (
        ProcessPoolExecutor(max_workers=n_workers) as executor,
        open(data_path, "r+b") as fp,
        mmap.mmap(fp.fileno(), 0) as mm,
    ):
        for i, index_batch in enumerate(batched(index_tuples, n_workers)):
            # if i > 0: break
            result = executor.map(
                process_batch,
                [mm[start : (end - start)] for start, end in index_batch],
            )
            results.extend(result)
    final_result = consolidate_accumulators(results)
    # pretty_print_solution(final_result)
