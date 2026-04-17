#min_max_masive

import numpy as np
import time
from concurrent.futures import ProcessPoolExecutor

def generate_large_array(n=1_000_000):
    """
    Generates a large dataset of numbers.
    Non-normal distribution (exponential) for realism.
    """
    return np.random.exponential(scale=10, size=n)


def sequential_stats(arr):
    """
    Baseline sequential computation.
    """
    return {
        "min": np.min(arr),
        "max": np.max(arr),
        "mean": np.mean(arr),
        "median": np.median(arr)
    }


def chunk_stats(chunk):
    """
    Compute partial statistics for a chunk.
    """
    return {
        "min": np.min(chunk),
        "max": np.max(chunk),
        "sum": np.sum(chunk),
        "count": len(chunk),
        "median": np.median(chunk)
    }


def map_reduce_stats(arr, chunks=8):
    """
    Map-Reduce implementation of statistics.
    """
    split_data = np.array_split(arr, chunks)

    # MAP
    with ProcessPoolExecutor() as executor:
        results = list(executor.map(chunk_stats, split_data))

    # REDUCE
    global_min = min(r["min"] for r in results)
    global_max = max(r["max"] for r in results)
    global_sum = sum(r["sum"] for r in results)
    global_count = sum(r["count"] for r in results)
    mean = global_sum / global_count
    chunk_medians = [r["median"] for r in results]
    weights = [r["count"] for r in results]

    weighted_average_of_medians=np.average(chunk_medians, weights=weights)

    # median cannot be computed properly here without full data
    median = np.median(arr)
    median = weighted_average_of_medians

    return {
        "min": global_min,
        "max": global_max,
        "mean": mean,
        "median": median
    }

def worker_pool_stats(arr):
    """
    Worker Pool version of statistics computation.
    """
    chunks = np.array_split(arr, 8)

    with ProcessPoolExecutor(max_workers=4) as executor:
        results = list(executor.map(chunk_stats, chunks))

    global_min = min(r["min"] for r in results)
    global_max = max(r["max"] for r in results)
    global_sum = sum(r["sum"] for r in results)
    global_count = sum(r["count"] for r in results)

    chunk_medians = [r["median"] for r in results]
    weights = [r["count"] for r in results]

    median =np.average(chunk_medians, weights=weights)

    return {
        "min": global_min,
        "max": global_max,
        "mean": global_sum / global_count,
        "median": median 
    }

THRESHOLD = 100_000

def fork_join_stats(arr):
    """
    Recursive Fork–Join implementation.
    """

    # BASE CASE
    if len(arr) <= THRESHOLD:
        return {
            "min": np.min(arr),
            "max": np.max(arr),
            "sum": np.sum(arr),
            "count": len(arr),
            "median": np.median(arr)
        }

    mid = len(arr) // 2

    with ProcessPoolExecutor() as executor:
        left_future = executor.submit(fork_join_stats, arr[:mid])
        right_result = fork_join_stats(arr[mid:])

        left_result = left_future.result()

    # JOIN step (correct aggregation)
    total_count = left_result["count"] + right_result["count"]

    # weighted median (APPROXIMATION)
    median = (
        left_result["median"] * left_result["count"] +
        right_result["median"] * right_result["count"]
    ) / total_count

    # JOIN step
    return {
        "min": min(left_result["min"], right_result["min"]),
        "max": max(left_result["max"], right_result["max"]),
        "sum": left_result["sum"] + right_result["sum"],
        "count": left_result["count"] + right_result["count"],
        "median": median
    }

if __name__ == "__main__":
    arr = generate_large_array(n=1_000_000)
    start = time.time()
    statscount = sequential_stats(arr)
    end = time.time()
    print(f"Time: {end - start:.4f} sec for sequential_stats")
    for tag, count in statscount.items():
        print(f"{tag}: {count}")
    #======================================
    start = time.time()
    statscount = map_reduce_stats(arr)
    end = time.time()
    print(f"Time: {end - start:.4f} sec for map_reduce_stats")
    for tag, count in statscount.items():
        print(f"{tag}: {count}")
    #=====================================
    start = time.time()
    statscount = worker_pool_stats(arr)
    end = time.time()
    print(f"Time: {end - start:.4f} sec for worker_pool_stats")
    for tag, count in statscount.items():
        print(f"{tag}: {count}") 
    #=====================================
    start = time.time()
    statscount = fork_join_stats(arr)
    end = time.time()
    print(f"Time: {end - start:.4f} sec for fork_join_stats")
    for tag, count in statscount.items():
        print(f"{tag}: {count}")
