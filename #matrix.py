#matrix
import time
import numpy as np
from concurrent.futures import ProcessPoolExecutor

def generate_matrices(n=1000):
    """
    Generates two random NxN matrices.
    """
    A = np.random.rand(n, n)
    B = np.random.rand(n, n)
    return A, B

def sequential_matrix_multiply(A, B):
    """
    Sequential matrix multiplication using NumPy.
    """
    return np.dot(A, B)

def compute_row(args):
    """
    Computes one row of the result matrix.
    """
    A, B, i = args
    return np.dot(A[i, :], B)

CHUNK_SIZE = 100

def compute_chunk(args):
    """
    Computes multiple rows at once.
    """
    A_chunk, B = args
    return np.dot(A_chunk, B)

def worker_pool_matrix(A, B):
    """
    Worker Pool implementation.
    """
    tasks = []
    for i in range(0, A.shape[0], CHUNK_SIZE):
        chunk = A[i:i+CHUNK_SIZE]
        tasks.append((chunk, B))

    with ProcessPoolExecutor() as executor:
        results = list(executor.map(compute_chunk, tasks))

    return np.vstack(results)

def map_reduce_matrix(A, B):
    """
    Map-Reduce version of matrix multiplication.
    """
    chunks = []
    for i in range(0, A.shape[0], CHUNK_SIZE):
        chunks.append(A[i:i+CHUNK_SIZE])

    # MAP phase
    with ProcessPoolExecutor() as executor:
        partial_results = list(executor.map(compute_chunk,  [(c, B) for c in chunks]))

    # REDUCE phase (combine rows)
    result = np.vstack(partial_results)

    return result

THRESHOLD = 100  # number of rows

def fork_join_matrix(A, B):
    """
    Fork–Join implementation using recursion.
    """

    # BASE CASE
    if A.shape[0] <= THRESHOLD:
        return np.dot(A, B)

    # SPLIT
    mid = A.shape[0] // 2
    top = A[:mid]
    bottom = A[mid:]

    with ProcessPoolExecutor() as executor:
        top_future = executor.submit(fork_join_matrix, top, B)
        bottom_result = fork_join_matrix(bottom, B)

        top_result = top_future.result()

    # JOIN
    return np.vstack((top_result, bottom_result))

if __name__ == "__main__":
    matA, matB = generate_matrices()
    start = time.time()
    mult_matrix = sequential_matrix_multiply(matA, matB)
    end = time.time()
    print(f"Time: {end - start:.4f} sec for sequential_matrix_multiply using NumPy")

    start = time.time()
    mult_matrix_map_reduce = map_reduce_matrix(matA, matB)
    end = time.time()
    correct = np.allclose(mult_matrix, mult_matrix_map_reduce)
    print(f"Time: {end - start:.4f} sec for map_reduce_matrix Correct = {correct}")

    start = time.time()
    mult_matrix_worker_pool = worker_pool_matrix(matA, matB)
    end = time.time()
    correct = np.allclose(mult_matrix, mult_matrix_worker_pool)
    print(f"Time: {end - start:.4f} sec for worker_pool_matrix Correct = {correct}")

    start = time.time()
    mult_matrix_join = fork_join_matrix(matA, matB)
    end = time.time()
    correct = np.allclose(mult_matrix, mult_matrix_join)
    print(f"Time: {end - start:.4f} sec for fork_join_matrix Correct = {correct}")

    
    
        