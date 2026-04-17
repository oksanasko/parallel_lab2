import os
import random
import re
from collections import Counter
from concurrent.futures import ProcessPoolExecutor
import time


# All tags we will simulate
TAGS = ["html", "body", "div", "p", "span", "a", "ul", "li", "table", "tr", "td"]
TAG_REGEX = re.compile(r"<(\w+)>")

def generate_html_dataset(folder=r"C:\Users\user\Desktop\унік\2сем\parall\laba2\htmls", n_files=1000):
    """
    Generates synthetic HTML files for testing concurrency patterns.
    """
    os.makedirs(folder, exist_ok=True)

    for i in range(n_files):
        # random file size (number of tags in file)
        size = random.randint(50, 200)

        content = ""

        for _ in range(size):
            tag = random.choice(TAGS)
            content += f"<{tag}>content</{tag}>\n"

        file_path = os.path.join(folder, f"file_{i}.html")

        with open(file_path, "w", encoding="utf-8") as f:
            f.write(content)

    print(f"Generated {n_files} HTML files in '{folder}'")


def count_tags_in_file(filepath):
    """
    Extracts all HTML tags from one file.
    Returns a Counter dictionary.
    """
    with open(filepath, "r", encoding="utf-8") as f:
        text = f.read()

    tags = TAG_REGEX.findall(text)
    return Counter(tags)


def sequential_count(folder):
    """
    Sequential processing of all HTML files.
    """
    total = Counter()

    for filename in os.listdir(folder):
        filepath = os.path.join(folder, filename)
        total += count_tags_in_file(filepath)

    return total

def map_reduce_count(folder):
    files = [os.path.join(folder, f) for f in os.listdir(folder)]

    # MAP phase: parallel processing of files
    with ProcessPoolExecutor() as executor:
        partial_results = list(executor.map(count_tags_in_file, files))

    # REDUCE phase: combine results
    total = Counter()
    for result in partial_results:
        total.update(result) 

    return total

def worker_pool_count(folder):
    files = [os.path.join(folder, f) for f in os.listdir(folder)]

    total = Counter()

    # Worker pool distributes tasks automatically
    with ProcessPoolExecutor(max_workers=4) as executor:
        for result in executor.map(count_tags_in_file, files):
            total += result

    return total

THRESHOLD = 100

def fork_join_count(files):
    """
    Recursive divide-and-conquer implementation.
    """
    
    # BASE CASE: small enough to process directly
    if len(files) <= THRESHOLD:
        total = Counter()
        for f in files:
            total += count_tags_in_file(f)
        return total

    # SPLIT
    mid = len(files) // 2
    left = files[:mid]
    right = files[mid:]

    # RECURSIVE PARALLEL EXECUTION
    with ProcessPoolExecutor() as executor:
        left_future = executor.submit(fork_join_count, left)
        right_result = fork_join_count(right)

        left_result = left_future.result()

    # COMBINE
    return left_result + right_result


if __name__ == "__main__":
    base_dir = r"C:\Users\user\Desktop\унік\2сем\parall\laba2\htmls"
    generate_html_dataset(n_files=100_000)
    # ===========================
    # start = time.time()
    # tagscount = sequential_count(base_dir)
    # end = time.time()
    # print(f"Time: {end - start:.4f} sec for sequential_count")
    # for tag, count in tagscount.items():
    #     print(f"{tag}: {count}")

    # ===========================
    start = time.time()
    tagscount = map_reduce_count(base_dir)
    end = time.time()
    print(f"Time: {end - start:.4f} sec for map_reduce_count")
    for tag, count in tagscount.items():
        print(f"{tag}: {count}")

    # ===========================
    start = time.time()
    tagscount = worker_pool_count(base_dir)
    end = time.time()
    print(f"Time: {end - start:.4f} sec for worker_pool_count")
    for tag, count in tagscount.items():
        print(f"{tag}: {count}")

    # ===========================
    start = time.time()
    files = [os.path.join(base_dir, f) for f in os.listdir(base_dir)]
    tagscount = fork_join_count(files)
    end = time.time()
    print(f"Time: {end - start:.4f} sec for fork_join_count")
    for tag, count in tagscount.items():
        print(f"{tag}: {count}")
    
    
   