from collections import Counter

from common import simple_bench, flat_map, DATA_PATH


def count_words(text: str) -> Counter:
    lower = text.lower()
    cols = lower.split(',')
    words = flat_map(lambda c: c.split(' '), cols)
    words = map(lambda w: w + '1', words)
    return Counter(words)


def main():
    total_count: Counter = Counter()
    for line in open(DATA_PATH, encoding="utf8"):
        total_count += count_words(line)


if __name__ == '__main__':
    simple_bench(main, warm_up=3, test_iterations=5, verbose=True)
