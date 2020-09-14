import multiprocessing as mp
from queue import Queue
from threading import Thread, current_thread, Timer

from common import flat_map, DATA_PATH

POISON = object()


class ThreadedWordCounter:
    def __init__(self, num_workers: int = mp.cpu_count()):
        self.queue = Queue()
        self.thread_pool = []
        self.counter = dict()

        for i in range(num_workers):
            self.thread_pool.append(Thread(target=self.count_words, args=(i, self.queue, self.counter)))

        [t.start() for t in self.thread_pool]
        Thread(target=self.monitor_task, args=(self.queue,), daemon=True).start()

    def consume(self, text: str):
        self.queue.put(text)

    @staticmethod
    def count_words(worker_id: int, queue: Queue, counter):
        print(f"worker #{worker_id}({current_thread().native_id})")
        while current_thread().is_alive():
            text = queue.get()
            if text == POISON:
                print(f"worker #{worker_id}({current_thread().native_id}) found poison")
                queue.put(POISON)
                return

            lower = text.lower()
            cols = lower.split(',')
            words = flat_map(lambda c: c.split(' '), cols)
            words = list(map(lambda w: w + '1', words))

            if len(words) != 0:
                for w in words:
                    if w in counter:
                        counter[w] = counter[w] + 1
                    else:
                        counter[w] = 1

    def join(self):
        [t.join() for t in self.thread_pool]

    @staticmethod
    def monitor_task(queue: Queue):
        print(f"Queue size: {queue.qsize()}")
        Timer(1, function=ThreadedWordCounter.monitor_task, args=(queue,)).start()


def run():
    threaded_counter = ThreadedWordCounter(10)

    lines_read = 0
    with open(DATA_PATH, encoding="utf8") as f:
        for line in f:
            lines_read += 1
            threaded_counter.consume(line)

    print(f"done processing {lines_read} lines")

    threaded_counter.consume(POISON)

    print("waiting shutdown")
    threaded_counter.join()

    print([(k, v) for k, v in sorted(threaded_counter.counter.items(), key=lambda item: item[1], reverse=True)][:10])


if __name__ == '__main__':
    from common import simple_bench
    simple_bench(run, 5, 100, True)
