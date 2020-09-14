import multiprocessing as mp
from multiprocessing import Process, current_process, Queue, Manager, Lock
from threading import Thread, Timer
from typing import Dict
from common import flat_map, DATA_PATH

POISON = "POISON"
LOCK = Lock()


class MultiprocessWordCounter:
    def __init__(self, num_workers: int = mp.cpu_count()):
        self.queue = Queue()
        self.process_pool = []
        self.counter = Manager().dict()

        for i in range(num_workers):
            self.process_pool.append(Process(target=self.count_words, args=(i, self.queue, self.counter)))

        [t.start() for t in self.process_pool]
        Thread(target=self.monitor_task, args=(self.queue,), daemon=True).start()

    def consume(self, text: str):
        self.queue.put(text)

    @staticmethod
    def count_words(worker_id: int, queue: Queue, mp_counter: Dict):
        dict_counter = dict()
        print(f"worker #{worker_id}({current_process().pid})")
        while current_process().is_alive():
            text = queue.get()
            if text == POISON:
                print(f"worker #{worker_id}({current_process().pid}) found poison")
                queue.put(POISON)
                with LOCK:
                    for k, v in dict_counter.items():
                        if k in mp_counter:
                            mp_counter[k] = mp_counter[k] + v
                        else:
                            mp_counter[k] = v
                return

            lower = text.lower()
            cols = lower.split(',')
            words = flat_map(lambda c: c.split(' '), cols)
            words = list(map(lambda w: w + '1', words))

            if len(words) != 0:
                for w in words:
                    if w in dict_counter:
                        dict_counter[w] = dict_counter[w] + 1
                    else:
                        dict_counter[w] = 1

    def join(self):
        [t.join() for t in self.process_pool]

    @staticmethod
    def monitor_task(queue: Queue):
        print(f"Queue size: {queue.qsize()}")
        Timer(1, function=MultiprocessWordCounter.monitor_task, args=(queue,)).start()


def run():
    multiprocess = MultiprocessWordCounter(10)

    lines_read = 0
    with open(DATA_PATH, encoding="utf8") as f:
        for line in f:
            lines_read += 1
            multiprocess.consume(line)

    print(f"done processing {lines_read} lines")

    multiprocess.consume(POISON)

    print("waiting shutdown")
    multiprocess.join()


if __name__ == '__main__':
    from common import simple_bench
    simple_bench(run, 5, 10, True)
