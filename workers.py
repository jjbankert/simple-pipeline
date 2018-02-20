import multiprocessing


def queue_iterator(queue):
    while True:
        item = queue.get()

        if item is None:
            break

        yield item


class Worker(multiprocessing.Process):
    def __init__(self, name):
        multiprocessing.Process.__init__(self, name=name)


class Producer(Worker):
    def __init__(self, name, out_queue):
        Worker.__init__(self, name)
        self.out_queue = out_queue

    def run(self):
        self.process()

    def process(self):
        raise NotImplementedError("Should have subclassed the Producer")


class Mapper(Worker):
    def __init__(self, name, in_queue, out_queue):
        Worker.__init__(self, name)
        self.in_queue = in_queue
        self.out_queue = out_queue

    def run(self):
        for item in queue_iterator(self.in_queue):
            self.process(item)

    def process(self, item):
        raise NotImplementedError("Should have subclassed the Mapper")


class Consumer(Worker):
    def __init__(self, name, in_queue):
        Worker.__init__(self, name)
        self.in_queue = in_queue

    def run(self):
        for item in queue_iterator(self.in_queue):
            self.process(item)

    def process(self, item):
        raise NotImplementedError("Should have subclassed the Consumer")
