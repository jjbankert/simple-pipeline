import multiprocessing
import workers
from termcolor import colored


class Task:
    def __init__(self, worker, amount):
        self.worker = worker
        self.name = worker.__name__
        self.amount = amount


class Pipeline:
    def __init__(self, tasks_and_queue_sizes):
        self.tasks = []
        self.queue_sizes = []

        self.expecting_task = True
        self.has_producer = False
        self.has_consumer = False
        for item in tasks_and_queue_sizes:
            if self.has_consumer:
                raise AssertionError("Can't add to the Pipeline beyond the Consumer")

            if self.expecting_task:
                self.evaluate_task(item)
            else:
                if type(item) is int:
                    self.queue_sizes.append(item)
                    self.expecting_task = True
                else:
                    self.queue_sizes.append(-1)
                    self.evaluate_task(item)

        if not self.has_consumer:
            raise AssertionError("Pipeline must end with a Consumer")

        assert len(self.tasks) == len(self.queue_sizes) + 1
        assert len(self.tasks) > 1

    def evaluate_task(self, item):
        if type(item) is not Task:
            raise AssertionError("Expecting a Task but got: {} ({})".format(str(item), str(type(item))))

        if not issubclass(item.worker, workers.Worker):
            raise AssertionError("Expecting a Worker but got: {} ({})".format(str(item), str(type(item))))

        if not self.has_producer and not issubclass(item.worker, workers.Producer):
            raise AssertionError("Pipeline must start with a Producer")

        if self.has_producer and issubclass(item.worker, workers.Producer):
            raise AssertionError("Producer can only be at the start of the Pipeline")

        self.tasks.append(item)
        self.expecting_task = False

        if issubclass(item.worker, workers.Producer):
            self.has_producer = True

        if issubclass(item.worker, workers.Consumer):
            self.has_consumer = True

    def execute(self):
        stages = []
        queues = []

        for queue_size in self.queue_sizes:
            queues.append(multiprocessing.Queue(queue_size))

        for task_idx, task in enumerate(self.tasks):
            task_workers = []
            for worker_idx in range(task.amount):
                name = '{}-{}'.format(task.name, worker_idx)

                if issubclass(task.worker, workers.Producer):
                    worker = task.worker(name, queues[task_idx])
                elif issubclass(task.worker, workers.Mapper):
                    worker = task.worker(name, queues[task_idx - 1], queues[task_idx])
                elif issubclass(task.worker, workers.Consumer):
                    worker = task.worker(name, queues[task_idx - 1])

                worker.start()
                task_workers.append(worker)

            stages.append(task_workers)

        for idx in range(len(queues)):
            self.join_processes(stages[idx])

            queue = queues[idx]
            for _ in range(len(stages[idx + 1])):
                queue.put(None)

            queue.close()
            queue.join_thread()
            # print(colored('joined queue {}->{}'.format(self.tasks[idx].name, self.tasks[idx + 1].name), 'blue'))

        self.join_processes(stages[-1])

    @staticmethod
    def join_processes(processes):
        for process in processes:
            process.join()
            # print(colored('joined {}'.format(process.name), 'blue'))
