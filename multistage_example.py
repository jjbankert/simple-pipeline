import pipeline
import workers
from hello_pipeline import HelloWorldProducer, PrintConsumer


def multistage_example():
    pipeline.Pipeline([
        pipeline.Task(HelloWorldProducer, 1),
        pipeline.Task(ReverserMapper, 1),
        pipeline.Task(PrintConsumer, 1)
    ]).execute()


class ReverserMapper(workers.Mapper):
    def process(self, item):
        self.out_queue.put('{}: {}'.format(self.name, str(item)[::-1]))


if __name__ == '__main__':
    multistage_example()
