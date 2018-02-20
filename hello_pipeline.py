import pipeline
import workers


def hello_pipeline():
    pipeline.Pipeline([
        pipeline.Task(HelloWorldProducer, 1),
        pipeline.Task(PrintConsumer, 1)
    ]).execute()


class HelloWorldProducer(workers.Producer):
    def process(self):
        self.out_queue.put('{}: Hello World'.format(self.name))


class PrintConsumer(workers.Consumer):
    def process(self, item):
        print("{}: '{}'".format(self.name, str(item)))


if __name__ == '__main__':
    hello_pipeline()
