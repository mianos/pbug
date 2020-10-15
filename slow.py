from time import sleep
import argparse

from prefect import Flow, Parameter, unmapped, task, context
from prefect.engine.executors import LocalDaskExecutor


@task(timeout=30)
def slow_task(opts, item, scripts):
    logger = context.get('logger')
    logger.info(f"==== IN TASK {item} Sleeping {opts.sleep_time}")
    sleep(opts.sleep_time)
    logger.info(f"## Awake {item}")
    return item


@task
def produce_range(opts):
    return range(opts.range)


with Flow("PS Version") as flow:
    scripts = Parameter('scripts')
    opts = Parameter('opts')

    nrange = produce_range(opts)
    results = slow_task.map(item=nrange,
                            scripts=unmapped(scripts),
                            opts=unmapped(opts))


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='test pywinrm')
    parser.add_argument('--workers', type=int, default=10)
    parser.add_argument('--sleep_time', type=int, default=2)
    parser.add_argument('--range', type=int, default=10)

    opts = parser.parse_args()

    executor = LocalDaskExecutor(num_workers=opts.workers)
    state = flow.run(executor=executor,
                     scripts="hello",
                     opts=opts)
    for ii in state.result[results].result:
        print(ii)
