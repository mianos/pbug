from typing import Any, Callable

from concurrent.futures import ThreadPoolExecutor, as_completed

from prefect.engine.executors.base import Executor


class CFExecutor(Executor):
    def __init__(self):
        self.pool = ThreadPoolExecutor(3)

    def submit(self, fn: Callable, *args: Any, extra_context: dict = None, **kwargs: Any) -> Any:
        return self.pool.submit(fn, *args, **kwargs)

    def wait(self, futures: Any) -> Any:
        print(f"type {type(futures)}")
        if type(futures) == dict:
            from ipdb import set_trace
            set_trace()
            aa = [kk for kk in as_completed([ii for ii in futures.values()])]
            return aa
        return next(as_completed((futures,))).result()
