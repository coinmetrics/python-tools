import time
import threading
from typing import Callable, Union, Tuple, List, Any


def executeWithRetries(proc: Callable, args: Union[Tuple, List], maxRetries: int, sleepTime: float=0):
    retries = 0
    while True:
        try:
            return proc(*args)
        except KeyboardInterrupt:
            raise
        except Exception as e:
            if retries >= maxRetries:
                raise e
            else:
                retries += 1
                if sleepTime > 0:
                    time.sleep(sleepTime)


def executeInParallel(procs: List[Callable], args: List[Union[Tuple, List]]) -> List[Tuple[Any, Exception]]:
    def threadWrap(index, proc, args):
        try:
            partialResult = proc(*args)
            exception = None
        except Exception as e:
            partialResult = None
            exception = e
        result[index] = (partialResult, exception)

    assert len(procs) == len(args)
    result = []
    threads = []
    for index in range(len(procs)):
        result.append(None)
        t = threading.Thread(target=threadWrap, args=(index, procs[index], args[index]))
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    return result


def executeInParallelSameProc(proc: Callable, args: List[Union[Tuple, List]]) -> List[Tuple[Any, Exception]]:
    return executeInParallel([proc for _ in range(len(args))], args)
