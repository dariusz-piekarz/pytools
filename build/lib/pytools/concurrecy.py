from multiprocessing import Process, Lock as MLock, Manager, cpu_count
from concurrent.futures import ThreadPoolExecutor
from threading import Thread, Lock as TLock
from asyncio import gather, run
from pandas import DataFrame, read_csv
from io import BytesIO
from typing import Union
from aiofiles import open
from functools import wraps
from pytools.metaclass import time_decor


def div_to_subsets(set_to_partition: list[any], length_of_subset: int) -> list[list[any]]:
    """
    This function divides a set into subsets of a given length.

    Parameters
    ----------
    set_to_partition : list[any]
        The set to be divided into subsets.
    length_of_subset : int
        The length of each subset.

    Returns
    -------
    list[list[any]]
        A list of subsets.
    """
    return [set_to_partition[i: i + length_of_subset] for i in range(0, len(set_to_partition), length_of_subset)]


def locker(lock: bool = False, own_lock: bool = False) -> callable:
    """
    This function is a decorator that can be used to lock or unlock a function.

    Parameters
    ----------
    lock : bool, optional
        A boolean value that determines whether the function should be locked or not. If `True`,
        the function will be locked using a thread lock. If `False`, the function will not be locked.
    own_lock : bool, optional:
        A boolean value which specifies if user wants to use TLock in his own way. If `True`, the function will
        expect a kwargs variable named 'mutex'.
    Returns
    -------
    outer_wrapper : function
        A function that wraps the original function and applies the locking or unlocking behavior based on the
        `lock` parameter.

    Examples
    --------
    Here is an example of how to use the `locker` decorator:

    ```python
    @locker(lock=True)
    def my_function(args, kwargs):
        # This function will be locked using a thread lock
        pass

    @locker(lock=False)
    def my_function(args, kwargs):
        # This function will not be locked
        pass
    ```

    The `locker` decorator can be used to ensure that a function is executed in a thread-safe manner,
    preventing race conditions or other concurrency issues.
    """
    def outer_wrapper(function: callable) -> callable:
        @wraps(function)
        def inner_wrapper(*args, **kwargs) -> any:
            if not own_lock:
                del kwargs['mutex']

            if lock and not own_lock:
                with TLock():
                    return function(*args, **kwargs)
            else:
                return function(*args, **kwargs)
        return inner_wrapper
    return outer_wrapper


def concurrency_manager(engine: str = 'thread', lock: bool = False, own_lock: bool = False) -> callable:
    """
    A decorator that allows the user to execute a function in a concurrent manner.

    Parameters:
    engine (str): The type of concurrency engine to be used. Supported values are 'thread' and 'async'.
    lock (bool): A boolean value that determines whether the function should be locked or not.
    own_lock : bool, optional:
        A boolean value which specifies if user wants to use TLock in his own way. If `True`, the function will
        expect a kwargs variable named 'mutex'.
    Returns:
    callable: A function that wraps the original function and applies the locking or unlocking behavior based on the
     `lock` and 'own_lock' parameters.

    Examples:
    Here is an example of how to use the `concurrency_manager` decorator:

    ```python
    @concurrency_manager(engine='thread', lock=True)
    def my_function(args: dict[str, tuple[any]], kwargs: dict[str, dict[str, any]]) -> dict[str, any]:
        # Your function implementation here
        pass
    ```

    The `concurrency_manager` decorator can be used to ensure that a function is executed in a thread-safe manner,
    preventing race conditions or other concurrency issues.
    """
    def outer_wrap(function: callable) -> callable:

        @time_decor
        @wraps(function)
        def inner_wrap(args: Union[dict[str, tuple[any]], dict[str, dict[str, any]]],
                       kwargs: Union[dict[str, dict[str, any]], None] = None) -> dict[str, any]:
            if engine not in ['thread', 'async']:
                raise ValueError("Invalid engine type. Supported values are 'thread' and 'async'.")

            if args is None:
                raise ValueError("You must provide arguments!")

            if engine == 'thread':
                mutex = None
                if own_lock:
                    mutex = TLock()

                with ThreadPoolExecutor() as executor:
                    if kwargs is not None:
                        if args.keys() != kwargs.keys():
                            raise ValueError("args and kwargs must have the same keys!")

                        results = [executor.submit(locker(lock, own_lock)(function), *args[key], **kwargs[key],
                                                   mutex=mutex) for key in args.keys()]

                    elif isinstance(list(args.values())[0], tuple):
                        results = [executor.submit(locker(lock, own_lock)(function), *args[key], mutex=mutex)
                                   for key in args.keys()]

                    elif isinstance(list(args.values())[0], dict):
                        results = [executor.submit(locker(lock, own_lock)(function), **args[key], mutex=mutex)
                                   for key in args.keys()]
                return {key: result.result() for key, result in zip(args.keys(), results)}

            elif engine == 'async':
                async def async_inner_wrap() -> dict[str: any]:
                    coros = []
                    if kwargs is not None:
                        if args.keys() != kwargs.keys():
                            coros = [function(*args[key], **kwargs[key]) for key in args.keys()]

                    elif isinstance(list(args.values())[0], tuple):
                        coros = [function(*args[key]) for key in args.keys()]

                    elif isinstance(list(args.values())[0], dict):
                        coros = [function(**args[key]) for key in args.keys()]

                    results = await gather(*coros)
                    return {key: result for key, result in zip(args.keys(), results)}
                return run(async_inner_wrap())
        return inner_wrap
    return outer_wrap


class ConcurrencyManager:
    """
    A class to manage concurrent execution of functions.

    Attributes:
        function (callable): The function to be executed concurrently.
        engine (str): The type of concurrency engine to be used. Can be 'thread', 'process', or 'async'.
        mutex (Union[MLock, TLock, None]): A mutex object used to synchronize access to shared resources.
        shared_data (dict | None): A dictionary to store the results of the function execution.
        own_locker (bool): A boolean value which specifies if user wants to use TLock in his own way.
         If `True`, the function will expect a kwargs variable named 'mutex'.

    Methods:
        __init__(function: callable, engine: str = 'thread', locker: bool = True, own_locker: bool = False):
            Initialize the ConcurrencyManager instance with the provided function, engine type, and locker status.

        concurrency_manager(args: dict[str: tuple[any]] | dict[str: dict[str: any]],
                                  kwargs: dict[str: dict[str: any]] | None = None):
            Execute the function concurrently with the provided arguments and keywords.
            The results are stored in the shared_data dictionary.

        guard(args: tuple[any] | dict[str: any], kwargs: dict[str: any] | None, id: str) -> None:
            Execute the function with the provided arguments and keywords in a thread/process/event,
            and store the result in the shared_data dictionary.

        async_root(args: dict[str, tuple[any]] | None = None,
                         kwargs: dict[str, dict[str, any]] | None = None) -> None:
            Execute the function concurrently with the provided arguments and keywords asynchronously,
            and store the results in the shared_data dictionary.

        single_async(args: tuple[any] | dict[str: any], kwargs: dict[str: any] | None) -> Any:
            Execute the function with the provided arguments and keywords asynchronously, and return the result.
    """
    mutex: Union[MLock, TLock, None]
    shared_data: dict | None = None
    engine: str
    function: callable
    share_results: bool
    own_locker: bool

    def __init__(self, function: callable, engine: str = 'thread', locker: bool = True, own_locker: bool = False):

        self.function = function
        if engine in ['thread', 'process', 'async']:
            self.engine = engine
        else:
            raise ValueError(f'Engine {engine} is not supported.')

        self.own_locker = own_locker

        if locker or own_locker:
            if engine == 'thread':
                self.mutex = TLock()
            elif engine == 'process':
                self.mutex = MLock()
        else:
            self.mutex = None

    @time_decor
    def concurrency_manager(self, args: dict[str: tuple[any]] | dict[str: dict[str: any]],
                                  kwargs: dict[str: dict[str: any]] | None = None) -> dict[str: any]:

        if (args is not None and kwargs is None) or (
                isinstance(args, dict) and isinstance(kwargs, dict) and args.keys() == kwargs.keys()):

            if self.engine == 'thread':
                self.shared_data = {key: None for key in (args.keys() if args is not None else kwargs.keys())}

                threads = [Thread(target=self.guard, args=(args[key], kwargs[key] if kwargs is not None else None, key))
                           for key in args.keys()]

                for thread in threads:
                    thread.start()

                for thread in threads:
                    thread.join()

            elif self.engine == 'process':
                core_available = int(cpu_count()/2)
                partition = div_to_subsets(list(args.keys()), core_available)
                mgr = Manager()
                self.shared_data = mgr.dict({key: None for key in (args.keys() if args is not None else kwargs.keys())})
                for subset in partition:
                    processes = [Process(target=self.guard, args=(args[key], kwargs[key]
                                         if kwargs is not None else None, key)) for key in subset]

                    for process in processes:
                        process.start()

                    for process in processes:
                        process.join()

            elif self.engine == 'async':
                self.shared_data = {key: None for key in (args.keys() if args is not None else kwargs.keys())}
                run(self.async_root(args, kwargs))

        elif args is None and kwargs is None:
            raise ValueError("arguments must be provided!")

        elif ~isinstance(args, dict) or ~isinstance(kwargs, dict):
            raise TypeError("args and kwargs must be dictionaries which keys correspond to identifier of"
                            " threads/processes/events, and values are either tuples (arg1, arg2,...) or dictionaries"
                            " {function_variable_name: value} of arguments for single thread/process/event!")

        else:
            raise ValueError("args and kwargs must have the same keys!")

        return self.shared_data

    def guard(self, args: tuple[any] | dict[str: any], kwargs: dict[str: any] | None, id: str) -> None:

        if self.mutex is not None and not self.own_locker:
            self.mutex.acquire()

        if isinstance(args, tuple) and isinstance(kwargs, dict):
            if self.own_locker:
                self.shared_data[id] = self.function(*args, **kwargs, mutex=self.mutex)
            else:
                self.shared_data[id] = self.function(*args, **kwargs)
        else:
            if isinstance(args, tuple):
                if self.own_locker:
                    self.shared_data[id] = self.function(*args, mutex=self.mutex)
                else:
                    self.shared_data[id] = self.function(*args)
            else:
                if self.own_locker:
                    self.shared_data[id] = self.function(**args, mutex=self.mutex)
                else:
                    self.shared_data[id] = self.function(**args)

        if self.mutex is not None and not self.own_locker:
            self.mutex.release()

    async def async_root(self, args: dict[str, tuple[any]] | None = None,
                         kwargs: dict[str, dict[str, any]] | None = None) -> None:

        functions = [self.single_async(args[key] if args is not None else None,
                                              kwargs[key] if kwargs is not None else None)
                     for key in (args.keys() if args is not None else kwargs.keys())]

        results = await gather(*functions)
        keys = list(args.keys() if args is not None else kwargs.keys())
        self.shared_data = {keys[i]: results[i] for i in range(len(keys))}

    async def single_async(self, args: tuple[any] | dict[str: any], kwargs: dict[str: any] | None) -> any:
        if isinstance(args, tuple) and isinstance(kwargs, dict):
            return await self.function(*args, **kwargs)
        else:
            if isinstance(args, tuple):
                return await self.function(*args)
            else:
                return await self.function(**args)


class MultifunctionsConcurrencyManager:
    """
     A class to manage concurrent execution of functions.

     Attributes:
         functions dict[str: callable]: Functions to be executed concurrently.
         engine (str): The type of concurrency engine to be used. Can be 'thread', 'process', or 'async'.
         mutex (Union[MLock, TLock, None]): A mutex object used to synchronize access to shared resources.
         shared_data (dict | None): A dictionary to store the results of the function execution.
         own_locker (bool): A boolean value which specifies if user wants to use TLock in his own way.
                            If `True`, the function will expect a kwargs variable named 'mutex'.

     Methods:
         __init__(functions: dict[str: callable], engine: str = 'thread', locker: bool = True, own_locker: bool = False):
             Initialize the ConcurrencyManager instance with the provided functions, engine type, and locker status.

         concurrency_manager(args: dict[str: tuple[any]] | dict[str: dict[str: any]],
                                   kwargs: dict[str: dict[str: any]] | None = None):
             Execute the function concurrently with the provided arguments and keywords.
             The results are stored in the shared_data dictionary.

         guard(args: tuple[any] | dict[str: any], kwargs: dict[str: any] | None, id: str) -> None:
             Execute the function with the provided arguments and keywords in a thread/process/event,
             and store the result in the shared_data dictionary.

         async_root(args: dict[str, tuple[any]] | None = None,
                          kwargs: dict[str, dict[str, any]] | None = None) -> None:
             Execute the function concurrently with the provided arguments and keywords asynchronously,
             and store the results in the shared_data dictionary.

         single_async(function: callable, args: tuple[any] | dict[str: any], kwargs: dict[str: any] | None) -> Any:
             Execute the function with the provided arguments and keywords asynchronously, and return the result.
     """

    mutex: Union[MLock, TLock, None]
    shared_data: dict | None = None
    engine: str
    functions: dict[str: callable]
    share_results: bool
    own_locker: bool

    def __init__(self, functions: dict[str: callable], engine: str = 'thread', locker: bool = True,
                 own_locker: bool = False):

        if not all([callable(function) for function in functions.values()]) or not isinstance(functions, dict):
            raise TypeError("functions must be callables, and provided as dict!")
        self.functions = functions

        if engine in ['thread', 'process', 'async']:
            self.engine = engine
        else:
            raise ValueError(f'Engine {engine} is not supported.')

        self.own_locker = own_locker

        if locker or own_locker:
            if engine == 'thread':
                self.mutex = TLock()
            elif engine == 'process':
                self.mutex = MLock()
        else:
            self.mutex = None

    @time_decor
    def concurrency_manager(self, args: dict[str: tuple[any]] | dict[str: dict[str: any]],
                                  kwargs: dict[str: dict[str: any]] | None = None) -> dict[str: any]:

        if (args is not None and kwargs is None and args.keys() == self.functions.keys()) or (
                isinstance(args, dict) and isinstance(kwargs, dict) and args.keys() == kwargs.keys()
                                                                    and args.keys() == self.functions.keys()):

            if self.engine == 'thread':
                self.shared_data = {key: None for key in (args.keys() if args is not None else kwargs.keys())}

                threads = [Thread(target=self.guard, args=(args[key], kwargs[key] if kwargs is not None else None, key))
                           for key in args.keys()]

                for thread in threads:
                    thread.start()

                for thread in threads:
                    thread.join()

            elif self.engine == 'process':
                core_available = int(cpu_count()/2)
                partition = div_to_subsets(list(args.keys()), core_available)
                mgr = Manager()
                self.shared_data = mgr.dict({key: None for key in (args.keys() if args is not None else kwargs.keys())})
                for subset in partition:
                    processes = [Process(target=self.guard, args=(args[key], kwargs[key]
                                         if kwargs is not None else None, key)) for key in subset]

                    for process in processes:
                        process.start()

                    for process in processes:
                        process.join()

            elif self.engine == 'async':
                self.shared_data = {key: None for key in (args.keys() if args is not None else kwargs.keys())}
                run(self.async_root(args, kwargs))

        elif args is None and kwargs is None:
            raise ValueError("arguments must be provided!")

        elif ~isinstance(args, dict) or ~isinstance(kwargs, dict):
            raise TypeError("args and kwargs must be dictionaries which keys correspond to identifier of"
                            " threads/processes/events, and values are either tuples (arg1, arg2,...) or dictionaries"
                            " {function_variable_name: value} of arguments for single thread/process/event!")

        else:
            raise ValueError("args,kwargs and functions must have the same keys!")

        return self.shared_data

    def guard(self, args: tuple[any] | dict[str: any], kwargs: dict[str: any] | None, id: str) -> None:

        if self.mutex is not None and not self.own_locker:
            self.mutex.acquire()

        if isinstance(args, tuple) and isinstance(kwargs, dict):
            if self.own_locker:
                self.shared_data[id] = self.functions[id](*args, **kwargs, mutex=self.mutex)
            else:
                self.shared_data[id] = self.functions[id](*args, **kwargs)
        else:
            if isinstance(args, tuple):
                if self.own_locker:
                    self.shared_data[id] = self.functions[id](*args, mutex=self.mutex)
                else:
                    self.shared_data[id] = self.functions[id](*args)
            else:
                if self.own_locker:
                    self.shared_data[id] = self.functions[id](**args, mutex=self.mutex)
                else:
                    self.shared_data[id] = self.functions[id](**args)

        if self.mutex is not None and not self.own_locker:
            self.mutex.release()

    async def async_root(self, args: dict[str, tuple[any]] | None = None,
                               kwargs: dict[str, dict[str, any]] | None = None) -> None:

        functions = [self.single_async(self.functions[key], args[key] if args is not None else None,
                                                            kwargs[key] if kwargs is not None else None)
                     for key in (args.keys() if args is not None else kwargs.keys())]

        results = await gather(*functions)
        keys = list(args.keys() if args is not None else kwargs.keys())
        self.shared_data = {keys[i]: results[i] for i in range(len(keys))}

    async def single_async(self, function: callable, args: tuple[any] | dict[str: any],
                                                     kwargs: dict[str: any] | None) -> any:
        if isinstance(args, tuple) and isinstance(kwargs, dict):
            return await function(*args, **kwargs)
        else:
            if isinstance(args, tuple):
                return await function(*args)
            else:
                return await function(**args)


def read_csv_file(path: str, *args, **kwargs) -> DataFrame:
    return read_csv(path,  *args, **kwargs)


def save_to_csv(df: DataFrame, path: str,  *args, **kwargs) -> None:
    df.to_csv(path,  *args, **kwargs)


async def read_csv_file_async(path: str, *args, **kwargs) -> DataFrame:
    async with open(path, mode='rb') as file:
        content = await file.read()
        return read_csv(BytesIO(content),  *args, **kwargs)


async def save_to_csv_async(df: DataFrame, path: str, *args, **kwargs) -> None:

    bytes_buffer = BytesIO()
    df.to_csv(bytes_buffer, *args, **kwargs)
    bytes_data = bytes_buffer.getvalue()

    async with open(path, mode='wb') as file:
        await file.write(bytes_data)


def csv_read(engine: str, kwargs: dict[str: dict[str: any]]) -> dict[str: DataFrame]:
    if engine == 'thread':
        cm = ConcurrencyManager(read_csv_file, engine="thread")
        return cm.concurrency_manager(kwargs)
    elif engine == 'process':
        cm = ConcurrencyManager(read_csv_file, engine="process")
        return cm.concurrency_manager(kwargs)
    elif engine == 'async':
        cm = ConcurrencyManager(read_csv_file_async, engine="async")
        return cm.concurrency_manager(kwargs)
    else:
        raise ValueError("engine must be thread, process or async!")


def csv_write(engine: str, kwargs: dict[str: dict[str: any]]) -> None:
    if engine == 'thread':
        cm = ConcurrencyManager(save_to_csv, engine="thread", locker=False)
        return cm.concurrency_manager(kwargs)
    elif engine == 'process':
        cm = ConcurrencyManager(save_to_csv, engine="process", locker=False)
        return cm.concurrency_manager(kwargs)
    elif engine == 'async':
        cm = ConcurrencyManager(save_to_csv_async, engine="async", locker=False)
        return cm.concurrency_manager(kwargs)
    else:
        raise ValueError("engine must be thread, process or async!")


if __name__ == '__main__':
    pass
