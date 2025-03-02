from functools import wraps
from datetime import datetime
from loguru import logger
from types import ModuleType
from typing import Optional
from inspect import getmodule
from pytools.stringcolor import color


def documentation(doc: str) -> callable:
    """
    This function equip a function with its documentation provided as a string.
        
        Parameters
        ----------
        doc: str
            The function documentation.

        Returns
        -------
        function
            The wrapped function.
    """
    if not isinstance(doc, str):
        raise TypeError(f"'doc' should be a string, got {type(doc)}!")
        
    def outer(fun: callable) -> callable:
        if not callable(fun):
            raise TypeError(f"'fun' should be callable, got {type(fun)}!")
        
        @wraps(fun)
        def inner(*args, **kwargs) -> any:
            fun.__doc__ = doc
            return fun(*args, **kwargs)
        return inner
    return outer


def name_decor(fun: callable) -> callable:
    """
        This function wraps a function and logs the full name of called function.

        Parameters
        ----------
        fun : function
            The function to be wrapped.

        Returns
        -------
        function
            The wrapped function.

        """
    if not callable(fun):
        raise TypeError(f"'fun' should be callable, got {type(fun)}!")
                
    @wraps(fun)
    def wrapper(*args, **kwargs) -> any:
        logger.info(fun.__qualname__)
        return fun(*args, **kwargs)
    return wrapper


def time_decor(fun: callable) -> callable:
    """
        This function wraps a function and logs the time it took to execute.

        Parameters
        ----------
        fun : function
            The function to be wrapped.

        Returns
        -------
        function
            The wrapped function.

        """
    if not callable(fun):
        raise TypeError(f"'fun' should be callable, got {type(fun)}!")
        
    @wraps(fun)
    def wrapper(*args, **kwargs) -> any:

        t0: datetime.time = datetime.now()
        res: any = fun(*args, **kwargs)
        t1: datetime.time = datetime.now()
        module: Optional[ModuleType] = getmodule(fun)
        module_path: str = f"`{module.__name__}.{fun.__qualname__}()`" if module else f"`<Unknown>.{fun.__qualname__}()`"
        logger.info(f"Function {color.color(module_path, 'red')} time execution: {color.color(t1-t0, 'yellow')}.")
        return res
    return wrapper


class NameMetaclass(type):
    """
    Metaclass for classes that can be used as a decorator. This metaclass is used
    to decorate the functions that are going to be called. The decorator prints logs with the name
    of executed function.
    """
    def __new__(cls, clsnames: str, clsbases: tuple[type, ...], clsdict: dict[str, any], *args, **kwargs) -> callable:
        """
        This method is called when a new class is being created.

        Parameters
        ----------
        cls : type
            The class that is being created.
        clsnames : tuple
            The tuple of class names.
        clsbases : tuple
            The tuple of base classes.
        clsdict : dict
            The dictionary containing the class's namespace.
        *args : tuple
            Additional positional arguments.
        **kwargs : dict
            Additional keyword arguments.

        Returns
        -------
        type
            The new class.

        """
        new_cls_dict: dict[str, any] = clsdict
        for key, val in clsdict.items():
            if callable(val):
                new_cls_dict[key] = name_decor(val)
        return super(type, cls).__new__(cls, clsnames, clsbases, new_cls_dict)


class TimeMetaclass(type):
    """
    Metaclass for classes that can be used as a decorator. This metaclass is used
    to decorate the functions that are going to be called.
    The decorator prints time of execution of the called method
    """
    def __new__(cls, clsnames: str, clsbases: tuple[type, ...], clsdict: dict[str, any]) -> callable:
        new_cls_dict: dict[str, any] = clsdict
        for key, val in clsdict.items():
            if callable(val):
                new_cls_dict[key] = time_decor(val)
        return super(type, cls).__new__(cls, clsnames, clsbases, new_cls_dict)


class LoguruMetaclass(type):
    """
    Metaclass for classes that can be used as a decorator. This metaclass is used
    to decorate the functions that are going to be called. The decorator prints logs from the loguru module in case
    of an exception, warnings or errors.
    """
    def __new__(cls, clsnames: str, clsbases: tuple[type, ...], clsdict: dict[str, any]) -> callable:
        """
        This method is called when a new class is being created.

        Parameters
        ----------
        cls : type
            The class that is being created.
        clsnames : tuple
            The tuple of class names.
        clsbases : tuple
            The tuple of base classes.
        clsdict : dict
            The dictionary containing the class's namespace.

        Returns
        -------
        type
            The new class.

        """
        new_cls_dict: dict[str, any] = clsdict
        for key, val in clsdict.items():
            if callable(val):
                new_cls_dict[key] = logger.catch(val)
        return super(type, cls).__new__(cls, clsnames, clsbases, new_cls_dict)


class CallBlockerMetaclass(type):
    """
        Metaclass for classes that can have only one instance.

        This metaclass overrides the __call__ method to check if there exists an instance of the class and if not, create it.
    """
    counter: int = 0

    def __new__(cls, clsnames: str, clsbases: tuple[type, ...], clsdict: dict[str, any]) -> callable:
        return super(CallBlockerMetaclass, cls).__new__(cls, clsnames, clsbases, clsdict)

    def __call__(cls, *args, **kwargs) -> callable:
        """
        Call the object instance.

        This method is called when an instance of the class is called as a function.
        It checks if there exists an instance of the class and if not, create it.

        Parameters
        ----------
        args : tuple
            Positional arguments passed to the instance when called as a function.
        kwargs : dict
            Keyword arguments passed to the instance when called as a function.

        Returns
        -------
        object
            The instance of the class.

        """

        if CallBlockerMetaclass.counter in [1, 2]:
            CallBlockerMetaclass.counter = 2
        elif CallBlockerMetaclass.counter == 0:
            CallBlockerMetaclass.counter = 1
            return super(CallBlockerMetaclass, cls).__call__(*args, **kwargs)
