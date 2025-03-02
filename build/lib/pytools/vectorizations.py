import numpy as np
import pandas as pd
from functools import wraps
from loguru import logger
from datetime import datetime


def vectorize(fun):
    """
    This decorator takes a function of one variable and returns a new function that applies
    the original function to all elements of a data structure that are of a basic data type
    (int, float, str, etc.).
    The new function also handles nested data structures such as
    lists, tuples, sets, dictionaries, and NumPy arrays.
    :param fun: a function of one variable to be vectorized
    :return: a function that applies the original function to all elements of a data structure that are of a basic data type
    """
    @logger.catch
    @wraps(fun)
    def to_vectorize(obj):
        types = (int, np.int64, np.int32, float, np.float32, np.float32, complex, np.complex128, str)

        if isinstance(obj, types):
            return fun(obj)

        if not isinstance(obj, (list, set, tuple, dict, np.ndarray, pd.DataFrame)):
            raise (TypeError(f"{obj} is not a data container."))

        if isinstance(obj, list):
            if all([isinstance(el, types) for el in obj]):
                return [fun(el) for el in obj]
            else:
                ret = []
                for el in obj:
                    if isinstance(el, types):
                        ret.append(fun(el))
                    else:
                        ret.append(to_vectorize(el))
                return ret

        elif isinstance(obj, tuple):
            return tuple(to_vectorize(list(obj)))

        elif isinstance(obj, set):
            return set(to_vectorize(list(obj)))

        elif isinstance(obj, dict):
            for key, val in obj.items():
                if isinstance(obj, dict):
                    obj[key] = fun(val)
                else:
                    obj[key] = to_vectorize(val)
            return obj

        elif isinstance(obj, np.ndarray):
            return fun(obj)

        elif isinstance(obj, pd.DataFrame):
            return pd.DataFrame(to_vectorize(obj.to_dict(orient='list')))

    return to_vectorize


def vectorize_param(fun):
    """
    This decorator takes a function of one variable and returns a new function that applies
    the original function to all elements of a data structure that are of a basic data type
    (int, float, str, etc.). The function allows to have uniform parameters (**params).
    The new function also handles nested data structures such as
    lists, tuples, sets, dictionaries, and NumPy arrays.
    :param fun: a function of one variable (+ **params parameters) to be vectorized
    :return: a function that applies the original function to all elements of a data structure that are of a basic data type
    """
    @logger.catch
    @wraps(fun)
    def to_vectorize(obj, **params):
        types = (int, np.int64, np.int32, float, np.float32, np.float32, complex, np.complex128, str)

        if isinstance(obj, types):
            return fun(obj, **params)

        if not isinstance(obj, (list, set, tuple, dict, np.ndarray, pd.DataFrame)):
            raise (TypeError(f"{obj} is not a data container."))

        if isinstance(obj, list):
            if all([isinstance(el, types) for el in obj]):
                return [fun(el, **params) for el in obj]
            else:
                ret = []
                for el in obj:
                    if isinstance(el, types):
                        ret.append(fun(el, **params))
                    else:
                        ret.append(to_vectorize(el))
                return ret

        elif isinstance(obj, tuple):
            return tuple(to_vectorize(list(obj)))

        elif isinstance(obj, set):
            return set(to_vectorize(list(obj)))

        elif isinstance(obj, dict):
            for key, val in obj.items():
                if isinstance(obj, dict):
                    obj[key] = fun(val, **params)
                else:
                    obj[key] = to_vectorize(val)
            return obj

        elif isinstance(obj, np.ndarray):
            return fun(obj, **params)

        elif isinstance(obj, pd.DataFrame):
            return pd.DataFrame(to_vectorize(obj.to_dict(orient='list')))

    return to_vectorize


def vectorize_multi(fun):
    """
    This decorator takes a function of *objects variables and returns a new function that applies
    the original function to all elements of a data structure that are of a basic data type
    (int, float, str, etc.).
    The new function also handles nested data structures such as
    lists, tuples, sets, dictionaries, and NumPy arrays.
    :param fun: a function of *objects variables to be vectorized
    :return: a function that applies the original function to all elements of a data structure that are of a basic data type
    """
    @logger.catch
    @wraps(fun)
    def to_vectorize(*objects):
        types = (int, np.int64, np.int32, float, np.float32, np.float32, complex, np.complex128, str)
        container_types = (list, set, tuple, dict, np.ndarray, pd.DataFrame, pd.Series)

        if all([isinstance(obj, types) for obj in objects]):
            return fun(*objects)

        if not any([all([isinstance(obj, typ) for obj in objects]) for typ in container_types]):
            raise (TypeError(f"Input objects should have the same type. Allowed types are {container_types}."))

        if isinstance(objects[0], list):
            if not all([len(obj) == len(objects[0]) for obj in objects]):
                raise (TypeError("All arguments should have the same shape."))

            ret = []
            for i in range(len(objects[0])):
                el = tuple([obj[i] for obj in objects])
                if all([isinstance(element, types) for element in el]):
                    ret.append(fun(*el))
                else:
                    ret.append(to_vectorize(*el))
            return ret

        elif isinstance(objects[0], tuple):
            if not all([len(obj) == len(objects[0]) for obj in objects]):
                raise (TypeError("All arguments should have the same shape."))

            new_objects = tuple([list(obj) for obj in objects])
            return tuple(to_vectorize(*new_objects))

        elif isinstance(objects[0], set):
            if not all([len(obj) == len(objects[0]) for obj in objects]):
                raise (TypeError("All arguments should have the same shape."))

            new_objects = tuple([list(obj) for obj in objects])
            return set(to_vectorize(*new_objects))

        elif isinstance(objects[0], dict):
            if not all([len(obj.keys()) == len(objects[0].keys()) for obj in objects]):
                raise (TypeError("All arguments should have the same shape."))

            ret = dict()
            for i in range(len(objects[0].keys())):
                key_el = [list(obj.keys())[i] for obj in objects]
                val_el = tuple([list(obj.values())[i] for obj in objects])
                if all([isinstance(element, types) for element in val_el]):
                    ret[f"{fun.__name__}{key_el}"] = fun(*val_el)
                else:
                    ret[f"{fun.__name__}{key_el}"] = to_vectorize(*val_el)
            return ret

        elif isinstance(objects[0], np.ndarray):
            if not all([np.shape(obj) == np.shape(objects[0]) for obj in objects]):
                raise (TypeError("All arguments should have the same shape."))

            return fun(*objects)

        elif isinstance(objects[0], pd.DataFrame):
            if not all([obj.shape == objects[0].shape for obj in objects]):
                raise (TypeError("All arguments should have the same shape."))

            new_objects = tuple([obj.to_dict(orient='list') for obj in objects])
            return pd.DataFrame(to_vectorize(*new_objects))

        elif isinstance(objects[0], pd.Series):
            if not all([obj.shape == objects[0].shape for obj in objects]):
                raise (TypeError("All arguments should have the same shape."))

            new_objects = tuple([obj.to_dict() for obj in objects])
            return pd.Series(to_vectorize(*new_objects))
    return to_vectorize


def vectorize_multi_param(fun):
    """
     This decorator takes a function of *objects variable and returns a new function that applies
     the original function to all elements of a data structure that are of a basic data type
     (int, float, str, etc.). The function allows to have uniform parameters (**params).
     The new function also handles nested data structures such as
     lists, tuples, sets, dictionaries, and NumPy arrays.
     :param fun: a function of *objects variables (+ **params parameters) to be vectorized
     :return: a function that applies the original function to all elements of a data structure that are of a basic data type
     """
    @logger.catch
    @wraps(fun)
    def to_vectorize(*objects, **params):
        types = (int, np.int64, np.int32, float, np.float32, complex, np.complex128, str, datetime)
        container_types = (list, set, tuple, dict, np.ndarray, pd.DataFrame, pd.Series)

        if not all([isinstance(param, types) for param in params.values()]):
            raise TypeError(f"Parameters should be of type: {container_types}")

        if all([isinstance(obj, types) for obj in objects]):
            return fun(*objects, **params)

        if not any([all([isinstance(obj, typ) for obj in objects]) for typ in container_types]):
            raise TypeError(f"Input objects should have the same type. Allowed types are {container_types}.")

        if isinstance(objects[0], list):
            if not all([len(obj) == len(objects[0]) for obj in objects]):
                raise TypeError("All arguments should have the same shape.")

            ret = []
            for i in range(len(objects[0])):
                el = tuple([obj[i] for obj in objects])
                if all([isinstance(element, types) for element in el]):
                    ret.append(fun(*el, **params))
                else:
                    ret.append(to_vectorize(*el, **params))
            return ret

        elif isinstance(objects[0], tuple):
            if not all([len(obj) == len(objects[0]) for obj in objects]):
                raise TypeError("All arguments should have the same shape.")

            new_objects = tuple([list(obj) for obj in objects])
            return tuple(to_vectorize(*new_objects, **params))

        elif isinstance(objects[0], set):
            if not all([len(obj) == len(objects[0]) for obj in objects]):
                raise TypeError("All arguments should have the same shape.")

            new_objects = tuple([list(obj) for obj in objects])
            return set(to_vectorize(*new_objects, **params))

        elif isinstance(objects[0], dict):
            if not all([len(obj.keys()) == len(objects[0].keys()) for obj in objects]):
                raise TypeError("All arguments should have the same shape.")

            ret = dict()
            for i in range(len(objects[0].keys())):
                key_el = [list(obj.keys())[i] for obj in objects]
                val_el = tuple([list(obj.values())[i] for obj in objects])
                if all([isinstance(element, types) for element in val_el]):
                    ret[f"{fun.__name__}{key_el}"] = fun(*val_el, **params)
                else:
                    ret[f"{fun.__name__}{key_el}"] = to_vectorize(*val_el, **params)
            return ret

        elif isinstance(objects[0], np.ndarray):
            if not all([np.shape(obj) == np.shape(objects[0]) for obj in objects]):
                raise TypeError("All arguments should have the same shape.")

            return fun(*objects, **params)

        elif isinstance(objects[0], pd.DataFrame):
            if not all([obj.shape == objects[0].shape for obj in objects]):
                raise TypeError("All arguments should have the same shape.")

            new_objects = tuple([obj.to_dict(orient='list') for obj in objects])
            return pd.DataFrame(to_vectorize(*new_objects, **params))

        elif isinstance(objects[0], pd.Series):
            if not all([obj.shape == objects[0].shape for obj in objects]):
                raise TypeError("All arguments should have the same shape.")

            new_objects = tuple([obj.to_dict() for obj in objects])
            return pd.Series(to_vectorize(*new_objects, **params))

    return to_vectorize
