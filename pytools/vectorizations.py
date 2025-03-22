import numpy as np
import pandas as pd
from functools import wraps
from loguru import logger
from typing import Hashable
from datetime import datetime, timedelta, time, date, timezone
import pyarrow as pa

ALL_ARGUMENTS: str = "All arguments should have the same shape."

BASE_TYPES: list[type] = [int, float, complex, str, bytes, bytearray, memoryview, bool]

NUMPY_TYPES: list[type] = [
    np.int8,
    np.int16,
    np.int32,
    np.int64,
    np.uint8,
    np.uint16,
    np.uint32,
    np.uint64,
    np.float16,
    np.float32,
    np.float64,
    np.complex64,
    np.complex128,
    np.bool_,
    np.str_,
    np.bytes_,
]

PANDAS_TYPES: list[type] = [
    pd.Int8Dtype,
    pd.Int16Dtype,
    pd.Int32Dtype,
    pd.Int64Dtype,
    pd.UInt8Dtype,
    pd.UInt16Dtype,
    pd.UInt32Dtype,
    pd.UInt64Dtype,
    pd.Float32Dtype,
    pd.Float64Dtype,
    pd.BooleanDtype,
    pd.StringDtype,
]

DATETIME_TYPES: list[type] = [
    datetime,
    timedelta,
    time,
    date,
    timezone,
    np.datetime64,
    pd.Timestamp,
    pd.Timedelta,
    pa.TimestampType,
]

TYPES: tuple[type, ...] = tuple(BASE_TYPES + NUMPY_TYPES + PANDAS_TYPES + DATETIME_TYPES)
CONTAINER_TYPES: tuple[type, ...] = (
    list,
    set,
    tuple,
    dict,
    np.ndarray,
    pd.DataFrame,
    pd.Series,
)


def all_types_same(obj: any, types: tuple[type, ...]) -> bool:
    return all([isinstance(el, types) for el in obj])


def argument_consistency(objects: tuple[any, ...], cont_types: tuple[type, ...]) -> bool:
    return any([all([isinstance(obj, typ) for obj in objects]) for typ in cont_types])


def len_consistency(objects: any, ctype: str = "list") -> bool:
    if ctype in ("tuple", "list", "str"):
        return all([len(obj) == len(objects[0]) for obj in objects])
    elif ctype == "dict":
        return all([len(obj.keys()) == len(objects[0].keys()) for obj in objects])
    elif ctype in ("pd.Series", "pd.DataFrame"):
        return all([obj.shape == objects[0].shape for obj in objects])
    elif ctype == "np.ndarray":
        return all([np.shape(obj) == np.shape(objects[0]) for obj in objects])
    else:
        return False


def get_section(
    objects: tuple[any, ...], i: int, ctype: str = "list"
) -> tuple[any, ...] | tuple[list[Hashable], tuple[any, ...]]:
    if ctype == "list":
        return tuple([obj[i] for obj in objects])
    elif ctype == "dict":
        key_el: list[Hashable] = [list(obj.keys())[i] for obj in objects]
        val_el: tuple[any, ...] = tuple([list(obj.values())[i] for obj in objects])
        return key_el, val_el

    else:
        return tuple()


def dtype_consistency_in_container(container: tuple[any, ...], base_dtypes: tuple[type, ...] = TYPES) -> bool:
    return all([isinstance(element, base_dtypes) for element in container])


def vectorize(fun: callable) -> callable:
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

        if isinstance(obj, TYPES):
            return fun(obj)

        if not isinstance(obj, CONTAINER_TYPES):
            raise (TypeError(f"{obj} is not a data container."))

        if isinstance(obj, list):
            if all_types_same(obj, TYPES):
                return [fun(el) for el in obj]
            else:
                return [fun(el) if isinstance(el, TYPES) else to_vectorize(el) for el in obj]

        elif isinstance(obj, tuple):
            return tuple(to_vectorize(list(obj)))

        elif isinstance(obj, set):
            return set(to_vectorize(list(obj)))

        elif isinstance(obj, dict):
            return {key: (fun(val) if isinstance(obj, dict) else to_vectorize(val)) for key, val in obj.items()}

        elif isinstance(obj, np.ndarray):
            return fun(obj)

        elif isinstance(obj, pd.DataFrame):
            return pd.DataFrame(to_vectorize(obj.to_dict(orient="list")))

    return to_vectorize


def vectorize_param(fun: callable) -> callable:
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
    def to_vectorize(obj, **params: object):

        if isinstance(obj, TYPES):
            return fun(obj, **params)

        if not isinstance(obj, CONTAINER_TYPES):
            raise (TypeError(f"{obj} is not a data container."))

        if isinstance(obj, list):
            if all_types_same(obj, TYPES):
                return [fun(el, **params) for el in obj]
            else:
                return [fun(el, **params) if isinstance(el, TYPES) else to_vectorize(el) for el in obj]

        elif isinstance(obj, tuple):
            return tuple(to_vectorize(list(obj)))

        elif isinstance(obj, set):
            return set(to_vectorize(list(obj)))

        elif isinstance(obj, dict):
            return {key: (fun(val, **params) if isinstance(obj, dict) else to_vectorize(val)) for key, val in obj.items()}

        elif isinstance(obj, np.ndarray):
            return fun(obj, **params)

        elif isinstance(obj, pd.DataFrame):
            return pd.DataFrame(to_vectorize(obj.to_dict(orient="list")))

    return to_vectorize


def vectorize_multi(fun: callable) -> callable:
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
    def to_vectorize(*objects: object):

        if all_types_same(objects, TYPES):
            return fun(*objects)

        if not argument_consistency(objects, CONTAINER_TYPES):
            raise (TypeError(f"Input objects should have the same type. Allowed types are {CONTAINER_TYPES}."))

        if isinstance(objects[0], list):
            if not len_consistency(objects):
                raise (TypeError(ALL_ARGUMENTS))

            ret: list[any] = []
            for i in range(len(objects[0])):
                el = get_section(objects, i)
                if all_types_same(el, TYPES):
                    ret.append(fun(*el))
                else:
                    ret.append(to_vectorize(*el))
            return ret

        elif isinstance(objects[0], tuple):
            if not len_consistency(objects):
                raise (TypeError(ALL_ARGUMENTS))

            new_objects: tuple[any, ...] = tuple([list(obj) for obj in objects])
            return tuple(to_vectorize(*new_objects))

        elif isinstance(objects[0], set):
            if not len_consistency(objects):
                raise (TypeError(ALL_ARGUMENTS))

            new_objects: tuple[any, ...] = tuple([list(obj) for obj in objects])
            return set(to_vectorize(*new_objects))

        elif isinstance(objects[0], dict):
            if not all([len(obj.keys()) == len(objects[0].keys()) for obj in objects]):
                raise (TypeError(ALL_ARGUMENTS))

            ret: dict[Hashable, any] = dict()
            for i in range(len(objects[0].keys())):
                key_el: list[Hashable] = [list(obj.keys())[i] for obj in objects]
                val_el: tuple[any, ...] = tuple([list(obj.values())[i] for obj in objects])
                if all([isinstance(element, TYPES) for element in val_el]):
                    ret[f"{fun.__name__}{key_el}"] = fun(*val_el)
                else:
                    ret[f"{fun.__name__}{key_el}"] = to_vectorize(*val_el)
            return ret

        elif isinstance(objects[0], np.ndarray):
            if not all([np.shape(obj) == np.shape(objects[0]) for obj in objects]):
                raise (TypeError(ALL_ARGUMENTS))

            return fun(*objects)

        elif isinstance(objects[0], pd.DataFrame):
            if not all([obj.shape == objects[0].shape for obj in objects]):
                raise (TypeError(ALL_ARGUMENTS))

            new_objects: tuple[any, ...] = tuple([obj.to_dict(orient="list") for obj in objects])
            return pd.DataFrame(to_vectorize(*new_objects))

        elif isinstance(objects[0], pd.Series):
            if not all([obj.shape == objects[0].shape for obj in objects]):
                raise (TypeError(ALL_ARGUMENTS))

            new_objects: tuple[any, ...] = tuple([obj.to_dict() for obj in objects])
            return pd.Series(to_vectorize(*new_objects))

    return to_vectorize


def vectorize_multi_param(fun: callable) -> callable:
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
    def to_vectorize(*objects: object, **params: object):

        if not all([isinstance(param, TYPES) for param in params.values()]):
            raise TypeError(f"Parameters should be of type: {CONTAINER_TYPES}")

        if all([isinstance(obj, TYPES) for obj in objects]):
            return fun(*objects, **params)

        if not any([all([isinstance(obj, typ) for obj in objects]) for typ in CONTAINER_TYPES]):
            raise TypeError(f"Input objects should have the same type. Allowed types are {CONTAINER_TYPES}.")

        if isinstance(objects[0], list):
            if not len_consistency(objects, "list"):
                raise TypeError(ALL_ARGUMENTS)

            ret: list[any] = []
            for i in range(len(objects[0])):
                el: tuple[any, ...] = get_section(objects, i, "list")
                if dtype_consistency_in_container(el, TYPES):
                    ret.append(fun(*el, **params))
                else:
                    ret.append(to_vectorize(*el, **params))
            return ret

        elif isinstance(objects[0], tuple):
            if not len_consistency(objects, "tuple"):
                raise TypeError(ALL_ARGUMENTS)

            new_objects: tuple[any, ...] = tuple([list(obj) for obj in objects])
            return tuple(to_vectorize(*new_objects, **params))

        elif isinstance(objects[0], set):
            if not len_consistency(objects, "set"):
                raise TypeError(ALL_ARGUMENTS)

            new_objects: tuple[any, ...] = tuple([list(obj) for obj in objects])
            return set(to_vectorize(*new_objects, **params))

        elif isinstance(objects[0], dict):
            if not len_consistency(objects, "dict"):
                raise TypeError(ALL_ARGUMENTS)

            ret: dict = dict()

            for i in range(len(objects[0].keys())):
                key_el: list[Hashable]
                val_el: tuple[any, ...]
                key_el, val_el = get_section(objects, i, "dict")

                if dtype_consistency_in_container(val_el, TYPES):
                    ret[f"{fun.__name__}{key_el}"] = fun(*val_el, **params)
                else:
                    ret[f"{fun.__name__}{key_el}"] = to_vectorize(*val_el, **params)
            return ret

        elif isinstance(objects[0], np.ndarray):
            if not len_consistency(objects, "np.ndarray"):
                raise TypeError(ALL_ARGUMENTS)

            return fun(*objects, **params)

        elif isinstance(objects[0], pd.DataFrame):
            if not len_consistency(objects, "pd.DataFrame"):
                raise TypeError(ALL_ARGUMENTS)

            new_objects: tuple[any, ...] = tuple([obj.to_dict(orient="list") for obj in objects])
            return pd.DataFrame(to_vectorize(*new_objects, **params))

        elif isinstance(objects[0], pd.Series):
            if not len_consistency(objects, "pd.Series"):
                raise TypeError(ALL_ARGUMENTS)

            new_objects: tuple[any, ...] = tuple([obj.to_dict() for obj in objects])
            return pd.Series(to_vectorize(*new_objects, **params))

    return to_vectorize
