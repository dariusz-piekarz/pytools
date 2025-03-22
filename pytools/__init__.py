from .stringcolor import Color
from typing import TypeVar
from typing import Callable, Any

__version__ = "0.0.1"

__all__ = ["color", "Functional", "Functorial"]

color = Color()

Functional = TypeVar("Functional", bound=Callable[..., Any])
Functorial = TypeVar("Functorial", bound=Callable[[Callable[..., Any]], Callable[..., Any]])
