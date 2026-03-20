"""Parameter validation utilities."""

from __future__ import annotations

import difflib
import functools
import inspect
from typing import Any, Callable, TypeVar

F = TypeVar("F", bound=Callable[..., Any])


def validate_params(func: F) -> F:
    """Validate kwargs and suggest closest parameter name on typo."""
    valid = set(inspect.signature(func).parameters)

    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        for key in kwargs:
            if key not in valid:
                matches = difflib.get_close_matches(key, valid, n=1)
                hint = f" Did you mean '{matches[0]}'?" if matches else ""
                raise TypeError(f"{func.__name__}(): unknown parameter '{key}'.{hint}")
        return func(*args, **kwargs)

    return wrapper  # type: ignore[return-value]
