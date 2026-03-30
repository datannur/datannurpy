"""Parameter validation utilities."""

from __future__ import annotations

import difflib
import functools
import inspect
from typing import Any, Callable, TypeVar

from ..errors import ConfigError

F = TypeVar("F", bound=Callable[..., Any])

MIN_SAMPLE_SIZE = 100


def validate_params(func: F) -> F:
    """Validate kwargs and suggest closest parameter name on typo."""
    valid = set(inspect.signature(func).parameters)

    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        for key in kwargs:
            if key not in valid:
                matches = difflib.get_close_matches(key, valid, n=1)
                hint = f" Did you mean '{matches[0]}'?" if matches else ""
                raise ConfigError(
                    f"{func.__name__}(): unknown parameter '{key}'.{hint}"
                )
        # Reject sample_size below minimum
        if "sample_size" in kwargs and kwargs["sample_size"] is not None:
            if kwargs["sample_size"] < MIN_SAMPLE_SIZE:
                raise ConfigError(
                    f"sample_size must be at least {MIN_SAMPLE_SIZE}, "
                    f"got {kwargs['sample_size']}"
                )
        return func(*args, **kwargs)

    return wrapper  # type: ignore[return-value]
