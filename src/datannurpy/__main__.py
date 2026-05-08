"""CLI entry point for python -m datannurpy."""

from __future__ import annotations

import sys
from importlib.metadata import version


USAGE = "Usage: python -m datannurpy <config.yml>"


def main() -> None:
    """Run datannurpy CLI."""
    if len(sys.argv) >= 2 and sys.argv[1] in ("-h", "--help"):
        print(USAGE)
        sys.exit(0)

    if len(sys.argv) >= 2 and sys.argv[1] in ("-V", "--version"):
        print(f"datannurpy {version('datannurpy')}")
        sys.exit(0)

    if len(sys.argv) < 2:
        print(USAGE)
        sys.exit(1)

    from .config import run_config
    from .errors import ConfigError

    try:
        run_config(sys.argv[1])
    except ConfigError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":  # pragma: no cover
    main()
