"""CLI entry point for python -m datannurpy."""

from __future__ import annotations

import sys


def main() -> None:
    """Run datannurpy CLI."""
    if len(sys.argv) < 2:
        print("Usage: python -m datannurpy <config.yml>")
        sys.exit(1)

    from .config import run_config

    try:
        run_config(sys.argv[1])
    except (ValueError, FileNotFoundError) as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":  # pragma: no cover
    main()
