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
        catalog = run_config(sys.argv[1])
    except ConfigError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

    # Scanning is continue-on-error: a partial-scan run still exports a valid
    # (truncated) catalogue and, by default (on_scan_error="warn"), exits 0. Set
    # on_scan_error="fail" to make partial failures fail the run (exit 2) so CI
    # doesn't publish a truncated catalogue green. ConfigError keeps exit 1.
    if catalog.on_scan_error == "fail" and catalog.run_errors:
        print(
            f"Error: {catalog.run_errors} dataset(s) failed to scan "
            f"(on_scan_error='fail')",
            file=sys.stderr,
        )
        sys.exit(2)


if __name__ == "__main__":  # pragma: no cover
    main()
