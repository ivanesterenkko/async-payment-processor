from __future__ import annotations

import argparse
import time
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Heartbeat-file healthcheck.")
    parser.add_argument("--file", required=True, help="Heartbeat file path.")
    parser.add_argument(
        "--max-age-seconds",
        type=float,
        required=True,
        help="Maximum accepted heartbeat age in seconds.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    heartbeat_path = Path(args.file)
    if not heartbeat_path.exists():
        return 1
    age_seconds = time.time() - heartbeat_path.stat().st_mtime
    return 0 if age_seconds <= args.max_age_seconds else 1


if __name__ == "__main__":
    raise SystemExit(main())
