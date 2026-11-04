"""Local runner: `python -m pipeline.run all`."""

from __future__ import annotations

import argparse
import logging

from . import bronze, gold, publish, quality, silver
from .config import Settings

STAGES = ("bronze", "silver", "gold", "quality", "publish")

RUNNERS = {
    "bronze": bronze.run,
    "silver": silver.run,
    "gold": gold.run,
    "quality": quality.run,
    "publish": publish.run,
}


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the sales medallion pipeline")
    parser.add_argument(
        "stage",
        choices=(*STAGES, "all"),
        help="single stage, or 'all' to run them in order",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s | %(message)s",
        datefmt="%H:%M:%S",
    )

    settings = Settings.from_env()
    stages = STAGES if args.stage == "all" else (args.stage,)

    for stage in stages:
        logging.getLogger(__name__).info("--- %s ---", stage)
        result = RUNNERS[stage](settings)
        logging.getLogger(__name__).info("%s done: %s", stage, result)


if __name__ == "__main__":
    main()
