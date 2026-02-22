from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any, Dict

import yaml

from agent.pipeline import DEFAULT_CONFIG, run_pipeline
from agent.utils.logging import setup_logging


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Educational options screener for cash-secured puts and covered calls."
    )
    parser.add_argument("--config", type=str, default=None, help="Path to config YAML")
    parser.add_argument(
        "--tickers",
        type=str,
        default=None,
        help='Comma-separated tickers, e.g. "SPY,QQQ,MSFT"',
    )
    parser.add_argument("--output-dir", type=str, default=None, help="Report output directory")
    parser.add_argument(
        "--max-candidates",
        type=int,
        default=None,
        help="Override max candidates per ticker per bucket",
    )
    return parser.parse_args()


def load_config(args: argparse.Namespace) -> Dict[str, Any]:
    config = dict(DEFAULT_CONFIG)

    config_path = None
    if args.config:
        config_path = Path(args.config)
    else:
        local_default = Path("config.yaml")
        if local_default.exists():
            config_path = local_default

    if config_path:
        with config_path.open("r", encoding="utf-8") as f:
            loaded = yaml.safe_load(f) or {}
            if not isinstance(loaded, dict):
                raise ValueError("Config YAML must parse to a dictionary")
            config.update(loaded)

    if args.tickers:
        tickers = [t.strip().upper() for t in args.tickers.split(",") if t.strip()]
        if tickers:
            config["tickers"] = tickers

    if args.output_dir:
        config["output_dir"] = args.output_dir

    if args.max_candidates is not None:
        config["max_candidates_per_ticker_per_bucket"] = args.max_candidates

    return config


def main() -> None:
    args = parse_args()
    config = load_config(args)
    logger = setup_logging(config)
    run_pipeline(config, logger)


if __name__ == "__main__":
    main()
