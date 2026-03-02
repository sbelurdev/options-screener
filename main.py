from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any, Dict

import yaml

from agent.providers.factory import build_options_provider
from agent.pipeline import DEFAULT_CONFIG, run_pipeline
from agent.utils.env import load_dotenv_if_present
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
    parser.add_argument(
        "--provider-smoke-test",
        action="store_true",
        help="Validate configured options provider connectivity and basic data access, then exit",
    )
    parser.add_argument(
        "--smoke-ticker",
        type=str,
        default=None,
        help="Ticker symbol to use for --provider-smoke-test",
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
            for key, value in loaded.items():
                if isinstance(value, dict) and isinstance(config.get(key), dict):
                    config[key] = {**config[key], **value}
                else:
                    config[key] = value

    if args.tickers:
        tickers = [t.strip().upper() for t in args.tickers.split(",") if t.strip()]
        if tickers:
            config["covered_call_tickers"] = tickers
            config["cash_secured_put_tickers"] = tickers

    if args.output_dir:
        config["output_dir"] = args.output_dir

    if args.max_candidates is not None:
        config["max_candidates_per_ticker_per_bucket"] = args.max_candidates

    return config


def main() -> None:
    # Load .env from both current working directory and repo root.
    load_dotenv_if_present(".env")
    load_dotenv_if_present(str(Path(__file__).resolve().parent / ".env"))
    args = parse_args()
    config = load_config(args)
    logger = setup_logging(config)

    if args.provider_smoke_test:
        provider = build_options_provider(config, logger)
        ticker = str(args.smoke_ticker or "SPY").upper()
        print("=" * 72)
        print("Provider Smoke Test")
        print("=" * 72)
        print(f"Options provider: {str(config.get('options_data_provider', 'yfinance')).lower()}")
        print(f"Ticker: {ticker}")
        if hasattr(provider, "smoke_test"):
            result = provider.smoke_test(ticker)  # type: ignore[attr-defined]
            print("Status: PASS")
            print(f"Account ID: {result.get('account_id')}")
            print(f"Expirations returned: {result.get('expiration_count')}")
            sample = result.get("sample_expirations") or []
            if sample:
                print(f"Sample expirations: {', '.join(sample)}")
            return

        expirations = provider.get_options_expirations(ticker)
        if not expirations:
            raise RuntimeError("Provider smoke test failed: no expirations returned")
        print("Status: PASS")
        print(f"Expirations returned: {len(expirations)}")
        print(f"Sample expirations: {', '.join([d.isoformat() for d in expirations[:5]])}")
        return

    run_pipeline(config, logger)


if __name__ == "__main__":
    main()
