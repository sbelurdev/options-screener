from __future__ import annotations

import os
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests

from agent.providers.base import OptionsChainProvider


class PublicOptionsProvider(OptionsChainProvider):
    """Public.com options provider using market-data + greeks endpoints."""

    def __init__(self, logger, config: Dict[str, Any], log_dir: str = "./logs") -> None:
        self.logger = logger
        self.config = config
        self.log_dir = log_dir
        self.base_url = str(config.get("public_api_base_url", "https://api.public.com")).rstrip("/")
        self.timeout_seconds = float(config.get("public_http_timeout_seconds", 20))
        self.secret_env_var = str(config.get("public_api_key_env_var", "PUBLIC_API_KEY"))
        self.token_validity_minutes = int(config.get("public_access_token_validity_minutes", 15))
        self.instrument_type = str(
            config.get("public_underlying_instrument_type", config.get("public_instrument_type", "EQUITY"))
        )
        self._access_token: Optional[str] = None
        self._access_token_expires_at: Optional[datetime] = None
        self._account_id: Optional[str] = str(config.get("public_account_id") or "").strip() or None

    @staticmethod
    def _normalize_osi_symbol(symbol: Any) -> str:
        return str(symbol or "").replace(" ", "").strip().upper()

    @staticmethod
    def _parse_osi(symbol: str) -> Dict[str, Any]:
        s = str(symbol or "").strip()
        out: Dict[str, Any] = {"contract_symbol": s, "strategy": None, "strike": None, "expiration": None}
        if len(s) < 15:
            return out
        tail = s[-15:]
        date_part = tail[:6]
        cp = tail[6:7]
        strike_part = tail[7:]
        if cp not in {"C", "P"}:
            return out
        try:
            parsed_exp = datetime.strptime(date_part, "%y%m%d").date()
            parsed_strike = int(strike_part) / 1000.0
        except ValueError:
            return out
        out["strategy"] = "CALL" if cp == "C" else "PUT"
        out["strike"] = parsed_strike
        out["expiration"] = parsed_exp
        return out

    @staticmethod
    def _as_float(value: Any) -> Optional[float]:
        try:
            if value is None:
                return None
            return float(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _as_int(value: Any) -> Optional[int]:
        try:
            if value is None:
                return None
            return int(float(value))
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _chunks(items: List[str], size: int) -> List[List[str]]:
        return [items[i : i + size] for i in range(0, len(items), size)]

    @staticmethod
    def _get_ci(d: Any, key: str) -> Any:
        if not isinstance(d, dict):
            return None
        if key in d:
            return d[key]
        key_l = key.lower()
        for k, v in d.items():
            if str(k).lower() == key_l:
                return v
        return None

    def _extract_metric(self, row: Dict[str, Any], metric: str) -> Optional[float]:
        # Direct field variants
        direct_keys = [metric, metric.capitalize(), f"{metric}Value", f"option{metric.capitalize()}"]
        for k in direct_keys:
            v = self._get_ci(row, k)
            fv = self._as_float(v)
            if fv is not None:
                return fv

        # Nested dict variants
        nested_containers = ["greeks", "greek", "optionGreeks", "greekValues", "values", "payload"]
        for container_key in nested_containers:
            container = self._get_ci(row, container_key)
            if isinstance(container, dict):
                v = self._get_ci(container, metric)
                fv = self._as_float(v)
                if fv is not None:
                    return fv
            if isinstance(container, list):
                for item in container:
                    if not isinstance(item, dict):
                        continue
                    greek_name = (
                        self._get_ci(item, "name")
                        or self._get_ci(item, "type")
                        or self._get_ci(item, "greek")
                        or self._get_ci(item, "greekType")
                    )
                    if str(greek_name or "").strip().lower() != metric.lower():
                        continue
                    v = (
                        self._get_ci(item, "value")
                        or self._get_ci(item, "greekValue")
                        or self._get_ci(item, "val")
                        or self._get_ci(item, metric)
                    )
                    fv = self._as_float(v)
                    if fv is not None:
                        return fv
        return None

    def _request_json(
        self,
        method: str,
        path: str,
        *,
        headers: Optional[Dict[str, str]] = None,
        params: Optional[Dict[str, Any]] = None,
        payload: Optional[Dict[str, Any]] = None,
    ) -> Any:
        url = f"{self.base_url}{path}"
        resp = requests.request(
            method=method,
            url=url,
            headers=headers,
            params=params,
            json=payload,
            timeout=self.timeout_seconds,
        )
        if resp.status_code >= 400:
            self.logger.error(
                "Public API request failed method=%s path=%s status=%s params=%s payload=%s response=%s",
                method,
                path,
                resp.status_code,
                params,
                payload,
                (resp.text or "")[:1000],
            )
        resp.raise_for_status()
        return resp.json()

    def _get_secret(self) -> str:
        secret = os.getenv(self.secret_env_var)
        if not secret:
            raise ValueError(
                f"Missing Public API secret: set environment variable '{self.secret_env_var}'"
            )
        return secret

    def _get_access_token(self) -> str:
        now = datetime.now(timezone.utc)
        if (
            self._access_token
            and self._access_token_expires_at is not None
            and now < self._access_token_expires_at
        ):
            return self._access_token

        body = {"secret": self._get_secret(), "validityInMinutes": self.token_validity_minutes}
        data = self._request_json("POST", "/userapiauthservice/personal/access-tokens", payload=body)
        token = str((data or {}).get("accessToken") or "").strip()
        if not token:
            raise RuntimeError("Public API token response did not include accessToken")
        ttl = max(self.token_validity_minutes - 1, 1)
        self._access_token = token
        self._access_token_expires_at = now + timedelta(minutes=ttl)
        return token

    def _auth_headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self._get_access_token()}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    def _get_account_id(self) -> str:
        if self._account_id:
            return self._account_id

        data = self._request_json("GET", "/userapigateway/trading/account", headers=self._auth_headers())
        if isinstance(data, dict):
            accounts = data.get("accounts") or []
        elif isinstance(data, list):
            accounts = data
        else:
            accounts = []
        if not accounts:
            raise RuntimeError("Public API returned no accounts")
        brokerage = [a for a in accounts if str(a.get("accountType", "")).upper() == "BROKERAGE"]
        selected = brokerage[0] if brokerage else accounts[0]
        account_id = str(selected.get("accountId") or "").strip()
        if not account_id:
            raise RuntimeError("Public API account payload missing accountId")
        self._account_id = account_id
        return account_id

    def _marketdata_post(self, endpoint: str, payload: Dict[str, Any]) -> Any:
        account_id = self._get_account_id()
        path = f"/userapigateway/marketdata/{account_id}/{endpoint}"
        return self._request_json("POST", path, headers=self._auth_headers(), payload=payload)

    def _get_greeks(self, osi_symbols: List[str]) -> Dict[str, Dict[str, Optional[float]]]:
        if not osi_symbols:
            return {}
        out: Dict[str, Dict[str, Optional[float]]] = {}
        account_id = self._get_account_id()
        path = f"/userapigateway/option-details/{account_id}/greeks"
        for chunk in self._chunks(osi_symbols, 250):
            data = self._request_json(
                "GET",
                path,
                headers=self._auth_headers(),
                params={"osiSymbols": ",".join(chunk)},
            )
            if isinstance(data, list):
                rows = data
            elif isinstance(data, dict):
                payload = data.get("payload")
                if isinstance(payload, list):
                    rows = payload
                elif isinstance(payload, dict):
                    rows = (
                        payload.get("greeks")
                        or payload.get("optionGreeks")
                        or payload.get("data")
                        or []
                    )
                else:
                    rows = (
                        data.get("greeks")
                        or data.get("optionGreeks")
                        or data.get("data")
                        or []
                    )
            else:
                rows = []
            for row in rows:
                instrument = row.get("instrument") if isinstance(row, dict) else {}
                sym = self._normalize_osi_symbol(
                    row.get("osiSymbol")
                    or row.get("osi")
                    or row.get("symbol")
                    or row.get("optionSymbol")
                    or (instrument.get("symbol") if isinstance(instrument, dict) else None)
                )
                if not sym:
                    continue
                out[sym] = {
                    "delta": self._extract_metric(row, "delta"),
                    "impliedVolatility": self._extract_metric(row, "impliedVolatility"),
                }
            matched_with_delta = sum(1 for v in out.values() if v.get("delta") is not None)
            self.logger.debug(
                "public greeks lookup requested=%d returned=%d matched_with_delta=%d",
                len(chunk),
                len(rows),
                matched_with_delta,
            )
        return out

    def _fetch_options_expirations(self, ticker: str) -> List[date]:
        payload = {"instrument": {"symbol": ticker, "type": self.instrument_type}}
        data = self._marketdata_post("option-expirations", payload)
        if isinstance(data, dict):
            rows = data.get("expirations") or []
        elif isinstance(data, list):
            rows = data
        else:
            rows = []
        expirations: List[date] = []
        for row in rows:
            if isinstance(row, str):
                raw = row
            else:
                raw = row.get("expirationDate") or row.get("expiration")
            if not raw:
                continue
            try:
                expirations.append(date.fromisoformat(str(raw)))
            except ValueError:
                continue
        return sorted(set(expirations))

    def smoke_test(self, ticker: str) -> Dict[str, Any]:
        token = self._get_access_token()
        account_id = self._get_account_id()
        expirations = self._fetch_options_expirations(ticker)
        if not expirations:
            raise RuntimeError(f"No expirations returned for ticker '{ticker}'")
        return {
            "provider": "public",
            "ticker": ticker,
            "account_id": account_id,
            "access_token_obtained": bool(token),
            "expiration_count": len(expirations),
            "sample_expirations": [d.isoformat() for d in expirations[:5]],
        }

    def get_options_expirations(self, ticker: str) -> List[date]:
        try:
            return self._fetch_options_expirations(ticker)
        except Exception as exc:
            self.logger.warning("%s: public expirations lookup failed: %s", ticker, exc)
            return []

    def get_options_chain(self, ticker: str, expiration: date) -> Tuple[pd.DataFrame, pd.DataFrame]:
        try:
            payload = {
                "instrument": {"symbol": ticker, "type": self.instrument_type},
                "expirationDate": expiration.isoformat(),
            }
            data = self._marketdata_post("option-chain", payload)
            if isinstance(data, dict):
                calls_payload = data.get("calls") or []
                puts_payload = data.get("puts") or []
                rows = [*calls_payload, *puts_payload]
            elif isinstance(data, list):
                rows = data
            else:
                rows = []
            options: List[Dict[str, Any]] = []
            for row in rows:
                instrument = row.get("instrument") or {}
                symbol = str(
                    row.get("symbol")
                    or row.get("osiSymbol")
                    or instrument.get("symbol")
                    or ""
                ).strip()
                parsed = self._parse_osi(symbol)
                symbol_key = self._normalize_osi_symbol(symbol)
                row_exp = row.get("expirationDate") or row.get("expiration")
                parsed_exp = parsed.get("expiration")
                if row_exp:
                    try:
                        parsed_exp = date.fromisoformat(str(row_exp))
                    except ValueError:
                        pass
                if parsed_exp is not None and parsed_exp != expiration:
                    continue

                strategy = str(row.get("optionType") or row.get("type") or "").upper()
                if strategy not in {"PUT", "CALL"}:
                    strategy = str(
                        row.get("putOrCall")
                        or instrument.get("putOrCall")
                        or parsed.get("strategy")
                        or ""
                    ).upper()
                strike = self._as_float(row.get("strikePrice"))
                if strike is None:
                    strike = self._as_float(row.get("strike"))
                if strike is None:
                    strike = self._as_float(parsed.get("strike"))
                options.append(
                    {
                        "contractSymbol": symbol,
                        "_contractSymbolKey": symbol_key,
                        "strategy": strategy,
                        "strike": strike,
                        "bid": self._as_float(row.get("bid") if row.get("bid") is not None else row.get("bidPrice")),
                        "ask": self._as_float(row.get("ask") if row.get("ask") is not None else row.get("askPrice")),
                        "lastPrice": self._as_float(
                            row.get("last")
                            if row.get("last") is not None
                            else row.get("lastPrice")
                        ),
                        "volume": self._as_int(row.get("volume")),
                        "openInterest": self._as_int(
                            row.get("openInterest")
                            if row.get("openInterest") is not None
                            else row.get("open_interest")
                        ),
                        "impliedVolatility": self._as_float(
                            row.get("impliedVolatility")
                            if row.get("impliedVolatility") is not None
                            else row.get("iv")
                        ),
                    }
                )

            symbols = [o["_contractSymbolKey"] for o in options if o.get("_contractSymbolKey")]
            greeks = self._get_greeks(symbols)
            for option in options:
                sym = option.get("_contractSymbolKey")
                if sym in greeks:
                    option["delta"] = greeks[sym].get("delta")
                    if option.get("impliedVolatility") is None:
                        option["impliedVolatility"] = greeks[sym].get("impliedVolatility")
            for option in options:
                option.pop("_contractSymbolKey", None)

            calls_df = pd.DataFrame([o for o in options if o.get("strategy") == "CALL"])
            puts_df = pd.DataFrame([o for o in options if o.get("strategy") == "PUT"])
            return calls_df, puts_df
        except Exception as exc:
            self.logger.warning("%s %s: public option chain lookup failed: %s", ticker, expiration.isoformat(), exc)
            return pd.DataFrame(), pd.DataFrame()
