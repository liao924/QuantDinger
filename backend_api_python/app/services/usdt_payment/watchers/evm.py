"""EVM USDT watcher (BEP20 via BscScan, ERC20 via Etherscan).

Both BscScan and Etherscan expose an identical `module=account&action=tokentx`
endpoint, so a single function handles both. The chain code selects which
host + API key pair to use.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, Optional

import requests

from app.utils.logger import get_logger

from ..chains import CHAIN_SPECS
from .base import IncomingTransfer, WatcherResult, register


logger = get_logger(__name__)


_EVM_HOSTS: Dict[str, Dict[str, Any]] = {
    "BEP20": {
        "base_env": "BSCSCAN_BASE_URL",
        "base_default": "https://api.bscscan.com",
        "key_env": "BSCSCAN_API_KEY",
    },
    "ERC20": {
        "base_env": "ETHERSCAN_BASE_URL",
        "base_default": "https://api.etherscan.io",
        "key_env": "ETHERSCAN_API_KEY",
    },
}


def _resolve_contract(chain: str) -> str:
    spec = CHAIN_SPECS[chain]
    return (os.getenv(spec.contract_env, "") or "").strip() or spec.contract_default


def _parse_created_at(raw: Any) -> Optional[datetime]:
    if raw is None:
        return None
    if isinstance(raw, datetime):
        return raw if raw.tzinfo else raw.replace(tzinfo=timezone.utc)
    if isinstance(raw, str):
        try:
            dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except ValueError:
            return None
    return None


def _make_finder(chain: str):
    spec = CHAIN_SPECS[chain]
    cfg = _EVM_HOSTS[chain]

    def find_incoming(address: str, amount: Decimal, created_at: Optional[datetime]) -> WatcherResult:
        address = (address or "").strip()
        if not address or amount <= 0:
            return None, "bad_args"

        base = (os.getenv(cfg["base_env"], "") or cfg["base_default"]).strip().rstrip("/")
        api_key = (os.getenv(cfg["key_env"], "") or "").strip()
        contract = _resolve_contract(chain)

        if not api_key:
            # Both Etherscan and BscScan accept anonymous traffic but the
            # daily quota is very low (200/day). We log a one-time warning
            # but proceed.
            logger.debug("%s watcher: no API key configured, using anonymous quota", chain)

        target = int((amount * (Decimal(10) ** spec.decimals)).to_integral_value())
        ct = _parse_created_at(created_at)
        min_ts = int(ct.timestamp()) - 60 if ct else None  # explorers use seconds, not ms

        params: Dict[str, Any] = {
            "module": "account",
            "action": "tokentx",
            "contractaddress": contract,
            "address": address,
            "page": 1,
            "offset": 50,
            "sort": "desc",
        }
        if api_key:
            params["apikey"] = api_key

        url = f"{base}/api"
        try:
            resp = requests.get(url, params=params, timeout=15)
            if resp.status_code != 200:
                head = (resp.text or "")[:200].replace("\n", " ")
                return None, f"{chain.lower()}_http={resp.status_code} body={head!r}"

            body = resp.json() or {}
            status = str(body.get("status") or "")
            items = body.get("result") or []
            # Etherscan returns status="0", result=[] when there are no
            # transfers yet — treat as an empty scan, not an error.
            if status == "0" and isinstance(items, list) and not items:
                return None, "no_match empty_result"
            if status == "0" and not isinstance(items, list):
                return None, f"explorer_error msg={body.get('message')!r} result={str(items)[:120]!r}"

            scanned = 0
            wrong_to = before_order = wrong_amount = parse_err = 0
            for it in items:
                scanned += 1
                try:
                    if (it.get("to") or "").lower() != address.lower():
                        wrong_to += 1
                        continue
                    ts = int(it.get("timeStamp") or 0)
                    if min_ts is not None and ts < min_ts:
                        before_order += 1
                        continue
                    val = int(it.get("value") or 0)
                    # Allow 1 wei slop; in practice EVM transfers are exact.
                    if val < target - 1 or val > target + 1:
                        wrong_amount += 1
                        continue
                    transfer = IncomingTransfer(
                        tx_hash=str(it.get("hash") or ""),
                        block_timestamp_ms=ts * 1000,
                        from_addr=str(it.get("from") or ""),
                        to_addr=str(it.get("to") or ""),
                        value_smallest_unit=val,
                        raw=it,
                    )
                    return transfer, f"ok scanned={scanned}"
                except (TypeError, ValueError):
                    parse_err += 1

            note = (
                f"no_match scanned={scanned} target_raw={target} "
                f"wrong_to={wrong_to} before_order={before_order} wrong_amount={wrong_amount} "
                f"parse_err={parse_err}"
            )
            return None, note
        except requests.RequestException as exc:
            return None, f"{chain.lower()}_request_error:{type(exc).__name__}:{exc}"

    return find_incoming


register("BEP20", _make_finder("BEP20"))
register("ERC20", _make_finder("ERC20"))
