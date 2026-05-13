"""Solana SPL-USDT watcher (raw JSON-RPC, no extra dependency).

Strategy:
  1. ``getSignaturesForAddress(wallet, limit=20)`` to grab recent signatures
     involving the receiving wallet.
  2. For each signature, ``getTransaction(sig, encoding=jsonParsed)``.
  3. Walk ``meta.preTokenBalances`` / ``meta.postTokenBalances`` to find the
     delta on the USDT mint where the receiving wallet is the owner and the
     delta equals the order amount.

This avoids hard-pinning solders / solana-py for the backend; we only need
plain HTTP. The Solana mainnet public endpoint is fine for the volumes a
typical SaaS sees (a few orders per minute) but operators can swap in
Helius / QuickNode by setting ``SOLANA_RPC_URL``.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional

import requests

from app.utils.logger import get_logger

from ..chains import CHAIN_SPECS
from .base import IncomingTransfer, WatcherResult, register


logger = get_logger(__name__)

_SPEC = CHAIN_SPECS["SOL"]


def _rpc_url() -> str:
    return (os.getenv("SOLANA_RPC_URL", "https://api.mainnet-beta.solana.com") or "").strip().rstrip("/")


def _usdt_mint() -> str:
    return (os.getenv(_SPEC.contract_env, "") or "").strip() or _SPEC.contract_default


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


def _rpc(method: str, params: List[Any]) -> Dict[str, Any]:
    payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    resp = requests.post(_rpc_url(), json=payload, timeout=15)
    resp.raise_for_status()
    return resp.json() or {}


def _delta_for_owner(
    pre: List[Dict[str, Any]],
    post: List[Dict[str, Any]],
    *,
    mint: str,
    owner: str,
) -> int:
    """Return ``post - pre`` raw token amount for (owner, mint).

    Token-balance entries reference accounts by ``accountIndex`` plus
    ``mint`` + ``owner`` triples; the same accountIndex shows up in both
    arrays when the balance changed, so we can pair them by accountIndex.
    """
    def collect(arr: List[Dict[str, Any]]) -> Dict[int, int]:
        out: Dict[int, int] = {}
        for entry in arr or []:
            try:
                if (entry.get("mint") or "").strip() != mint:
                    continue
                if (entry.get("owner") or "").strip() != owner:
                    continue
                idx = int(entry.get("accountIndex"))
                amount = entry.get("uiTokenAmount") or {}
                raw = int(amount.get("amount") or 0)
                out[idx] = out.get(idx, 0) + raw
            except (TypeError, ValueError):
                continue
        return out

    pre_map = collect(pre)
    post_map = collect(post)
    keys = set(pre_map.keys()) | set(post_map.keys())
    return sum(post_map.get(k, 0) - pre_map.get(k, 0) for k in keys)


def find_incoming(address: str, amount: Decimal, created_at: Optional[datetime]) -> WatcherResult:
    address = (address or "").strip()
    if not address or amount <= 0:
        return None, "bad_args"

    mint = _usdt_mint()
    target = int((amount * Decimal(10 ** _SPEC.decimals)).to_integral_value())
    ct = _parse_created_at(created_at)
    min_ts = int(ct.timestamp()) - 60 if ct else None

    try:
        sigs_resp = _rpc("getSignaturesForAddress", [address, {"limit": 25}])
    except requests.RequestException as exc:
        return None, f"solana_rpc_error:{type(exc).__name__}:{exc}"

    err = sigs_resp.get("error")
    if err:
        return None, f"solana_rpc_error:{err.get('code')}:{err.get('message')!r}"
    signatures = sigs_resp.get("result") or []
    if not signatures:
        return None, "no_match empty_signatures"

    scanned = 0
    before_order = wrong_amount = parse_err = 0

    for sig_entry in signatures:
        try:
            sig = str(sig_entry.get("signature") or "")
            block_time = int(sig_entry.get("blockTime") or 0)
            if not sig:
                parse_err += 1
                continue
            if min_ts is not None and block_time and block_time < min_ts:
                before_order += 1
                continue
            tx_resp = _rpc("getTransaction", [sig, {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 0}])
            tx = (tx_resp.get("result") or {})
            meta = tx.get("meta") or {}
            if meta.get("err"):
                continue
            pre = meta.get("preTokenBalances") or []
            post = meta.get("postTokenBalances") or []
            delta = _delta_for_owner(pre, post, mint=mint, owner=address)
            scanned += 1
            if delta < target - 1 or delta > target + 1:
                wrong_amount += 1
                continue
            transfer = IncomingTransfer(
                tx_hash=sig,
                block_timestamp_ms=block_time * 1000 if block_time else 0,
                from_addr="",   # SPL has multiple senders possible; not needed for matching
                to_addr=address,
                value_smallest_unit=delta,
                raw={"signature": sig, "blockTime": block_time, "meta_keys": list(meta.keys())},
            )
            return transfer, f"ok scanned={scanned}"
        except (TypeError, ValueError):
            parse_err += 1
        except requests.RequestException as exc:
            return None, f"solana_rpc_error:{type(exc).__name__}:{exc}"

    note = (
        f"no_match scanned={scanned} target_raw={target} "
        f"before_order={before_order} wrong_amount={wrong_amount} parse_err={parse_err}"
    )
    return None, note


register("SOL", find_incoming)
