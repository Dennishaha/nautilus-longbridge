"""Optional raw history response records; no research or strategy decisions."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from .history import market_timezone
from .mapping import timestamp_ns


class HistoryEvidence:
    def __init__(
        self, directory: str, request_id: str, symbol: str, period: str, adjustment: str,
        trade_sessions: str = "Intraday",
        enable_overnight: bool = False,
    ) -> None:
        self.directory = Path(directory) / request_id
        self.directory.mkdir(parents=True, exist_ok=False)
        self.received_ns: dict[int, int] = {}
        self.manifest: dict[str, Any] = {
            "schema": "longbridge-history-evidence-v1",
            "status": "COLLECTING",
            "request_id": request_id,
            "symbol": symbol,
            "period": period,
            "adjustment": adjustment,
            "trade_sessions": trade_sessions,
            "enable_overnight": enable_overnight,
            "request_mode": "offset" if trade_sessions == "All" and period.endswith("Min_1") else "date",
            "requests": [],
        }
        self._write("manifest.json", self.manifest)

    def _write(self, name: str, value: dict) -> Path:
        path = self.directory / name
        temporary = path.with_suffix(".tmp")
        temporary.write_text(json.dumps(value, indent=2, allow_nan=False) + "\n")
        temporary.replace(path)
        return path

    def capture(self, first: date, last: date, values: list[Any]) -> None:
        received = datetime.now(UTC)
        zone = market_timezone(self.manifest["symbol"])
        rows = []
        for candle in values:
            stamp = candle.timestamp.astimezone(zone)
            rows.append(
                {
                    "timestamp": stamp.isoformat(),
                    "timestamp_sdk": candle.timestamp.isoformat(),
                    "session": stamp.date().isoformat(),
                    "provider_trade_session": str(getattr(candle, "trade_session", "UNSPECIFIED")),
                    **{
                        key: str(getattr(candle, key))
                        for key in ("open", "high", "low", "close", "volume")
                    },
                }
            )
            self.received_ns[timestamp_ns(candle.timestamp, 0)] = timestamp_ns(received, 0)
        name = f"{len(self.manifest['requests']):04d}.json"
        path = self._write(
            name,
            {
                "start": str(first),
                "end": str(last),
                "received_at": received.isoformat(),
                "candles": rows,
            },
        )
        self.manifest["requests"].append(
            {
                "path": name,
                "start": str(first),
                "end": str(last),
                "rows": len(rows),
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            }
        )
        self._write("manifest.json", self.manifest)

    def capture_error(self, anchor: datetime, attempt: int, code: int | None, error_type: str) -> None:
        self.manifest.setdefault("attempt_errors", []).append({
            "anchor": anchor.isoformat(), "attempt": attempt, "provider_error_code": code,
            "error_type": error_type, "received_at": datetime.now(UTC).isoformat(),
        })
        self._write("manifest.json", self.manifest)

    def finish(self, error: BaseException | None = None) -> None:
        self.manifest["status"] = "FAILED" if error else "COMPLETE"
        if error:
            self.manifest["error_type"] = type(error).__name__
        self._write("manifest.json", self.manifest)

class QuoteEvidence:
    """Allowlisted SDK quote/depth fields before conversion, not raw wire packets."""

    def __init__(self, directory: str, request_id: str, symbol: str) -> None:
        self.directory = Path(directory) / f"quotes-{request_id}"
        self.directory.mkdir(parents=True, exist_ok=False)
        self.manifest: dict[str, Any] = {
            "schema": "longbridge-quote-evidence-v1", "status": "COLLECTING",
            "request_id": request_id, "symbol": symbol,
            "semantics": "Selected SDK response fields before conversion; not raw network packets",
            "responses": [],
        }
        self._save()

    def _save(self) -> None:
        path = self.directory / "manifest.json"
        temporary = path.with_suffix(".tmp")
        temporary.write_text(json.dumps(self.manifest, indent=2, allow_nan=False) + "\n")
        temporary.replace(path)

    @staticmethod
    def _fields(value: Any, names: tuple[str, ...]) -> dict[str, Any]:
        return {name: None if getattr(value, name, None) is None else str(getattr(value, name))
                for name in names}

    def capture_quotes(self, values: list[Any], received_ns: int) -> None:
        self.manifest["responses"].append({
            "kind": "quote", "received_ns": received_ns,
            "rows": [self._fields(value, ("symbol", "last_done", "timestamp", "trade_status"))
                     for value in values],
        })
        self._save()

    def capture_depth(self, depth: Any, received_ns: int) -> None:
        self.manifest["responses"].append({
            "kind": "depth", "received_ns": received_ns,
            **self._fields(depth, ("timestamp", "sequence")),
            **{side: [self._fields(row, ("position", "price", "volume", "order_num"))
                      for row in (getattr(depth, side, None) or [])]
               for side in ("bids", "asks")},
        })
        self._save()

    def finish(self, rows: int, error: BaseException | None = None) -> None:
        self.manifest["status"] = "FAILED" if error else "COMPLETE"
        self.manifest["nt_quote_rows"] = rows
        if error:
            self.manifest["error_type"] = type(error).__name__
        self._save()
