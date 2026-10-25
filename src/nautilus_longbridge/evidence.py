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
        self, directory: str, request_id: str, symbol: str, period: str, adjustment: str
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
            "trade_sessions": "Intraday",
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

    def finish(self, error: BaseException | None = None) -> None:
        self.manifest["status"] = "FAILED" if error else "COMPLETE"
        if error:
            self.manifest["error_type"] = type(error).__name__
        self._write("manifest.json", self.manifest)
