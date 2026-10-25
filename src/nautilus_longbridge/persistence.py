"""Native Nautilus streaming configuration for a reusable bar catalog."""

from __future__ import annotations

from pathlib import Path

from nautilus_trader.model import NautilusDataType, NautilusInstrumentType
from nautilus_trader.persistence import DataCatalogConfig, StreamingConfig


def bar_catalog_streaming(
    catalog_path: str | Path,
    *,
    flush_interval_ms: int = 1_000,
    promotion_interval_ms: int = 60_000,
) -> StreamingConfig:
    """Configure the NT-owned Feather -> Parquet lifecycle for bars and equities.

    Use with ``LiveNodeConfig(streaming=...)`` and a data client configured with
    ``completed_bars_only=True``. Staging stays under the catalog root so NT's
    ``convert_stream_to_data`` can also recover a retained live run there.
    ``node.dispose()`` closes the writer and promotes the remaining staged rows.
    """
    if not str(catalog_path).strip():
        raise ValueError("catalog_path must not be empty")
    for name, value in (
        ("flush_interval_ms", flush_interval_ms),
        ("promotion_interval_ms", promotion_interval_ms),
    ):
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise ValueError(f"{name} must be a positive integer")
    path = str(Path(catalog_path).expanduser().resolve())
    return StreamingConfig(
        writer_path=path,
        catalog=DataCatalogConfig(path=path),
        data_types=[NautilusDataType.Bar],
        instrument_types=[NautilusInstrumentType("Equity")],
        flush_interval_ms=flush_interval_ms,
        promotion_interval_ms=promotion_interval_ms,
        promote_on_close=True,
        delete_feather_after_promotion=False,
        use_ts_event_for_ts_init=False,
    )
