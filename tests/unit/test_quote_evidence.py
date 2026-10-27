import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from nautilus_trader.core import UUID4
from nautilus_trader.model import ClientId
from tests.unit.test_data_conversion import _client

from nautilus_longbridge.config import LongbridgeDataClientConfig
from nautilus_longbridge.symbols import instrument_id_from_symbol


@pytest.mark.asyncio
@pytest.mark.parametrize("case", ["valid", "missing_price", "empty", "empty_quote", "failed_depth"])
async def test_quote_response_evidence_precedes_conversion_and_retains_failures(tmp_path, case):
    client = _client()
    client._lb_config = LongbridgeDataClientConfig(historical_evidence_dir=str(tmp_path))
    client.client_id = ClientId("LONGBRIDGE_DATA")
    row = SimpleNamespace(position=1, price=None if case == "missing_price" else "10.1", volume=100, order_num=2)
    depth = SimpleNamespace(bids=[] if case == "empty" else [row],
                            asks=[SimpleNamespace(position=1, price="10.2", volume=80, order_num=3)])
    client._quote = SimpleNamespace(
        quote=AsyncMock(return_value=[] if case == "empty_quote" else [SimpleNamespace(symbol="700.HK", last_done="10.15")]),
        depth=AsyncMock(return_value=depth, side_effect=ValueError("secret-marker-not-for-evidence") if case == "failed_depth" else None),
    )
    async def invoke(operation):
        return await operation()
    client._quote_call = invoke
    responses = []
    client._handle_response = responses.append
    request_id = UUID4()
    request = SimpleNamespace(instrument_id=instrument_id_from_symbol("700.HK"), request_id=request_id,
                              start_ns=None, end_ns=None)
    if case == "failed_depth":
        with pytest.raises(ValueError):
            await client._request_quotes(request)
        assert not responses
    else:
        await client._request_quotes(request)
        assert len(responses) == 1 and len(responses[0].data) == (1 if case == "valid" else 0)
    path = tmp_path / f"quotes-{request_id}" / "manifest.json"
    evidence = json.loads(path.read_text())
    assert evidence["status"] == ("FAILED" if case == "failed_depth" else "COMPLETE")
    assert evidence["nt_quote_rows"] == (1 if case == "valid" else 0)
    assert "secret-marker" not in path.read_text()
    if case == "missing_price":
        assert evidence["responses"][1]["bids"][0]["price"] is None
    if case == "empty":
        assert evidence["responses"][1]["bids"] == []
    if case == "empty_quote":
        assert evidence["responses"] == [{"kind": "quote", "received_ns": client.clock.timestamp_ns(), "rows": []}]
    if case == "failed_depth":
        assert evidence["error_type"] == "ValueError" and len(evidence["responses"]) == 1
