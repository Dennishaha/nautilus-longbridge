from types import SimpleNamespace

import pytest

from nautilus_longbridge.reference import account_reference


@pytest.mark.asyncio
async def test_readonly_reference_preserves_charges_and_never_claims_borrow_from_margin():
    calls = []

    class Context:
        async def margin_ratio(self, symbol):
            calls.append("margin")
            return SimpleNamespace(im_factor=".5", mm_factor=".3", fm_factor=".2")

        async def history_orders(self, **kwargs):
            calls.append("history")
            return [SimpleNamespace(order_id="PRIVATE-ORDER", executed_quantity=2)]

        async def order_detail(self, order):
            calls.append("detail")
            return SimpleNamespace(side="Buy", executed_quantity=2, executed_price="100.01",
                updated_at="2026-10-01", charge_detail=SimpleNamespace(total_amount="1.02", currency="USD",
                    items=[SimpleNamespace(fees=[SimpleNamespace(code="PLATFORM", name="Platform", amount="1", currency="USD")])]))

        def __getattr__(self, name):
            pytest.fail(f"unexpected SDK operation {name}")

    result = await account_reference(Context(), "AAPL.US", received_ns=123)
    assert calls == ["margin", "history", "detail"]
    assert result["order_charges"][0]["total"] == "1.02"
    assert "PRIVATE-ORDER" not in str(result)
    assert result["borrow_apr"] is None and result["available_quantity"] is None
    assert result["fee_schedule"] == "NOT_EXPOSED_BY_SDK"


@pytest.mark.asyncio
async def test_failed_reference_query_records_failure_without_false_evidence_or_secret_text():
    class Context:
        async def margin_ratio(self, symbol):
            raise PermissionError("SECRET ACCOUNT DETAIL")

        async def history_orders(self, **kwargs):
            return []

    result = await account_reference(Context(), "AAPL.US", received_ns=123)
    assert result["errors"] == [{"operation": "margin_ratio", "type": "PermissionError"}]
    assert "SECRET" not in str(result) and "margin_ratio" not in result
