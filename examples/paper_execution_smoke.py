import os

from nautilus_longbridge.config import _env_bool


def main() -> None:
    if not _env_bool("LONGBRIDGE_PAPERTRADING", False):
        raise SystemExit("paper smoke requires LONGBRIDGE_PAPERTRADING=true")
    if not _env_bool("LONGBRIDGE_ENABLE_LIVE_SMOKE", False):
        raise SystemExit("paper smoke requires LONGBRIDGE_ENABLE_LIVE_SMOKE=1")
    if not _env_bool("LONGBRIDGE_ENABLE_ORDER_SMOKE", False):
        print("query-only paper smoke: order submission is disabled")
    print("Paper smoke is opt-in; configure credentials in the environment before connecting.")
    print("account_id:", os.getenv("LONGBRIDGE_ACCOUNT_ID", "<unset>"))


if __name__ == "__main__":
    main()
