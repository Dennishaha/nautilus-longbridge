"""The adapter must remain usable without research packages or their policies."""

import ast
from pathlib import Path


def test_connector_does_not_depend_on_research_tools_or_policies():
    root = Path(__file__).parents[2] / "src" / "nautilus_longbridge"
    banned_imports = {"nt_data_tools", "etf_rotation", "orb", "exchange_calendars"}
    banned_terms = {
        "candidate_price_range",
        "SAMPLE_PASS",
        "ml_training",
        "RVOL",
        "nt-research-catalog-v1",
    }
    for file in root.glob("*.py"):
        source = file.read_text()
        assert not any(term in source for term in banned_terms), file
        for node in ast.walk(ast.parse(source)):
            modules = (
                [alias.name for alias in node.names]
                if isinstance(node, ast.Import)
                else [node.module or ""]
                if isinstance(node, ast.ImportFrom)
                else []
            )
            assert not any(name.split(".")[0] in banned_imports for name in modules), file
