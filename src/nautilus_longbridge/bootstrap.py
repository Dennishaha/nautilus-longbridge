"""NT adapter bootstrap owns literal environment-file loading."""

import os
import re
import shlex
from pathlib import Path


def load_env(path: Path) -> None:
    for line in path.read_text().splitlines():
        key, separator, value = line.strip().removeprefix("export ").partition("=")
        if separator and re.fullmatch(r"LONGBRIDGE_[A-Z0-9_]+", key.strip()):
            values = shlex.split(value, comments=True)
            if len(values) > 1:
                raise ValueError("invalid Longbridge environment value")
            os.environ[key.strip()] = values[0] if values else ""
