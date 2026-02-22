from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict


def setup_logging(config: Dict[str, Any]) -> logging.Logger:
    log_dir = Path(config.get("log_dir", "./logs"))
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / "app.log"

    logger = logging.getLogger("options_agent")
    logger.setLevel(logging.INFO)

    if logger.handlers:
        return logger

    fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s - %(message)s")

    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setFormatter(fmt)
    logger.addHandler(file_handler)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(fmt)
    logger.addHandler(stream_handler)

    logger.propagate = False
    return logger
