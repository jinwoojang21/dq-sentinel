from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class Settings:
    """Runtime settings. Everything is overridable with DQ_* environment variables."""

    home: Path = field(default_factory=lambda: Path(os.getenv("DQ_HOME", ".dq")).resolve())
    contracts_dir: Path = field(
        default_factory=lambda: Path(os.getenv("DQ_CONTRACTS_DIR", "contracts")).resolve()
    )
    model: str = field(default_factory=lambda: os.getenv("DQ_MODEL", "claude-sonnet-5-5"))
    max_agent_turns: int = field(default_factory=lambda: int(os.getenv("DQ_MAX_AGENT_TURNS", "12")))
    kafka_bootstrap: str = field(default_factory=lambda: os.getenv("DQ_KAFKA_BOOTSTRAP", "localhost:19092"))
    kafka_topic: str = field(default_factory=lambda: os.getenv("DQ_KAFKA_TOPIC", "orders"))

    @property
    def warehouse(self) -> Path:
        return self.home / "warehouse"

    @property
    def catalog_db(self) -> Path:
        return self.home / "catalog.db"

    @property
    def state_db(self) -> Path:
        return self.home / "state.duckdb"

    @property
    def incidents_dir(self) -> Path:
        return self.home / "incidents"

    @property
    def proposals_dir(self) -> Path:
        return self.contracts_dir / "proposals"

    def ensure_dirs(self) -> None:
        for p in (self.home, self.warehouse, self.incidents_dir, self.proposals_dir):
            p.mkdir(parents=True, exist_ok=True)
