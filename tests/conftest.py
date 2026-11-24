from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from dq_sentinel.config import Settings
from dq_sentinel.contract import load_contract

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def contract():
    return load_contract(ROOT / "contracts" / "orders.yaml")


@pytest.fixture
def settings(tmp_path):
    cdir = tmp_path / "contracts"
    cdir.mkdir()
    shutil.copy(ROOT / "contracts" / "orders.yaml", cdir / "orders.yaml")
    return Settings(home=tmp_path / "home", contracts_dir=cdir)
