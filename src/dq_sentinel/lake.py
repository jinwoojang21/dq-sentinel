"""Apache Iceberg storage (local SQLite catalog + filesystem warehouse) via pyiceberg."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError

from .config import Settings
from .contract import Contract

NAMESPACE = "dq"
_ARROW_TYPES = {
    "string": pa.string(),
    "double": pa.float64(),
    "long": pa.int64(),
    "boolean": pa.bool_(),
    "timestamp": pa.timestamp("us", tz="UTC"),
}


def arrow_schema(contract: Contract) -> pa.Schema:
    cols = [pa.field(n, _ARROW_TYPES[s.type], nullable=True) for n, s in contract.fields.items()]
    cols += [
        pa.field("_batch_id", pa.string(), nullable=True),
        pa.field("_ingested_at", pa.timestamp("us", tz="UTC"), nullable=True),
    ]
    return pa.schema(cols)


QUARANTINE_SCHEMA = pa.schema(
    [
        pa.field("_batch_id", pa.string(), nullable=True),
        pa.field("raw_json", pa.string(), nullable=True),
        pa.field("reasons", pa.string(), nullable=True),
        pa.field("_ingested_at", pa.timestamp("us", tz="UTC"), nullable=True),
    ]
)


class Lake:
    def __init__(self, settings: Settings, contract: Contract):
        settings.ensure_dirs()
        self.contract = contract
        self.catalog = SqlCatalog(
            "dq",
            uri=f"sqlite:///{settings.catalog_db}",
            warehouse=f"file://{settings.warehouse}",
        )
        try:
            self.catalog.create_namespace(NAMESPACE)
        except NamespaceAlreadyExistsError:
            pass
        self.table_name = contract.name
        self.quarantine_name = f"{contract.name}_quarantine"
        self.table = self._ensure(self.table_name, arrow_schema(contract))
        self.quarantine = self._ensure(self.quarantine_name, QUARANTINE_SCHEMA)

    def _ensure(self, name: str, schema: pa.Schema):
        ident = f"{NAMESPACE}.{name}"
        try:
            return self.catalog.load_table(ident)
        except NoSuchTableError:
            return self.catalog.create_table(ident, schema=schema)

    def append_valid(self, rows: list[dict[str, Any]], batch_id: str) -> None:
        if not rows:
            return
        now = datetime.now(UTC)
        schema = arrow_schema(self.contract)
        data = {f.name: [r.get(f.name) for r in rows] for f in schema if not f.name.startswith("_")}
        data["_batch_id"] = [batch_id] * len(rows)
        data["_ingested_at"] = [now] * len(rows)
        self.table.append(pa.Table.from_pydict(data, schema=schema))

    def append_quarantine(self, items: list[tuple[dict[str, Any], list[str]]], batch_id: str) -> None:
        if not items:
            return
        now = datetime.now(UTC)
        self.quarantine.append(
            pa.Table.from_pydict(
                {
                    "_batch_id": [batch_id] * len(items),
                    "raw_json": [json.dumps(raw, default=str) for raw, _ in items],
                    "reasons": [json.dumps(reasons) for _, reasons in items],
                    "_ingested_at": [now] * len(items),
                },
                schema=QUARANTINE_SCHEMA,
            )
        )

    def scan(self, name: str) -> pa.Table:
        table = self.catalog.load_table(f"{NAMESPACE}.{name}")
        return table.scan().to_arrow()
