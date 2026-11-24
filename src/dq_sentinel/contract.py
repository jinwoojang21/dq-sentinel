from __future__ import annotations

import copy
import difflib
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

SUPPORTED_TYPES = {"string", "double", "long", "boolean", "timestamp"}


@dataclass
class FieldSpec:
    name: str
    type: str
    required: bool = False
    unique: bool = False
    min: float | None = None
    max: float | None = None
    allowed: list[str] | None = None
    max_null_rate: float | None = None

    @classmethod
    def from_dict(cls, name: str, d: dict[str, Any]) -> FieldSpec:
        if d.get("type") not in SUPPORTED_TYPES:
            raise ValueError(f"field {name!r}: unsupported type {d.get('type')!r}")
        return cls(
            name=name,
            type=d["type"],
            required=bool(d.get("required", False)),
            unique=bool(d.get("unique", False)),
            min=d.get("min"),
            max=d.get("max"),
            allowed=list(d["allowed"]) if d.get("allowed") is not None else None,
            max_null_rate=d.get("max_null_rate"),
        )


@dataclass
class Contract:
    name: str
    version: int
    owner: str
    fields: dict[str, FieldSpec]
    slos: dict[str, float]
    raw: dict[str, Any]
    path: Path | None = None

    @classmethod
    def from_dict(cls, raw: dict[str, Any], path: Path | None = None) -> Contract:
        fields = {n: FieldSpec.from_dict(n, spec or {}) for n, spec in raw["fields"].items()}
        return cls(
            name=raw["name"],
            version=int(raw.get("version", 1)),
            owner=raw.get("owner", "unknown"),
            fields=fields,
            slos=dict(raw.get("slos", {})),
            raw=raw,
            path=path,
        )

    @property
    def unique_fields(self) -> list[str]:
        return [f.name for f in self.fields.values() if f.unique]

    def slo(self, key: str, default: float) -> float:
        return float(self.slos.get(key, default))

    def to_yaml(self) -> str:
        return yaml.safe_dump(self.raw, sort_keys=False)


def load_contract(path: str | Path) -> Contract:
    path = Path(path)
    with path.open() as fh:
        return Contract.from_dict(yaml.safe_load(fh), path=path)


# --------------------------------------------------------------------------- patches
# The agent proposes changes as a small, typed list of operations rather than free-form YAML.
# That keeps proposals reviewable, testable and impossible to silently widen.

PATCH_OPS = {
    "rename_field": {"from", "to"},
    "add_allowed_value": {"field", "value"},
    "set_field_property": {"field", "key", "value"},
    "set_slo": {"key", "value"},
    "add_field": {"field", "spec"},
}
SETTABLE_FIELD_KEYS = {"required", "min", "max", "max_null_rate", "type", "unique"}


class PatchError(ValueError):
    pass


def apply_patch(raw: dict[str, Any], operations: list[dict[str, Any]]) -> dict[str, Any]:
    """Return a new contract dict with operations applied and the version bumped."""
    new = copy.deepcopy(raw)
    fields = new["fields"]
    for i, op in enumerate(operations):
        kind = op.get("op")
        if kind not in PATCH_OPS:
            raise PatchError(f"operation {i}: unknown op {kind!r}; expected one of {sorted(PATCH_OPS)}")
        missing = PATCH_OPS[kind] - op.keys()
        if missing:
            raise PatchError(f"operation {i} ({kind}): missing {sorted(missing)}")

        if kind == "rename_field":
            if op["from"] not in fields:
                raise PatchError(f"rename_field: no field {op['from']!r}")
            if op["to"] in fields:
                raise PatchError(f"rename_field: field {op['to']!r} already exists")
            # preserve field order
            new["fields"] = fields = {
                (op["to"] if k == op["from"] else k): v for k, v in fields.items()
            }
        elif kind == "add_allowed_value":
            spec = _field(fields, op["field"])
            allowed = spec.setdefault("allowed", [])
            if op["value"] not in allowed:
                allowed.append(op["value"])
        elif kind == "set_field_property":
            if op["key"] not in SETTABLE_FIELD_KEYS:
                raise PatchError(f"set_field_property: key must be one of {sorted(SETTABLE_FIELD_KEYS)}")
            _field(fields, op["field"])[op["key"]] = op["value"]
        elif kind == "set_slo":
            if op["key"] not in new.get("slos", {}):
                raise PatchError(f"set_slo: unknown slo {op['key']!r}")
            new["slos"][op["key"]] = op["value"]
        elif kind == "add_field":
            if op["field"] in fields:
                raise PatchError(f"add_field: field {op['field']!r} already exists")
            FieldSpec.from_dict(op["field"], op["spec"])  # validate
            fields[op["field"]] = op["spec"]

    new["version"] = int(raw.get("version", 1)) + 1
    Contract.from_dict(new)  # whole-contract validation
    return new


def _field(fields: dict[str, Any], name: str) -> dict[str, Any]:
    if name not in fields:
        raise PatchError(f"no field {name!r}")
    return fields[name]


def yaml_diff(old: dict[str, Any], new: dict[str, Any], name: str) -> str:
    a = yaml.safe_dump(old, sort_keys=False).splitlines(keepends=True)
    b = yaml.safe_dump(new, sort_keys=False).splitlines(keepends=True)
    return "".join(difflib.unified_diff(a, b, f"contracts/{name}.yaml", f"contracts/{name}.yaml (proposed)"))
