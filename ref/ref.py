#!/usr/bin/env python3
"""Read-only audit of IDMC target UPSERT key configuration and Databricks target metadata.

Run in a Databricks notebook or as a local Python script.
- Never writes to Delta tables.
- Never guesses merge keys.
- XML inspection proves only what is present in the supplied export.
- Databricks metadata checks do not prove IDMC's configured update-key semantics.
"""
from __future__ import annotations

import argparse
import re
import sys
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any, Iterable

TARGETS = {
    "TGT_CUSTOMER_GOLD": "idmc_poc.curated.customer_gold",
    "TGT_CUSTOMER_STANDARD": "idmc_poc.curated.customer_standard",
    "TGT_CUSTOMER_LOW_VALUE": "idmc_poc.curated.customer_low_value",
}

KEY_TERMS = re.compile(r"(merge.?key|key.?column|update.?column|primary.?key|match.?condition|upsert|update.?strategy|target.?operation|keycolumns|primarykey)", re.I)


def local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1].split(":")[-1]


def attrs_text(elem: ET.Element) -> str:
    return " ".join(f"{k}={v}" for k, v in elem.attrib.items())


def walk_with_path(root: ET.Element) -> Iterable[tuple[str, ET.Element]]:
    def rec(node: ET.Element, path: str):
        name = local_name(node.tag)
        this_path = f"{path}/{name}"
        yield this_path, node
        for child in list(node):
            yield from rec(child, this_path)
    yield from rec(root, "")


def inspect_xml(xml_path: Path) -> dict[str, Any]:
    print("\n" + "=" * 88)
    print("1. IDMC XML EXPORT INSPECTION (READ-ONLY)")
    print("=" * 88)
    if not xml_path.exists():
        raise FileNotFoundError(f"XML file not found: {xml_path}")

    root = ET.parse(xml_path).getroot()
    all_nodes = list(walk_with_path(root))
    target_results: dict[str, dict[str, Any]] = {}

    for target_id, expected_table in TARGETS.items():
        matches = []
        for path, elem in all_nodes:
            if local_name(elem.tag).lower() not in {"target", "targetinstance", "target_instance"}:
                continue
            ident = elem.attrib.get("id", "")
            name = elem.attrib.get("name", "")
            obj = elem.attrib.get("object", "")
            if ident == target_id or name == target_id or target_id in name:
                matches.append((path, elem))

        print(f"\nTarget: {target_id}  | expected Databricks table: {expected_table}")
        if not matches:
            print("  RESULT: target instance not found by ID/name in XML")
            target_results[target_id] = {"found": False, "key_evidence": []}
            continue

        evidence = []
        for path, elem in matches:
            print(f"  XML path: {path}")
            print(f"  Attributes: {attrs_text(elem) or '(none)'}")
            print(f"  Direct text: {(elem.text or '').strip() or '(none)'}")
            for child in elem.iter():
                if child is elem:
                    continue
                child_name = local_name(child.tag)
                detail = f"{child_name} {attrs_text(child)} {(child.text or '').strip()}".strip()
                if KEY_TERMS.search(detail):
                    evidence.append((path + "/" + child_name, detail))
                    print(f"  KEY/UPSERT-related child: {path}/{child_name}: {detail}")

        # Also scan attributes/text of all nodes for key-related configuration; this can reveal
        # extension/property nodes that aren't nested directly under the target instance.
        global_hits = []
        for path, elem in all_nodes:
            blob = f"{local_name(elem.tag)} {attrs_text(elem)} {(elem.text or '').strip()}"
            if KEY_TERMS.search(blob) and any(t.lower() in path.lower() or t.lower() in blob.lower() for t in (target_id, expected_table.split('.')[-1])):
                global_hits.append((path, blob))
        # Avoid duplicate output, while retaining useful target-specific evidence.
        seen = set(evidence)
        for path, blob in global_hits:
            item = (path, blob)
            if item not in seen:
                evidence.append(item)
                print(f"  XML key/config candidate: {path}: {blob}")
                seen.add(item)

        key_hits = [(p, v) for p, v in evidence if re.search(r"(merge.?key|key.?column|primary.?key|keycolumns|primarykey|match.?condition)", v, re.I)]
        update_hits = [(p, v) for p, v in evidence if re.search(r"(update.?column|update.?strategy|upsert|target.?operation)", v, re.I)]
        print(f"  UPSERT/update evidence count: {len(update_hits)}")
        print(f"  Explicit key/match evidence count: {len(key_hits)}")
        if not key_hits:
            print("  RESULT: NO explicit merge/update key found in this XML export. Do not infer customer_id.")
        target_results[target_id] = {
            "found": True,
            "key_evidence": key_hits,
            "update_evidence": update_hits,
        }

    print("\nMapping-task / task-instance evidence")
    task_hits = []
    for path, elem in all_nodes:
        name = local_name(elem.tag).lower()
        attrs = attrs_text(elem)
        if "task" in name or "task" in attrs.lower():
            blob = f"{path}: {local_name(elem.tag)} {attrs} {(elem.text or '').strip()}".strip()
            if KEY_TERMS.search(blob) or name in {"mappingtask", "mapping_task", "taskinstance", "task_instance"}:
                task_hits.append(blob)
    if task_hits:
        for hit in task_hits:
            print("  " + hit)
    else:
        print("  No mapping-task / task-instance write-key settings found in XML.")

    # Global scan: display all elements containing likely key/update config terms.
    print("\nAll XML elements mentioning key/update/UPSERT-related terms")
    global_key_hits = []
    for path, elem in all_nodes:
        blob = f"{local_name(elem.tag)} {attrs_text(elem)} {(elem.text or '').strip()}".strip()
        if KEY_TERMS.search(blob):
            global_key_hits.append((path, blob))
    if global_key_hits:
        for path, blob in global_key_hits:
            print(f"  {path}: {blob}")
    else:
        print("  No matching terms found.")

    return {"targets": target_results, "global_hits": global_key_hits}


def quote_identifier(identifier: str) -> str:
    # Only allow normal Unity Catalog identifiers, not arbitrary SQL fragments.
    parts = identifier.split(".")
    if len(parts) != 3 or not all(re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", p) for p in parts):
        raise ValueError(f"Unsafe/unexpected table identifier: {identifier}")
    return ".".join(f"`{p}`" for p in parts)


def rows_as_dicts(df) -> list[dict[str, Any]]:
    return [r.asDict(recursive=True) for r in df.collect()]


def inspect_databricks_tables(spark) -> None:
    print("\n" + "=" * 88)
    print("2. DATABRICKS TARGET TABLE METADATA (READ-ONLY)")
    print("=" * 88)
    for target_id, table in TARGETS.items():
        qtable = quote_identifier(table)
        print(f"\nTarget: {target_id} -> {table}")
        try:
            print("  Schema:")
            spark.sql(f"DESCRIBE TABLE {qtable}").show(200, truncate=False)
        except Exception as exc:
            print(f"  DESCRIBE TABLE unavailable: {type(exc).__name__}: {exc}")

        try:
            print("  Table properties:")
            spark.sql(f"SHOW TBLPROPERTIES {qtable}").show(200, truncate=False)
        except Exception as exc:
            print(f"  SHOW TBLPROPERTIES unavailable: {type(exc).__name__}: {exc}")

        # These views are available in Unity Catalog in many workspaces, but permissions
        # and platform versions differ. Their absence is reported rather than treated as proof.
        try:
            catalog, schema, table_name = table.split(".")
            constraints_sql = f"""
                SELECT constraint_name, constraint_type, enforced
                FROM `{catalog}`.information_schema.table_constraints
                WHERE lower(table_schema) = lower('{schema}')
                  AND lower(table_name) = lower('{table_name}')
                ORDER BY constraint_type, constraint_name
            """
            print("  Unity Catalog constraints:")
            spark.sql(constraints_sql).show(100, truncate=False)
        except Exception as exc:
            print(f"  Constraint metadata unavailable: {type(exc).__name__}: {exc}")

        try:
            catalog, schema, table_name = table.split(".")
            key_cols_sql = f"""
                SELECT tc.constraint_name, tc.constraint_type, kcu.column_name, kcu.ordinal_position
                FROM `{catalog}`.information_schema.table_constraints tc
                JOIN `{catalog}`.information_schema.key_column_usage kcu
                  ON tc.constraint_catalog = kcu.constraint_catalog
                 AND tc.constraint_schema = kcu.constraint_schema
                 AND tc.constraint_name = kcu.constraint_name
                 AND tc.table_name = kcu.table_name
                WHERE lower(tc.table_schema) = lower('{schema}')
                  AND lower(tc.table_name) = lower('{table_name}')
                ORDER BY tc.constraint_name, kcu.ordinal_position
            """
            print("  Declared constraint columns:")
            spark.sql(key_cols_sql).show(100, truncate=False)
        except Exception as exc:
            print(f"  Constraint-column metadata unavailable: {type(exc).__name__}: {exc}")

        print("  NOTE: Databricks constraints are not a substitute for IDMC target Update Columns.")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--xml", default="IDMC_EnterpriseCustomer360_fixed.xml", help="Path to IDMC XML export")
    parser.add_argument("--databricks", action="store_true", help="Also inspect target tables using an active Databricks Spark session")
    args = parser.parse_args()

    try:
        result = inspect_xml(Path(args.xml))
    except Exception as exc:
        print(f"ERROR: XML inspection failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 2

    if args.databricks:
        try:
            spark  # noqa: F821 - provided in Databricks notebooks
        except NameError:
            print("ERROR: --databricks requested, but no active Spark session named 'spark' exists.", file=sys.stderr)
            return 3
        inspect_databricks_tables(spark)  # noqa: F821

    print("\n" + "=" * 88)
    print("AUDIT SUMMARY")
    print("=" * 88)
    for target_id, info in result["targets"].items():
        if not info.get("found"):
            status = "TARGET NOT FOUND IN XML"
        elif info.get("key_evidence"):
            status = "POSSIBLE EXPLICIT KEY CONFIG FOUND — REVIEW THE PRINTED XML PATH/VALUE"
        else:
            status = "NO EXPLICIT MERGE KEY FOUND IN PROVIDED XML"
        print(f"{target_id}: {status}")
    print("\nNo table writes or MERGE operations were executed by this audit.")
    print("This script cannot inspect IDMC UI-only configuration that is absent from the XML export.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
