"""Build normalized, XML-grounded metadata for LLM-based IDMC migrations.

Backward compatible: build(mapping, graph) remains valid. If the source XML is
available, the builder enriches mapping metadata with the actual DQ rule
expressions and records missing source/target metadata without guessing it.
"""
from pathlib import Path
import xml.etree.ElementTree as ET


class LLMInputBuilder:
    def __init__(
        self,
        source_catalog="idmc_poc",
        source_schema="bronze",
        target_catalog="idmc_poc",
        target_schema="silver",
        source_file_base_path="/Volumes/idmc_poc/bronze/idmc_files",
        metadata_xml_path="/Volumes/idmc_poc/bronze/idmc_files/IDMC_Advanced_Complex_Metadata_Repository.xml",
        csv_defaults=None,
    ):
        self.source_catalog = source_catalog
        self.source_schema = source_schema
        self.target_catalog = target_catalog
        self.target_schema = target_schema
        self.source_file_base_path = source_file_base_path
        self.metadata_xml_path = metadata_xml_path
        # Current Customer360 landing CSV was verified to have a header row.
        # Override this per project/file contract when different.
        self.csv_defaults = dict(csv_defaults or {"header": "true", "inferSchema": "true"})

    def build(self, mapping, graph, xml_path=None, specialized_metadata=None):
        """Build the prompt payload; existing build(mapping, graph) calls work."""
        mapping_name = mapping.get("name") or mapping.get("mapping_name")
        xml_info = self._extract_xml_info(xml_path or self.metadata_xml_path, mapping_name)
        if specialized_metadata:
            xml_info["specialized_metadata"] = self._clean_metadata(specialized_metadata)

        sources = self._build_sources(mapping, xml_info)
        targets = self._build_targets(mapping, xml_info)
        transformations = self._build_transformations(mapping, xml_info)
        links = self._build_links(mapping, graph)
        blockers = self._identify_blockers(sources, targets, transformations, xml_info, links)

        return {
            "mapping": self._build_mapping_metadata(mapping),
            "graph": self._build_graph_metadata(graph),
            "sources": sources,
            "targets": targets,
            "transformations": transformations,
            "links": links,
            "data_quality_rules": xml_info.get("data_quality_rules", []),
            "xml_mapping_metadata": xml_info.get("mapping_metadata", {}),
            "xml_warning": xml_info.get("xml_warning"),
            # Keep repository-level metadata in the actual payload sent to the LLM.
            # Merely attaching it to the local xml_info object is not sufficient.
            "specialized_metadata": xml_info.get("specialized_metadata", {}),
            "blocking_issues": blockers,
            "databricks": {
                "source_catalog": self.source_catalog,
                "source_schema": self.source_schema,
                "target_catalog": self.target_catalog,
                "target_schema": self.target_schema,
                "source_file_base_path": self.source_file_base_path,
            },
        }

    def _extract_xml_info(self, xml_path, mapping_name):
        empty = {"mapping_metadata": {}, "source_fields": {}, "target_metadata": {},
                 "transformation_metadata": {}, "data_quality_rules": [], "xml_available": False}
        if not xml_path or not Path(str(xml_path)).exists():
            empty["xml_path"] = str(xml_path) if xml_path else None
            empty["xml_warning"] = "XML file was not readable by LLMInputBuilder; using parser output only."
            return empty
        try:
            root = ET.parse(str(xml_path)).getroot()
        except (ET.ParseError, OSError) as exc:
            empty["xml_path"] = str(xml_path)
            empty["xml_warning"] = f"Could not parse XML: {exc}"
            return empty

        def tag(el):
            return el.tag.rsplit("}", 1)[-1]
        def children(el, wanted):
            return [x for x in el.iter() if tag(x) == wanted]
        def text_child(el, wanted):
            for x in el.iter():
                if tag(x) == wanted:
                    return (x.text or "").strip()
            return None

        rules_by_id = {}
        for rule in children(root, "DataQualityRule"):
            rid = rule.attrib.get("id")
            if not rid:
                continue
            rules_by_id[rid] = {
                "id": rid,
                "name": rule.attrib.get("name"),
                "enabled": rule.attrib.get("enabled"),
                "severity": rule.attrib.get("severity"),
                "expression": text_child(rule, "RuleExpression"),
                "failure_action": text_child(rule, "FailureAction"),
            }

        selected = None
        for el in children(root, "Mapping"):
            if el.attrib.get("name") == mapping_name:
                selected = el
                break
        if selected is None:
            empty["xml_path"] = str(xml_path)
            empty["xml_warning"] = f"Mapping {mapping_name!r} not found in XML."
            empty["data_quality_rules"] = list(rules_by_id.values())
            return empty

        source_fields = {}
        for src in children(selected, "Source"):
            name = src.attrib.get("name")
            if not name:
                continue
            fields = []
            for field in src.iter():
                if field is src or tag(field) not in {"Field", "Column"}:
                    continue
                fname = field.attrib.get("name")
                if fname:
                    item = dict(field.attrib)
                    item["name"] = fname
                    fields.append(item)
            source_fields[name] = fields

        # Target definitions can be declared at repository/folder level rather than
        # nested inside the selected Mapping. Scan the XML for Target elements and
        # support both <Column> and <Field> metadata shapes. When duplicate target
        # names exist, retain the richest schema instead of letting an empty mapping
        # instance overwrite the actual target definition.
        target_metadata = {}

        def target_columns(tgt):
            columns = []
            # Support <Target><Column .../></Target>,
            # <Target><Columns><Column .../></Columns></Target>, and Field variants.
            for child in list(tgt):
                child_tag = tag(child)
                if child_tag in {"Column", "Field"}:
                    col = dict(child.attrib)
                    if col.get("name"):
                        columns.append(col)
                elif child_tag in {"Columns", "Fields"}:
                    for field in list(child):
                        if tag(field) in {"Column", "Field"}:
                            col = dict(field.attrib)
                            if col.get("name"):
                                columns.append(col)
            return columns

        def target_merge_keys(attrs, columns):
            keys = []
            for key_attr in ("mergeKeys", "merge_keys", "keyColumns", "primaryKey", "keys"):
                raw = attrs.get(key_attr)
                if raw:
                    keys.extend(x.strip() for x in raw.replace(";", ",").split(",") if x.strip())
            for col in columns:
                if str(col.get("key", col.get("isKey", ""))).lower() in {"true", "1", "yes"}:
                    if col.get("name"):
                        keys.append(col["name"])
            return list(dict.fromkeys(keys))

        for tgt in children(root, "Target"):
            name = tgt.attrib.get("name")
            if not name:
                continue
            columns = target_columns(tgt)
            attrs = dict(tgt.attrib)
            candidate = {
                "columns": columns,
                "merge_keys": target_merge_keys(attrs, columns),
                "operation": attrs.get("operation"),
                "attributes": attrs,
            }
            previous = target_metadata.get(name)
            # Keep the candidate with more schema/key information.
            score = (len(candidate["columns"]), len(candidate["merge_keys"]))
            old_score = (len(previous.get("columns", [])), len(previous.get("merge_keys", []))) if previous else (-1, -1)
            if previous is None or score > old_score:
                target_metadata[name] = candidate
            elif previous and not previous.get("operation") and candidate.get("operation"):
                previous["operation"] = candidate["operation"]

        transformation_metadata = {}
        for trans in children(selected, "Transformations"):
            for node in list(trans):
                name = node.attrib.get("name")
                if not name:
                    continue
                item = {"id": node.attrib.get("id"), "name": name,
                        "type": tag(node), "attributes": dict(node.attrib)}
                # Preserve exact XML expressions and conditions.
                expressions = []
                for child in node.iter():
                    child_tag = tag(child)
                    value = (child.text or "").strip()
                    if value and child_tag in {"Condition", "JoinCondition", "RuleExpression"}:
                        expressions.append({"element": child_tag, "expression": value})
                    if child_tag in {"DerivedField", "AggregateField", "GroupBy", "SortField", "OutputGroup", "ReturnField", "OutputField", "RuleRef"}:
                        expressions.append({"element": child_tag, "attributes": dict(child.attrib), "value": value or None})
                item["xml_details"] = expressions
                refs = [x.attrib.get("objectId") for x in node.iter() if tag(x) == "RuleRef" and x.attrib.get("objectId")]
                item["data_quality_rules"] = [rules_by_id[x] for x in refs if x in rules_by_id]
                transformation_metadata[name] = item

        mapping_meta = {
            "id": selected.attrib.get("id"),
            "name": selected.attrib.get("name"),
            "mode": selected.attrib.get("mode"),
            "status": selected.attrib.get("status"),
            "xml_path": str(xml_path),
        }
        # Only rules referenced by this mapping's DQ transformations are marked as mapping rules.
        referenced_rule_ids = set()
        for node in selected.iter():
            if tag(node) == "RuleRef" and node.attrib.get("objectId"):
                referenced_rule_ids.add(node.attrib["objectId"])
        return {
            "mapping_metadata": mapping_meta,
            "source_fields": source_fields,
            "target_metadata": target_metadata,
            "transformation_metadata": transformation_metadata,
            "data_quality_rules": [rules_by_id[x] for x in rules_by_id if x in referenced_rule_ids],
            "all_xml_data_quality_rules": list(rules_by_id.values()),
            "xml_available": True,
            "xml_path": str(xml_path),
        }

    def _build_mapping_metadata(self, mapping):
        return {"name": mapping.get("name") or mapping.get("mapping_name"),
                "description": mapping.get("description"),
                "attributes": self._clean_metadata(mapping.get("attributes", {}))}

    def _build_sources(self, mapping, xml_info):
        result = []
        xml_fields = xml_info.get("source_fields", {})
        for original in mapping.get("source_instances", mapping.get("sources", [])):
            source = self._clean_metadata(original)
            logical = source.get("source") or source.get("name") or source.get("object")
            source["logical_name"] = logical
            source_lookup_names = [
                source.get("name"),
                source.get("source"),
                source.get("object"),
                source.get("logical_name"),
            ]
            xml_source_name = next(
                (str(candidate) for candidate in source_lookup_names
                 if candidate and str(candidate) in xml_fields),
                None,
            ) if isinstance(xml_fields, dict) else None
            xml_declared = xml_fields.get(xml_source_name) if xml_source_name else None
            if xml_info.get("xml_available") and xml_source_name:
                # Empty Fields in the XML means no source columns were declared; do not
                # mislabel transformation-referenced names as physical schema metadata.
                source["columns"] = self._clean_metadata(xml_declared or [])
                source["ports"] = []
                source["fields"] = self._clean_metadata(xml_declared or [])
                source["source_columns_declared_in_xml"] = bool(xml_declared)
            supplied = source.get("file_path") or source.get("physical_table") or logical
            resolved = self._resolve_location(supplied, source.get("catalog") or self.source_catalog,
                                              source.get("schema") or self.source_schema)
            source.update(resolved)
            if source.get("location_type") == "file":
                source.setdefault("format", self._format_from_path(source.get("file_path")))
                if source.get("file_path") and not source["file_path"].startswith(("/", "dbfs:/", "abfss://", "s3://", "wasbs://", "gs://", "file:/")):
                    source["resolved_file_path"] = self.source_file_base_path.rstrip("/") + "/" + source["file_path"].lstrip("/")
                else:
                    source["resolved_file_path"] = source.get("file_path")
                # Keep generator and validator on the same explicit read contract.
                if str(source.get("format") or "").lower() == "csv":
                    options = source.get("options")
                    options = dict(options) if isinstance(options, dict) else {}
                    for option_name, option_value in self.csv_defaults.items():
                        options.setdefault(option_name, option_value)
                    source["options"] = options
            result.append(source)
        return result

    def _build_targets(self, mapping, xml_info):
        result = []
        xml_targets = xml_info.get("target_metadata", {})
        for original in mapping.get("target_instances", mapping.get("targets", [])):
            target = self._clean_metadata(original)
            logical = target.get("target") or target.get("name") or target.get("object")
            target["logical_name"] = logical
            supplied = target.get("physical_table") or target.get("table") or target.get("object") or logical
            resolved = self._resolve_location(supplied, target.get("catalog") or self.target_catalog,
                                              target.get("schema") or self.target_schema)
            target.update(resolved)
            target_lookup_names = [
                target.get("name"),
                target.get("target"),
                target.get("object"),
                target.get("table"),
                target.get("target_name"),
                target.get("logical_name"),
            ]
            xml_target = {}
            for lookup_name in target_lookup_names:
                if lookup_name and str(lookup_name) in xml_targets:
                    xml_target = xml_targets[str(lookup_name)]
                    break
            if not xml_target:
                # Match a fully-qualified identifier only by its final table component;
                # never infer columns or keys from the identifier itself.
                physical_name = target.get("physical_table")
                if physical_name:
                    final_component = str(physical_name).split(".")[-1]
                    xml_target = xml_targets.get(final_component, {})
            if xml_target:
                # Prefer non-empty XML-derived metadata, but never overwrite richer
                # parser-provided metadata with empty arrays.
                xml_columns = self._clean_metadata(xml_target.get("columns", []))
                xml_keys = self._clean_metadata(xml_target.get("merge_keys", []))
                if xml_columns:
                    target["columns"] = xml_columns
                else:
                    target.setdefault("columns", [])
                if xml_keys:
                    target["merge_keys"] = xml_keys
                else:
                    target.setdefault("merge_keys", [])
                target["operation"] = target.get("operation") or xml_target.get("operation")
            target.setdefault("merge_keys", [])
            target.setdefault("columns", [])
            result.append(target)
        return result

    def _build_transformations(self, mapping, xml_info):
        result = []
        xml_trans = xml_info.get("transformation_metadata", {})
        for original in mapping.get("transformations", []):
            trans = self._clean_metadata(original)
            name = trans.get("name")
            xml_item = xml_trans.get(name) if name else None
            if xml_item is None and name:
                xml_item = next(
                    (value for key, value in xml_trans.items()
                     if str(key).strip().casefold() == str(name).strip().casefold()),
                    None,
                )
            if xml_item:
                trans["xml_metadata"] = self._clean_metadata(xml_item)
                xml_rules = xml_item.get("data_quality_rules", [])
                if xml_rules:
                    # XML is authoritative for expressions and rule attributes.
                    trans["data_quality_rules"] = self._clean_metadata(xml_rules)
            result.append(trans)
        return result

    def _build_links(self, mapping, graph):
        edges = graph.get("edges", []) if isinstance(graph, dict) else []
        return self._clean_metadata(edges if edges else mapping.get("links", []))

    def _build_graph_metadata(self, graph):
        graph = graph if isinstance(graph, dict) else {}
        return {"nodes": self._clean_metadata(graph.get("nodes", [])),
                "edges": self._clean_metadata(graph.get("edges", [])),
                "execution_order": self._clean_metadata(graph.get("execution_order", [])),
                "node_count": len(graph.get("nodes", [])), "edge_count": len(graph.get("edges", []))}

    def _identify_blockers(self, sources, targets, transformations, xml_info, links):
        blockers = []
        for source in sources:
            if source.get("location_type") == "unresolved":
                blockers.append(f"Source {source.get('logical_name')} has no resolvable physical location.")
            if source.get("location_type") == "file" and not source.get("format"):
                blockers.append(f"File source {source.get('logical_name')} has no recognized format.")
            if source.get("location_type") == "file" and not source.get("columns"):
                blockers.append(f"Source {source.get('logical_name')} has no source-column schema declared in the XML; verify physical file schema at runtime.")
            if source.get("location_type") == "file" and source.get("format") == "csv":
                options = source.get("options", {})
                if not isinstance(options, dict) or not any(k in options for k in ("header", "delimiter", "sep")):
                    blockers.append(f"CSV source {source.get('logical_name')} does not declare header/delimiter options; configure these from the actual file contract.")
        for target in targets:
            op = str(target.get("operation") or "INSERT").upper()
            if op in {"UPSERT", "MERGE"} and not target.get("merge_keys"):
                blockers.append(f"Target {target.get('physical_table')} uses {op} but the XML does not define merge keys.")
            if not target.get("columns"):
                blockers.append(f"Target {target.get('physical_table')} has no target columns/schema declared in the XML.")
        router = [t for t in transformations if str(t.get("type", "")).lower() == "router" or "router" in str(t.get("type", "")).lower()]
        for trans in router:
            xml_meta = trans.get("xml_metadata", {})
            details = xml_meta.get("xml_details", []) if isinstance(xml_meta, dict) else []
            # XML extraction stores simple Condition text under "expression"
            # (not "value"). Accept both shapes for backward compatibility.
            has_condition = any(
                "condition" in str(item.get("element", "")).lower()
                and (item.get("expression") or item.get("value"))
                for item in details
                if isinstance(item, dict)
            )
            if not has_condition:
                blockers.append(f"Router {trans.get('name')} defines output groups but no routing conditions in the XML.")
        for trans in transformations:
            trans_type = str(trans.get("type", "")).replace("_", "").replace("-", "").lower()
            if "dataquality" in trans_type:
                rules = trans.get("data_quality_rules", [])
                if not rules:
                    blockers.append(f"Data-quality transformation {trans.get('name')} has no resolved rule expressions.")
                else:
                    missing_expressions = [
                        str(rule.get("id") or rule.get("name") or "UNKNOWN_RULE")
                        for rule in rules
                        if isinstance(rule, dict) and not str(rule.get("expression") or "").strip()
                    ]
                    if missing_expressions:
                        blockers.append(
                            f"Data-quality transformation {trans.get('name')} has rules without resolved expressions: "
                            + ", ".join(missing_expressions)
                        )
        if not xml_info.get("xml_available"):
            blockers.append(xml_info.get("xml_warning", "XML metadata could not be loaded by InputBuilder."))
        return list(dict.fromkeys(blockers))

    @staticmethod
    def _format_from_path(path):
        lower = str(path or "").lower().split("?", 1)[0]
        for ext, fmt in ((".csv", "csv"), (".jsonl", "json"), (".json", "json"),
                         (".parquet", "parquet"), (".avro", "avro"), (".orc", "orc"), (".txt", "text"), (".xml", "xml")):
            if lower.endswith(ext):
                return fmt
        return None

    @staticmethod
    def _looks_like_file_path(value):
        if not isinstance(value, str):
            return False
        value = value.strip()
        lower = value.lower()
        return lower.endswith((".csv", ".json", ".jsonl", ".parquet", ".avro", ".orc", ".txt", ".xml", ".xlsx", ".xls")) or lower.startswith(("dbfs:/", "abfss://", "s3://", "wasbs://", "gs://", "file:/", "file://", "/")) or "/" in value or "\\" in value

    def _resolve_location(self, value, catalog, default_schema):
        result = {"physical_table": None, "file_path": None, "location_type": "unresolved", "location_resolution": None}
        if value is None or not str(value).strip():
            result["location_resolution"] = "No location was supplied."
            return result
        name = str(value).strip()
        if self._looks_like_file_path(name):
            result.update(file_path=name, location_type="file", location_resolution="Preserved as file path; not a table identifier.")
            return result
        parts = [p.strip() for p in name.split(".")]
        if any(not p for p in parts):
            result["location_resolution"] = f"Invalid dotted identifier: {name!r}"
        elif len(parts) == 1:
            result.update(physical_table=f"{catalog}.{default_schema}.{parts[0]}", location_type="table", location_resolution="Resolved using configured catalog/schema.")
        elif len(parts) == 2:
            result.update(physical_table=f"{catalog}.{parts[0]}.{parts[1]}", location_type="table", location_resolution="Interpreted as schema.table and prefixed with catalog.")
        elif len(parts) == 3:
            result.update(physical_table=name, location_type="table", location_resolution="Preserved catalog.schema.table identifier.")
        else:
            result["location_resolution"] = f"Unsupported identifier: {name!r}"
        return result

    def _clean_metadata(self, value):
        if isinstance(value, dict):
            return {str(k): self._clean_metadata(v) for k, v in value.items()}
        if isinstance(value, list):
            return [self._clean_metadata(v) for v in value]
        if isinstance(value, (tuple, set)):
            return [self._clean_metadata(v) for v in value]
        if value is None or isinstance(value, (str, int, float, bool)):
            return value
        return str(value)
