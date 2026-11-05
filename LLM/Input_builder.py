"""Build a loss-minimizing, XML-grounded input payload for IDMC migration.

The builder deliberately does not invent source paths, schemas, merge keys,
routing conditions, or transformation expressions. It preserves the parser's
metadata and enriches it with the complete XML subtrees for the selected mapping.
"""
from pathlib import Path
import re
import xml.etree.ElementTree as ET


class LLMInputBuilder:
    def __init__(
        self,
        source_catalog="idmc_poc",
        source_schema="bronze",
        target_catalog="idmc_poc",
        target_schema="silver",
        source_file_base_path="/Volumes/idmc_poc/bronze/idmc_files/landing",
        metadata_xml_path="/Volumes/idmc_poc/bronze/idmc_files/EnterpriseCustomer360_Advanced_sanitized.xml",
        csv_defaults=None,
        source_schema_registry=None,
        ambiguous_source_schemas=None,
    ):
        self.source_catalog = source_catalog
        self.source_schema = source_schema
        self.target_catalog = target_catalog
        self.target_schema = target_schema
        self.source_file_base_path = source_file_base_path
        self.metadata_xml_path = metadata_xml_path

        # Logical source name -> physical schema. XML-declared locations always
        # take precedence over this configurable fallback registry.
        default_registry = {
            "category_master": "bronze",
            "country_master": "bronze",
            "customer": "bronze",
            "orders": "bronze",
            "product": "bronze",
            "products": "erp",
            "inventory": "wms",
            "product_category": "reference",
            "customer_consent": "landing",
            "privacy_policy": "reference",
            "transaction": "payments",
            "chargebacks": "risk",
            "device_trust": "risk",
            "web_events": "landing",
            "campaigns": "marketing",
            "invoice": "erp",
            "credit_note": "erp",
            "purchase_orders": "erp",
            "suppliers": "erp",
            "support_cases": "crm",
            "sla_policy": "reference",
        }
        self.source_schema_registry = {
            self._norm(key): value
            for key, value in (source_schema_registry or default_registry).items()
        }
        self.ambiguous_source_schemas = {
            self._norm(key): list(value)
            for key, value in (ambiguous_source_schemas or {"orders": ["bronze", "sales"]}).items()
        }

        # Keep defaults configurable. Pass {} to disable defaults completely.
        self.csv_defaults = dict(
            {"header": "true", "inferSchema": "true"}
            if csv_defaults is None else csv_defaults
        )

    @staticmethod
    def _tag(element):
        return str(element.tag).rsplit("}", 1)[-1]

    @classmethod
    def _xml_to_dict(cls, element):
        """Serialize every XML attribute, text node and descendant (no whitelist)."""
        result = {"tag": cls._tag(element)}
        if element.attrib:
            result["attributes"] = dict(element.attrib)
        text_value = (element.text or "").strip()
        if text_value:
            result["text"] = text_value
        children = list(element)
        if children:
            result["children"] = [cls._xml_to_dict(child) for child in children]
        return result

    @classmethod
    def _descendants(cls, element, wanted):
        return [node for node in element.iter() if cls._tag(node) == wanted]

    @classmethod
    def _first_text(cls, element, wanted):
        for node in element.iter():
            if cls._tag(node) == wanted:
                value = (node.text or "").strip()
                if value:
                    return value
        return None

    @staticmethod
    def _norm(value):
        return re.sub(r"[^a-z0-9]", "", str(value or "").casefold())

    def build(self, mapping, graph, xml_path=None, specialized_metadata=None):
        """Build the complete LLM payload. Existing build(mapping, graph) calls work."""
        mapping = mapping if isinstance(mapping, dict) else {}
        graph = graph if isinstance(graph, dict) else {}
        mapping_name = mapping.get("name") or mapping.get("mapping_name")
        xml_info = self._extract_xml_info(
            xml_path or self.metadata_xml_path, mapping_name
        )
        if specialized_metadata:
            xml_info["specialized_metadata"] = self._clean_metadata(
                specialized_metadata
            )

        sources = self._build_sources(mapping, xml_info)
        targets = self._build_targets(mapping, xml_info)
        transformations = self._build_transformations(mapping, xml_info)
        links = self._build_links(mapping, graph)
        blockers = self._identify_blockers(
            sources, targets, transformations, xml_info, links
        )

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
        empty = {
            "mapping_metadata": {},
            "source_fields": {},
            "target_metadata": {},
            "transformation_metadata": {},
            "data_quality_rules": [],
            "xml_available": False,
            "xml_path": str(xml_path) if xml_path else None,
        }
        if not xml_path or not Path(str(xml_path)).exists():
            empty["xml_warning"] = (
                "XML file was not found/readable by LLMInputBuilder; "
                "only parser-provided metadata is available."
            )
            return empty

        try:
            root = ET.parse(str(xml_path)).getroot()
        except (ET.ParseError, OSError, ValueError) as exc:
            empty["xml_warning"] = f"Could not parse XML: {exc}"
            return empty

        # Build repository-wide DQ rule index. Full XML is retained so that rule
        # properties not known to this version of the builder are not discarded.
        rules_by_id = {}
        for rule in self._descendants(root, "DataQualityRule"):
            attrs = dict(rule.attrib)
            rule_id = attrs.get("id") or attrs.get("objectId")
            if not rule_id:
                continue
            rules_by_id[str(rule_id)] = {
                "id": str(rule_id),
                "name": attrs.get("name"),
                "enabled": attrs.get("enabled"),
                "severity": attrs.get("severity"),
                "expression": self._first_text(rule, "RuleExpression"),
                "failure_action": self._first_text(rule, "FailureAction"),
                "attributes": attrs,
                "xml": self._xml_to_dict(rule),
            }

        selected = None
        wanted_name = self._norm(mapping_name)
        for candidate in self._descendants(root, "Mapping"):
            if self._norm(candidate.attrib.get("name")) == wanted_name and wanted_name:
                selected = candidate
                break

        if selected is None:
            empty["xml_warning"] = (
                f"Mapping {mapping_name!r} was not found in XML. "
                "No XML metadata was matched to this mapping."
            )
            empty["all_xml_data_quality_rules"] = list(rules_by_id.values())
            return empty

        # Mapping-specific sources: keep every declared field/column and the full
        # source subtree. Multiple source instances with the same name are retained.
        source_fields = {}
        source_xml = {}
        for src in self._descendants(selected, "Source"):
            name = src.attrib.get("name") or src.attrib.get("object")
            if not name:
                continue
            fields = []
            for field in src.iter():
                if field is src or self._tag(field) not in {"Field", "Column", "Port"}:
                    continue
                if field.attrib.get("name"):
                    fields.append(dict(field.attrib))
            key = str(name)
            source_fields.setdefault(key, [])
            known = {self._norm(x.get("name")) for x in source_fields[key]}
            for field in fields:
                if self._norm(field.get("name")) not in known:
                    source_fields[key].append(field)
            source_xml.setdefault(key, []).append(self._xml_to_dict(src))

        # Target definitions may be in the mapping or repository section. Keep
        # candidates by name and preserve all variants; choose the richest only
        # when resolving a particular mapping target.
        target_metadata = {}
        for tgt in self._descendants(root, "Target"):
            attrs = dict(tgt.attrib)
            name = attrs.get("name") or attrs.get("object")
            if not name:
                continue
            columns = []
            for node in tgt.iter():
                if node is tgt or self._tag(node) not in {"Column", "Field"}:
                    continue
                if node.attrib.get("name"):
                    columns.append(dict(node.attrib))
            candidate = {
                "name": name,
                "columns": columns,
                "merge_keys": self._extract_merge_keys(attrs, columns),
                "operation": self._first_nonempty(
                    attrs, ("operation", "writeMode", "write_mode", "loadType", "load_type")
                ),
                "attributes": attrs,
                "xml": self._xml_to_dict(tgt),
            }
            target_metadata.setdefault(str(name), []).append(candidate)

        # Transformations are direct children of a Transformations container.
        # Preserve the entire subtree, including connector-specific/unknown tags.
        transformation_metadata = {}
        for container in self._descendants(selected, "Transformations"):
            for node in list(container):
                name = node.attrib.get("name") or node.attrib.get("id")
                if not name:
                    continue
                refs = []
                for ref in self._descendants(node, "RuleRef"):
                    ref_id = (
                        ref.attrib.get("objectId")
                        or ref.attrib.get("id")
                        or ref.attrib.get("ref")
                    )
                    if ref_id:
                        refs.append(str(ref_id))
                transformation_metadata[str(name)] = {
                    "id": node.attrib.get("id"),
                    "name": node.attrib.get("name"),
                    "type": self._tag(node),
                    "attributes": dict(node.attrib),
                    "xml_details": [self._xml_to_dict(child) for child in list(node)],
                    "xml": self._xml_to_dict(node),
                    "data_quality_rule_refs": list(dict.fromkeys(refs)),
                    "data_quality_rules": [
                        rules_by_id[ref] for ref in dict.fromkeys(refs)
                        if ref in rules_by_id
                    ],
                }

        referenced_rule_ids = set()
        for node in selected.iter():
            if self._tag(node) == "RuleRef":
                ref_id = (
                    node.attrib.get("objectId")
                    or node.attrib.get("id")
                    or node.attrib.get("ref")
                )
                if ref_id:
                    referenced_rule_ids.add(str(ref_id))

        mapping_meta = {
            "id": selected.attrib.get("id"),
            "name": selected.attrib.get("name"),
            "mode": selected.attrib.get("mode"),
            "status": selected.attrib.get("status"),
            "attributes": dict(selected.attrib),
            "xml": self._xml_to_dict(selected),
            "xml_path": str(xml_path),
        }
        return {
            "mapping_metadata": mapping_meta,
            "source_fields": source_fields,
            "source_xml": source_xml,
            "target_metadata": target_metadata,
            "transformation_metadata": transformation_metadata,
            "data_quality_rules": [
                rules_by_id[key] for key in sorted(referenced_rule_ids)
                if key in rules_by_id
            ],
            "all_xml_data_quality_rules": list(rules_by_id.values()),
            "xml_available": True,
            "xml_path": str(xml_path),
        }

    @staticmethod
    def _first_nonempty(attributes, names):
        for name in names:
            value = attributes.get(name)
            if value is not None and str(value).strip():
                return str(value).strip()
        return None

    @staticmethod
    def _extract_merge_keys(attributes, columns):
        keys = []
        for attr in (
            "mergeKeys", "merge_keys", "keyColumns", "key_columns",
            "primaryKey", "primary_key", "keys",
        ):
            raw = attributes.get(attr)
            if raw:
                keys.extend(
                    item.strip() for item in re.split(r"[,;|]", str(raw))
                    if item.strip()
                )
        for column in columns:
            flag = (
                column.get("key")
                or column.get("isKey")
                or column.get("primaryKey")
                or column.get("isPrimaryKey")
            )
            if str(flag).strip().casefold() in {"true", "1", "yes", "y"}:
                if column.get("name"):
                    keys.append(column["name"])
        return list(dict.fromkeys(keys))

    def _build_mapping_metadata(self, mapping):
        return {
            "name": mapping.get("name") or mapping.get("mapping_name"),
            "description": mapping.get("description"),
            "attributes": self._clean_metadata(mapping.get("attributes", {})),
            "parser_metadata": self._clean_metadata(mapping),
        }

    def _build_sources(self, mapping, xml_info):
        result = []
        xml_fields = xml_info.get("source_fields", {})
        xml_source_trees = xml_info.get("source_xml", {})
        originals = mapping.get("source_instances", mapping.get("sources", [])) or []

        for original in originals:
            source = self._clean_metadata(original)
            logical = (
                source.get("source") or source.get("name")
                or source.get("object") or source.get("source_name")
            )
            source["logical_name"] = logical

            candidates = [
                source.get("name"), source.get("source"), source.get("object"),
                source.get("source_name"), logical,
            ]
            matched_name = self._match_key(candidates, xml_fields)
            if matched_name:
                xml_declared = self._clean_metadata(xml_fields[matched_name])
                if xml_declared:
                    source["xml_columns"] = xml_declared
                    source["columns"] = self._merge_named_records(
                        source.get("columns") or source.get("fields") or [],
                        xml_declared,
                    )
                source["xml_source"] = self._clean_metadata(
                    xml_source_trees.get(matched_name, [])
                )
                source["xml_source_name"] = matched_name
                source["source_columns_declared_in_xml"] = bool(xml_declared)

            # Normalize location metadata from parser fields, original XML
            # attributes, and the preserved XML source subtree.
            location_meta = self._source_location_metadata(source)
            for key, value in location_meta.items():
                if value not in (None, "") and source.get(key) in (None, ""):
                    source[key] = value

            supplied = (
                source.get("file_path") or source.get("physical_table")
                or source.get("table") or source.get("object") or logical
            )
            resolved = self._resolve_source_location(
                source=source,
                supplied=supplied,
                logical_name=logical,
            )
            source.update(resolved)

            if source.get("location_type") == "file":
                source.setdefault("format", self._format_from_path(source.get("file_path")))
                file_path = source.get("file_path")
                if file_path and not str(file_path).startswith(
                    ("/", "dbfs:/", "abfss://", "s3://", "wasbs://", "gs://", "file:/")
                ):
                    relative_path = str(file_path).lstrip("/")
                    base_path = self.source_file_base_path.rstrip("/")

                    # Avoid landing/landing when XML stores a path relative to
                    # the idmc_files root but the configured base is landing.
                    if base_path.endswith("/landing") and relative_path.startswith("landing/"):
                        relative_path = relative_path[len("landing/"):]

                    source["resolved_file_path"] = (
                        base_path + "/" + relative_path
                    )
                else:
                    source["resolved_file_path"] = file_path

                if str(source.get("format") or "").lower() == "csv":
                    options = source.get("options")
                    options = dict(options) if isinstance(options, dict) else {}
                    for option_name, option_value in self.csv_defaults.items():
                        options.setdefault(option_name, option_value)
                    source["options"] = options

            result.append(source)
        return result

    @staticmethod
    def _source_location_metadata(source):
        """Read source location hints without discarding original attributes."""
        aliases = {
            "catalog": ("catalog", "sourceCatalog", "source_catalog"),
            "schema": ("schema", "sourceSchema", "source_schema", "owner"),
            "physical_table": (
                "physicalTable", "physical_table", "table", "tableName",
                "sourceTable", "source_table",
            ),
            "file_path": (
                "filePath", "file_path", "path", "sourcePath", "source_path",
                "fileName", "filename", "objectPath", "object_path", "uri",
            ),
            "format": ("format", "fileFormat", "file_format", "sourceFormat"),
            "connection": ("connection", "connectionName", "connection_name"),
            "type": ("type", "sourceType", "source_type"),
        }

        candidates = []
        if isinstance(source, dict):
            candidates.append(source)
            if isinstance(source.get("attributes"), dict):
                candidates.append(source["attributes"])
            xml_sources = source.get("xml_source") or []
            if isinstance(xml_sources, dict):
                xml_sources = [xml_sources]
            for item in xml_sources:
                if isinstance(item, dict):
                    candidates.append(item.get("attributes", {}))

        result = {}
        for output_name, names in aliases.items():
            for candidate in candidates:
                if not isinstance(candidate, dict):
                    continue
                lowered = {str(k).casefold(): v for k, v in candidate.items()}
                for alias in names:
                    value = lowered.get(alias.casefold())
                    if value is not None and str(value).strip():
                        result[output_name] = str(value).strip()
                        break
                if output_name in result:
                    break
        return result

    def _resolve_source_location(self, source, supplied, logical_name):
        """Resolve a source using explicit XML metadata before configured defaults."""
        meta = self._source_location_metadata(source)
        source_type = self._norm(meta.get("type") or source.get("type"))
        explicit_path = (
            meta.get("file_path")
            or source.get("file_path")
            or source.get("path")
        )

        # A file path explicitly declared by XML/parser wins over a table name.
        if explicit_path and (
            self._looks_like_file_path(str(explicit_path))
            or "file" in source_type or "flat" in source_type
        ):
            path = str(explicit_path).strip()
            fmt = meta.get("format") or source.get("format") or self._format_from_path(path)
            return {
                "physical_table": None,
                "file_path": path,
                "format": fmt,
                "location_type": "file",
                "location_resolution": "Resolved from explicit XML/parser file metadata.",
                "source_resolution_method": "explicit_file_metadata",
            }

        value = (
            source.get("physical_table")
            or source.get("table")
            or meta.get("physical_table")
            or supplied
            or logical_name
        )
        if value is None or not str(value).strip():
            return {
                "physical_table": None,
                "file_path": None,
                "location_type": "unresolved",
                "location_resolution": "No source object, table, or file path was declared.",
                "source_resolution_method": "unresolved",
            }

        value = str(value).strip()
        if self._looks_like_file_path(value):
            fmt = meta.get("format") or source.get("format") or self._format_from_path(value)
            return {
                "physical_table": None,
                "file_path": value,
                "format": fmt,
                "location_type": "file",
                "location_resolution": "Resolved from source object path.",
                "source_resolution_method": "source_path",
            }

        parts = [part.strip() for part in value.split(".")]
        if len(parts) == 3 and all(parts):
            return {
                "physical_table": value,
                "file_path": None,
                "location_type": "table",
                "location_resolution": "Preserved fully qualified catalog.schema.table from metadata.",
                "source_resolution_method": "fully_qualified_metadata",
            }

        catalog = meta.get("catalog") or source.get("catalog") or self.source_catalog
        schema = meta.get("schema") or source.get("schema")

        if len(parts) == 2 and all(parts):
            # schema.table; an explicitly supplied schema is authoritative.
            schema, table_name = parts
            physical = f"{catalog}.{schema}.{table_name}"
            return {
                "physical_table": physical,
                "file_path": None,
                "location_type": "table",
                "location_resolution": "Resolved schema.table using configured catalog.",
                "source_resolution_method": "schema_qualified_metadata",
            }

        if len(parts) != 1 or not parts[0]:
            return {
                "physical_table": None,
                "file_path": None,
                "location_type": "unresolved",
                "location_resolution": f"Unsupported source identifier: {value!r}",
                "source_resolution_method": "unresolved",
            }

        table_name = parts[0]
        registry_key = self._norm(logical_name or table_name)
        table_key = self._norm(table_name)

        # An explicit schema from XML/parser always wins.
        if schema:
            return {
                "physical_table": f"{catalog}.{schema}.{table_name}",
                "file_path": None,
                "location_type": "table",
                "location_resolution": "Resolved using schema explicitly declared in XML/parser metadata.",
                "source_resolution_method": "explicit_schema",
            }

        # Do not guess where a known duplicate table name lives.
        ambiguous_schemas = (
            self.ambiguous_source_schemas.get(registry_key)
            or self.ambiguous_source_schemas.get(table_key)
        )
        if ambiguous_schemas:
            return {
                "physical_table": None,
                "file_path": None,
                "location_type": "unresolved",
                "location_resolution": (
                    f"Ambiguous source {table_name!r}; it exists or may exist in "
                    f"multiple schemas {ambiguous_schemas}. Declare schema/catalog "
                    "in XML or provide an explicit registry entry."
                ),
                "source_resolution_method": "ambiguous",
                "candidate_schemas": ambiguous_schemas,
            }

        registry_schema = (
            self.source_schema_registry.get(registry_key)
            or self.source_schema_registry.get(table_key)
        )
        if registry_schema:
            # Registry values may be a schema name or schema.table/catalog.schema.table.
            registry_parts = str(registry_schema).split(".")
            if len(registry_parts) == 3:
                physical = str(registry_schema)
            elif len(registry_parts) == 2:
                physical = f"{catalog}.{registry_parts[0]}.{registry_parts[1]}"
            else:
                physical = f"{catalog}.{registry_schema}.{table_name}"
            return {
                "physical_table": physical,
                "file_path": None,
                "location_type": "table",
                "location_resolution": "Resolved using configurable logical-source schema registry.",
                "source_resolution_method": "schema_registry",
            }

        # Preserve backwards compatibility for otherwise unregistered sources.
        fallback_schema = self.source_schema
        return {
            "physical_table": f"{catalog}.{fallback_schema}.{table_name}",
            "file_path": None,
            "location_type": "table",
            "location_resolution": (
                "Resolved using fallback source_schema. Confirm the table exists; "
                "add an explicit schema registry entry if it belongs elsewhere."
            ),
            "source_resolution_method": "fallback_schema",
        }

    def _build_targets(self, mapping, xml_info):
        result = []
        xml_targets = xml_info.get("target_metadata", {})
        originals = mapping.get("target_instances", mapping.get("targets", [])) or []

        for original in originals:
            target = self._clean_metadata(original)
            logical = (
                target.get("target") or target.get("name")
                or target.get("object") or target.get("target_name")
            )
            target["logical_name"] = logical
            supplied = (
                target.get("physical_table") or target.get("table")
                or target.get("object") or logical
            )
            target.update(self._resolve_location(
                supplied, target.get("catalog") or self.target_catalog,
                target.get("schema") or self.target_schema,
            ))

            lookup_names = [
                target.get("name"), target.get("target"), target.get("object"),
                target.get("table"), target.get("target_name"),
                target.get("logical_name"),
            ]
            if target.get("physical_table"):
                lookup_names.append(str(target["physical_table"]).split(".")[-1])
            xml_target = self._match_target(lookup_names, xml_targets)
            if xml_target:
                target["xml_metadata"] = self._clean_metadata(xml_target)
                xml_columns = xml_target.get("columns") or []
                if xml_columns:
                    target["columns"] = self._merge_named_records(
                        target.get("columns") or target.get("fields") or [],
                        xml_columns,
                    )
                target.setdefault("columns", target.get("fields") or [])
                xml_keys = xml_target.get("merge_keys") or []
                if xml_keys:
                    target["merge_keys"] = list(dict.fromkeys(
                        list(target.get("merge_keys") or []) + list(xml_keys)
                    ))
                else:
                    target.setdefault("merge_keys", [])
                target["operation"] = (
                    target.get("operation") or xml_target.get("operation")
                )
            else:
                target.setdefault("columns", target.get("fields") or [])
                target.setdefault("merge_keys", [])
            result.append(target)
        return result

    def _build_transformations(self, mapping, xml_info):
        result = []
        xml_trans = xml_info.get("transformation_metadata", {})
        for original in mapping.get("transformations", []) or []:
            trans = self._clean_metadata(original)
            name = trans.get("name") or trans.get("id")
            xml_item = self._match_key([name, trans.get("id")], xml_trans)
            if xml_item:
                metadata = self._clean_metadata(xml_trans[xml_item])
                trans["xml_metadata"] = metadata
                if metadata.get("data_quality_rules"):
                    trans["data_quality_rules"] = self._merge_rule_records(
                        trans.get("data_quality_rules") or [],
                        metadata["data_quality_rules"],
                    )
            result.append(trans)
        return result

    def _build_links(self, mapping, graph):
        edges = graph.get("edges", []) if isinstance(graph, dict) else []
        if not edges:
            edges = mapping.get("links", []) or []
        return self._clean_metadata(edges)

    def _build_graph_metadata(self, graph):
        graph = graph if isinstance(graph, dict) else {}
        nodes = graph.get("nodes", []) or []
        edges = graph.get("edges", []) or []
        return {
            "nodes": self._clean_metadata(nodes),
            "edges": self._clean_metadata(edges),
            "execution_order": self._clean_metadata(
                graph.get("execution_order", []) or []
            ),
            "node_count": len(nodes),
            "edge_count": len(edges),
        }

    def _identify_blockers(self, sources, targets, transformations, xml_info, links):
        blockers = []
        for source in sources:
            name = source.get("logical_name") or source.get("name") or "UNKNOWN"
            if source.get("location_type") == "unresolved":
                blockers.append(f"Source {name} has no resolvable physical location.")
            if source.get("location_type") == "file" and not source.get("format"):
                blockers.append(f"File source {name} has no recognized format.")
            if source.get("location_type") == "file" and not (
                source.get("columns") or source.get("fields")
            ):
                blockers.append(
                    f"File source {name} has no declared source-column schema; "
                    "verify the physical file schema before generation."
                )
            if source.get("location_type") == "file" and str(source.get("format") or "").casefold() == "csv":
                options = source.get("options", {})
                if not isinstance(options, dict) or not options:
                    blockers.append(
                        f"CSV source {name} has no read options; confirm header "
                        "and delimiter from the file contract."
                    )

        for target in targets:
            name = target.get("physical_table") or target.get("logical_name") or "UNKNOWN"
            operation = str(target.get("operation") or "INSERT").upper()
            if operation in {"UPSERT", "MERGE"} and not target.get("merge_keys"):
                blockers.append(
                    f"Target {name} uses {operation} but no merge keys are declared."
                )
            if not target.get("columns"):
                blockers.append(f"Target {name} has no target columns/schema declared.")

        for trans in transformations:
            trans_type = self._norm(trans.get("type") or trans.get("transformation_type"))
            trans_name = trans.get("name") or trans.get("id") or "UNKNOWN"
            metadata = trans.get("xml_metadata") or {}
            xml = metadata.get("xml") or {}
            details = metadata.get("xml_details") or []
            if "router" in trans_type:
                if not self._contains_xml_element(xml or details, {
                    "condition", "groupcondition", "filtercondition", "expression"
                }):
                    blockers.append(
                        f"Router {trans_name} has no preserved routing condition in XML."
                    )
            if "dataquality" in trans_type or trans_type in {"dq", "quality"}:
                rules = trans.get("data_quality_rules") or []
                if not rules:
                    blockers.append(
                        f"Data-quality transformation {trans_name} has no resolved rule references."
                    )
                else:
                    missing = [
                        str(rule.get("id") or rule.get("name") or "UNKNOWN_RULE")
                        for rule in rules if isinstance(rule, dict)
                        and not str(rule.get("expression") or "").strip()
                    ]
                    if missing:
                        blockers.append(
                            f"Data-quality transformation {trans_name} has rules "
                            "without resolved expressions: " + ", ".join(missing)
                        )

        if not xml_info.get("xml_available"):
            blockers.append(xml_info.get(
                "xml_warning", "XML metadata could not be loaded by InputBuilder."
            ))
        if not links and len(sources) + len(transformations) + len(targets) > 1:
            blockers.append(
                "Mapping graph has no links; transformation dependencies cannot be verified."
            )
        return list(dict.fromkeys(blockers))

    @classmethod
    def _contains_xml_element(cls, value, wanted):
        if isinstance(value, dict):
            if cls._norm(value.get("tag")) in wanted:
                return True
            return any(cls._contains_xml_element(v, wanted) for v in value.values())
        if isinstance(value, list):
            return any(cls._contains_xml_element(v, wanted) for v in value)
        return False

    @classmethod
    def _match_key(cls, candidates, dictionary):
        if not isinstance(dictionary, dict):
            return None
        for candidate in candidates:
            if candidate is None:
                continue
            candidate_norm = cls._norm(candidate)
            for key in dictionary:
                if cls._norm(key) == candidate_norm and candidate_norm:
                    return key
        return None

    @classmethod
    def _match_target(cls, candidates, target_dictionary):
        key = cls._match_key(candidates, target_dictionary)
        if key is None:
            return {}
        candidates_for_name = target_dictionary[key]
        if isinstance(candidates_for_name, dict):
            return candidates_for_name
        if not isinstance(candidates_for_name, list) or not candidates_for_name:
            return {}
        # Prefer the richest declared schema, then the one with merge-key metadata.
        return max(
            candidates_for_name,
            key=lambda item: (
                len(item.get("columns") or []),
                len(item.get("merge_keys") or []),
            ),
        )

    @classmethod
    def _merge_named_records(cls, existing, incoming):
        """Merge records by name; incoming XML attributes enrich matching fields."""
        if not isinstance(existing, list):
            existing = []
        if not isinstance(incoming, list):
            incoming = []
        merged = []
        positions = {}
        for item in existing + incoming:
            if not isinstance(item, dict):
                continue
            clean = cls._clean_metadata(item)
            name = clean.get("name") or clean.get("field") or clean.get("column")
            key = cls._norm(name)
            if key and key in positions:
                current = merged[positions[key]]
                current.update({k: v for k, v in clean.items() if v not in (None, "")})
            else:
                if key:
                    positions[key] = len(merged)
                merged.append(clean)
        return merged

    @classmethod
    def _merge_rule_records(cls, existing, incoming):
        merged = []
        seen = set()
        for rule in list(existing or []) + list(incoming or []):
            if not isinstance(rule, dict):
                continue
            key = str(rule.get("id") or rule.get("name") or repr(rule))
            if key in seen:
                # XML record is later and authoritative for expression/details.
                index = next(
                    (i for i, old in enumerate(merged)
                     if str(old.get("id") or old.get("name") or repr(old)) == key),
                    None,
                )
                if index is not None:
                    merged[index].update(cls._clean_metadata(rule))
            else:
                seen.add(key)
                merged.append(cls._clean_metadata(rule))
        return merged

    @staticmethod
    def _format_from_path(path):
        lower = str(path or "").lower().split("?", 1)[0]
        for extension, fmt in (
            (".csv", "csv"), (".jsonl", "json"), (".json", "json"),
            (".parquet", "parquet"), (".avro", "avro"), (".orc", "orc"),
            (".txt", "text"), (".xml", "xml"),
        ):
            if lower.endswith(extension):
                return fmt
        return None

    @staticmethod
    def _looks_like_file_path(value):
        if not isinstance(value, str):
            return False
        value = value.strip()
        lower = value.lower()
        return (
            lower.endswith((
                ".csv", ".json", ".jsonl", ".parquet", ".avro", ".orc",
                ".txt", ".xml", ".xlsx", ".xls",
            ))
            or lower.startswith((
                "dbfs:/", "abfss://", "s3://", "wasbs://", "gs://",
                "file:/", "file://", "/",
            ))
            or "/" in value or "\\" in value
        )

    def _resolve_location(self, value, catalog, default_schema):
        result = {
            "physical_table": None,
            "file_path": None,
            "location_type": "unresolved",
            "location_resolution": None,
        }
        if value is None or not str(value).strip():
            result["location_resolution"] = "No location was supplied."
            return result

        name = str(value).strip()
        if self._looks_like_file_path(name):
            result.update(
                file_path=name,
                location_type="file",
                location_resolution="Preserved as file path; not a table identifier.",
            )
            return result

        parts = [part.strip() for part in name.split(".")]
        if any(not part for part in parts):
            result["location_resolution"] = f"Invalid dotted identifier: {name!r}"
        elif len(parts) == 1:
            result.update(
                physical_table=f"{catalog}.{default_schema}.{parts[0]}",
                location_type="table",
                location_resolution="Resolved using configured catalog/schema.",
            )
        elif len(parts) == 2:
            result.update(
                physical_table=f"{catalog}.{parts[0]}.{parts[1]}",
                location_type="table",
                location_resolution="Interpreted as schema.table and prefixed with catalog.",
            )
        elif len(parts) == 3:
            result.update(
                physical_table=name,
                location_type="table",
                location_resolution="Preserved catalog.schema.table identifier.",
            )
        else:
            result["location_resolution"] = f"Unsupported identifier: {name!r}"
        return result

    @classmethod
    def _clean_metadata(cls, value):
        if isinstance(value, dict):
            return {str(k): cls._clean_metadata(v) for k, v in value.items()}
        if isinstance(value, list):
            return [cls._clean_metadata(v) for v in value]
        if isinstance(value, (tuple, set)):
            return [cls._clean_metadata(v) for v in value]
        if value is None or isinstance(value, (str, int, float, bool)):
            return value
        return str(value)
