import ast
import io
import re
import tokenize


class LLMPySparkValidator:
    """
    Validator specifically for LLM-generated Databricks PySpark.

    Validation includes:
        1. Python syntax
        2. Spark usage
        3. Source table references
        4. Physical source-column validation
        5. Target table reference
        6. Target-column coverage
        7. Basic join-safety checks
        8. Required transformation constructs
        9. Dangerous-operation checks

    The validator does not depend on IR.

    Important:
        Physical source schemas are treated as authoritative for
        executable PySpark. This prevents the LLM from generating
        references to logical/metadata columns that do not exist in
        the actual Bronze Delta tables.
    """

    def __init__(self, generated_code, llm_input, spark=None):
        self.generated_code = generated_code or ""
        self.llm_input = llm_input or {}
        self.spark = spark

        self.errors = []
        self.warnings = []
        self._tree = None

        # Cache physical schemas so each source table is inspected once.
        self._physical_source_schemas = {}

    # ============================================================
    # MAIN VALIDATION
    # ============================================================

    def validate(self):
        self.errors = []
        self.warnings = []
        self._tree = None
        self._physical_source_schemas = {}

        if not self.generated_code.strip():
            self.errors.append("Generated PySpark code is empty.")
            return self._build_result()

        # 1. Python syntax
        self._validate_python_syntax()

        if self.errors:
            return self._build_result()

        # 2. Spark usage
        self._validate_spark_usage()

        # 3. Source tables
        self._validate_source_tables()

        # 4. Physical source columns
        self._validate_source_columns()

        # 5. Target table
        self._validate_target_table()

        # 6. Target schema
        self._validate_target_schema()

        # 7. Join safety
        self._validate_join_safety()

        # 8. Explicit blocker comments (metadata gaps must never be ignored)
        self._validate_blocked_comments()

        # 9. Transformations
        self._validate_transformations()

        # 10. Dangerous operations
        self._validate_dangerous_operations()

        # 11. Mapping-aware semantic safety checks. Keep these in the validator,
        # not only in the generator, so the pipeline's final gate cannot disagree
        # with the generator's repair-loop verdict.
        self._validate_semantic_safety()

        # 12. Metadata blockers from the input builder (e.g. unresolved DQ rule).
        # Do not let generated comments conceal an unresolved migration requirement.
        existing = set(self.errors)
        for blocker in self.llm_input.get("blocking_issues", []) or []:
            blocker_text = str(blocker)
            if self._is_resolved_approved_poc_merge_key_blocker(blocker_text):
                continue
            message = "Metadata blocker: " + blocker_text
            if message not in existing:
                self.errors.append(message)
                existing.add(message)

        return self._build_result()

    def _is_resolved_approved_poc_merge_key_blocker(self, blocker_text):
        """Ignore only stale missing-key blockers superseded by explicit POC approval."""
        text = str(blocker_text or "").casefold()
        if "merge key" not in text:
            return False
        missing_key_language = any(token in text for token in (
            "no merge keys", "does not define merge keys",
            "merge keys are not declared", "without merge keys",
            "has no merge keys", "uses upsert but no merge keys",
        ))
        if not missing_key_language:
            return False
        override = self.llm_input.get("approved_poc_overrides", {})
        if not isinstance(override, dict) or override.get("approved_by_user") is not True:
            return False
        if str(override.get("mapping_name") or "").casefold() != "m_customer_360_enrichment":
            return False
        approved_names = set()
        for assumption in override.get("assumptions", []) or []:
            if isinstance(assumption, dict):
                approved_names.update(str(name).casefold() for name in (assumption.get("targets", []) or []))
        return any(name in text for name in approved_names)

    # ============================================================
    # PYTHON SYNTAX
    # ============================================================

    def _validate_python_syntax(self):
        try:
            self._tree = ast.parse(
                self.generated_code,
                filename="<llm_generated_pyspark>"
            )
        except SyntaxError as error:
            self.errors.append(
                "Python syntax error: "
                f"{error.msg} "
                f"(line {error.lineno})"
            )

    # ============================================================
    # SPARK USAGE
    # ============================================================

    def _validate_spark_usage(self):
        if "spark" not in self.generated_code:
            self.errors.append(
                "Generated code does not reference Spark."
            )

        if not self._get_spark_table_reads():
            self.warnings.append(
                "No Spark table-read call was detected."
            )

        # Target writes are validated per target in _validate_target_table().
        # Avoid a duplicate global write error; the per-target errors name the
        # exact table and required operation.

    # ============================================================
    # SOURCE TABLE VALIDATION
    # ============================================================

    def _validate_source_tables(self):
        sources = self.llm_input.get("sources", [])
        if not sources:
            self.warnings.append("No source metadata was provided to validator.")
            return

        generated_tables = set(self._get_spark_table_reads())
        generated_file_paths = set(self._get_loaded_file_paths())

        for source in sources:
            if not isinstance(source, dict):
                continue

            physical_table = source.get("physical_table")
            location_type = str(source.get("location_type") or "").lower()
            file_path = source.get("file_path")
            logical_name = (
                source.get("logical_name") or source.get("source")
                or source.get("name") or "UNKNOWN"
            )

            if location_type == "file" or file_path:
                resolved_path = self._resolve_file_path(source)
                if not resolved_path:
                    self.errors.append(
                        f"File source '{logical_name}' has relative path "
                        f"'{file_path}' but no source_file_base_path is configured. "
                        "Configure databricks.source_file_base_path in LLM input."
                    )
                    continue

                file_format = self._source_file_format(source, resolved_path)
                if not file_format:
                    self.errors.append(
                        f"File source '{logical_name}' has no explicit file format. "
                        "Provide file_format/format metadata or configure it explicitly."
                    )
                    continue

                if resolved_path not in generated_file_paths:
                    self.errors.append(
                        f"File source '{logical_name}' was not read from resolved path "
                        f"'{resolved_path}'. Use spark.read.format('{file_format}').load(...)."
                    )
                continue

            if not physical_table:
                self.errors.append(
                    f"Source '{logical_name}' has no resolved physical_table and is not "
                    "a resolvable file source."
                )
                continue

            if physical_table not in generated_tables:
                self.errors.append(
                    f"Source table '{physical_table}' was not referenced by generated PySpark."
                )

    def _resolve_file_path(self, source):
        # InputBuilder resolves relative XML paths once; prefer its absolute path.
        path = (
            source.get("resolved_file_path")
            or source.get("file_path")
            or source.get("path")
        )
        if not isinstance(path, str) or not path.strip():
            return None
        path = path.strip()
        if (
            path.startswith("/")
            or path.startswith("dbfs:/")
            or path.startswith("s3://")
            or path.startswith("abfss://")
            or path.startswith("wasbs://")
            or path.startswith("gs://")
            or path.startswith("file:/")
        ):
            return path

        databricks = self.llm_input.get("databricks", {})
        base_path = (
            databricks.get("source_file_base_path")
            if isinstance(databricks, dict) else None
        ) or self.llm_input.get("source_file_base_path")
        if not base_path:
            return None
        return str(base_path).rstrip("/") + "/" + path.lstrip("/")

    @staticmethod
    def _source_file_format(source, resolved_path):
        databricks = source.get("format") or source.get("file_format")
        if databricks:
            return str(databricks).lower()
        config = source.get("options")
        if isinstance(config, dict) and config.get("format"):
            return str(config["format"]).lower()
        # A file extension is a direct format hint, not a business-rule guess.
        lower_path = resolved_path.lower().split("?", 1)[0]
        for extension, file_format in (
            (".csv", "csv"), (".json", "json"), (".parquet", "parquet"),
            (".orc", "orc"), (".avro", "avro"), (".txt", "text")
        ):
            if lower_path.endswith(extension):
                return file_format
        return None

    # ============================================================
    # PHYSICAL SOURCE COLUMN VALIDATION
    # ============================================================

    def _validate_source_columns(self):
        """
        Validate explicit source metadata columns against their corresponding
        physical source schemas. Do not compare every F.col() in generated code
        against raw source schemas: many references are derived columns created
        by earlier transformations, so global AST checks create false positives.
        """
        if self.spark is None:
            self.warnings.append(
                "Spark session was not provided; physical source-schema validation was skipped."
            )
            return

        for source in self.llm_input.get("sources", []):
            if not isinstance(source, dict):
                continue

            location_type = str(source.get("location_type") or "").lower()
            file_path = source.get("file_path")
            physical_table = source.get("physical_table")

            try:
                if location_type == "file" or file_path:
                    resolved_path = self._resolve_file_path(source)
                    file_format = self._source_file_format(source, resolved_path or "")
                    if not resolved_path or not file_format:
                        continue
                    reader = self.spark.read.format(file_format)
                    options = source.get("options", {})
                    options = dict(options) if isinstance(options, dict) else {}
                    for key, value in options.items():
                        if key != "format":
                            reader = reader.option(str(key), str(value))
                    df = reader.load(resolved_path)
                    label = resolved_path
                elif physical_table:
                    df = self.spark.table(str(physical_table))
                    label = str(physical_table)
                else:
                    continue
            except Exception as error:
                # Source-table existence/readability is an actionable validation error.
                self.errors.append(
                    f"Unable to inspect source schema '{physical_table or file_path}': {error}"
                )
                continue

            actual_columns = {str(column).casefold() for column in df.columns}
            declared_columns = self._metadata_source_columns(source)
            if not declared_columns:
                self.warnings.append(
                    f"No explicit source-column metadata for '{label}'; "
                    "raw-source column lineage check was skipped."
                )
                continue

            missing = [
                column for column in declared_columns
                if column.casefold() not in actual_columns
            ]
            if missing:
                self.errors.append(
                    f"Source schema mismatch for '{label}': metadata columns not found "
                    "in the physical source: " + ", ".join(missing)
                )

    @staticmethod
    def _metadata_source_columns(source):
        names = []
        for key in ("columns", "ports", "fields", "attributes"):
            values = source.get(key, [])
            if not isinstance(values, list):
                continue
            for value in values:
                if isinstance(value, dict):
                    name = (
                        value.get("name") or value.get("column_name")
                        or value.get("port_name") or value.get("field_name")
                    )
                else:
                    name = value if isinstance(value, str) else None
                if name and str(name) not in names:
                    names.append(str(name))
        return names

    def _get_loaded_file_paths(self):
        """Return literal paths passed to load(), resolving simple path variables.

        Supports code such as:
            customer_path = "/Volumes/.../customer.csv"
            spark.read.format("csv").load(customer_path)

        This is intentionally limited to direct string assignments; it does not
        execute generated code or evaluate arbitrary expressions.
        """
        if self._tree is None:
            return []

        string_assignments = {}
        for node in ast.walk(self._tree):
            if isinstance(node, ast.Assign) and isinstance(node.value, ast.Constant):
                if isinstance(node.value.value, str):
                    for target in node.targets:
                        if isinstance(target, ast.Name):
                            string_assignments[target.id] = node.value.value
            elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
                if isinstance(node.value, ast.Constant) and isinstance(node.value.value, str):
                    string_assignments[node.target.id] = node.value.value

        paths = []
        for node in ast.walk(self._tree):
            if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
                continue
            if node.func.attr != "load" or not node.args:
                continue
            arg = node.args[0]
            if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
                paths.append(arg.value)
            elif isinstance(arg, ast.Name) and arg.id in string_assignments:
                paths.append(string_assignments[arg.id])

        return list(dict.fromkeys(paths))

    # ============================================================
    # TARGET TABLE VALIDATION
    # ============================================================

    def _validate_target_table(self):
        targets = self.llm_input.get("targets", [])
        if not targets:
            self.errors.append("No target metadata was provided to validator.")
            return

        generated_save_targets = set(self._get_save_as_table_targets())
        generated_merge_targets = set(self._get_delta_merge_targets())
        code_upper = self.generated_code.upper()
        uses_overwrite = bool(re.search(r"\.mode\(\s*['\"]overwrite['\"]\s*\)", self.generated_code, re.IGNORECASE))

        for target in targets:
            if not isinstance(target, dict):
                continue

            physical_table = target.get("physical_table")
            logical_name = (
                target.get("logical_name") or target.get("target")
                or target.get("name") or "UNKNOWN"
            )
            operation = str(target.get("operation") or "INSERT").upper()

            if not physical_table:
                self.errors.append(
                    f"Target '{logical_name}' does not have physical_table metadata."
                )
                continue

            if operation in {"UPSERT", "MERGE"}:
                key_status = self._target_merge_key_status(target)
                unapproved_key_status = key_status in {
                    "POC_ASSUMPTION", "ASSUMED", "UNVERIFIED", "UNKNOWN"
                }
                approved_poc_key_status = key_status == "APPROVED_POC_ASSUMPTION"
                has_merge_keys = (
                    self._target_has_merge_keys(target)
                    and (not unapproved_key_status)
                    and (key_status != "APPROVED_POC_ASSUMPTION" or approved_poc_key_status)
                )
                
                if unapproved_key_status:
                    if physical_table in generated_merge_targets:
                        self.errors.append(
                            f"Target '{physical_table}' has merge_key_status={key_status}; "
                            "the key is not authoritative, so generated MERGE writes are blocked. "
                            "Verify the actual IDMC target key configuration before enabling writes."
                        )
                    else:
                        self.errors.append(
                            f"Target '{physical_table}' has merge_key_status={key_status}; "
                            "target write is blocked until the actual IDMC merge key is verified."
                        )

                # Missing keys are a metadata blocker. In that state, the
                # generated code is expected to omit MERGE rather than invent
                # a match condition. Report the missing-key blocker without
                # also reporting the intentionally omitted MERGE as a second
                # error for the same target.
                if has_merge_keys and physical_table not in generated_merge_targets:
                    self.errors.append(
                        f"Target '{physical_table}' operation is {operation}, but no "
                        "DeltaTable merge targeting this table was detected. "
                        "Generate the MERGE only after the authoritative merge keys are supplied."
                    )

                if uses_overwrite:
                    self.errors.append(
                        f"Generated code uses overwrite while target '{physical_table}' "
                        f"requires {operation}. This can destroy existing target rows."
                    )

                if not has_merge_keys and not unapproved_key_status:
                    self.errors.append(
                        f"Target '{physical_table}' requires {operation}, but authoritative merge-key "
                        "metadata is missing. Add the actual IDMC key columns before execution."
                    )

            if operation in {"UPSERT", "MERGE"}:
                merge_keys = target.get("merge_keys", [])
                target_columns = target.get("columns", [])
                if self._target_merge_key_status(target) in {"POC_ASSUMPTION", "ASSUMED", "UNVERIFIED", "UNKNOWN"}:
                    merge_keys = []
                if isinstance(merge_keys, str):
                    merge_keys = [key.strip() for key in merge_keys.split(",") if key.strip()]
                if isinstance(target_columns, list) and target_columns and merge_keys:
                    declared_names = {
                        str(column.get("name") or column.get("column_name") or "")
                        if isinstance(column, dict) else str(column)
                        for column in target_columns
                    }
                    missing_keys = [
                        str(key) for key in merge_keys
                        if str(key) not in declared_names
                    ]
                    if missing_keys:
                        self.errors.append(
                            f"Target '{physical_table}' declares merge keys not present "
                            "in its target-column metadata: " + ", ".join(missing_keys)
                        )

            elif operation in {"OVERWRITE", "REPLACE"}:
                if physical_table not in generated_save_targets:
                    self.errors.append(
                        f"Overwrite target '{physical_table}' was not written with saveAsTable()."
                    )
            else:
                if physical_table not in generated_save_targets and physical_table not in generated_merge_targets:
                    self.errors.append(
                        f"Target table '{physical_table}' was not written by generated PySpark."
                    )

    @staticmethod
    def _target_merge_key_status(target):
        values = []
        if isinstance(target, dict):
            values.append(target.get("merge_key_status"))
            attributes = target.get("attributes", {})
            if isinstance(attributes, dict):
                values.append(attributes.get("merge_key_status"))
        for value in values:
            if value is not None and str(value).strip():
                return str(value).strip().upper()
        return ""

    @staticmethod
    def _target_has_merge_keys(target):
        for key in ("merge_keys", "key_columns", "primary_key", "keys"):
            value = target.get(key)
            if isinstance(value, str) and value.strip():
                return True
            if isinstance(value, list) and value:
                return True
        attributes = target.get("attributes", {})
        if isinstance(attributes, dict):
            for key in ("merge_keys", "key_columns", "primary_key", "keys"):
                value = attributes.get(key)
                if isinstance(value, str) and value.strip():
                    return True
                if isinstance(value, list) and value:
                    return True
        return False

    def _get_delta_merge_targets(self):
        """Return targets written through a real DeltaTable.merge() chain.

        Resolves simple string variables, e.g. target_table = "catalog.schema.table",
        and DeltaTable variables, e.g. delta_tbl = DeltaTable.forName(spark, target_table).
        A DataFrame.alias(...).merge(...) call is deliberately NOT accepted because
        DataFrame.merge is not the Delta Lake write API.
        """
        if self._tree is None:
            return []

        assignments = {}
        for statement in ast.walk(self._tree):
            if isinstance(statement, ast.Assign):
                for target in statement.targets:
                    if isinstance(target, ast.Name):
                        assignments[target.id] = statement.value
            elif isinstance(statement, ast.AnnAssign) and isinstance(statement.target, ast.Name):
                if statement.value is not None:
                    assignments[statement.target.id] = statement.value

        def resolve_string(node, seen=None):
            seen = set() if seen is None else seen
            if isinstance(node, ast.Constant) and isinstance(node.value, str):
                return node.value
            if isinstance(node, ast.Name) and node.id not in seen:
                seen.add(node.id)
                return resolve_string(assignments.get(node.id), seen)
            return None

        def resolve_delta_for_name(node, seen=None):
            """Resolve an expression back to DeltaTable.forName(...), if any."""
            seen = set() if seen is None else seen
            if isinstance(node, ast.Name):
                if node.id in seen:
                    return None
                seen.add(node.id)
                return resolve_delta_for_name(assignments.get(node.id), seen)

            if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
                return None

            # .alias(...) preserves the underlying DeltaTable object.
            if node.func.attr == "alias":
                return resolve_delta_for_name(node.func.value, seen)

            if node.func.attr == "forName":
                owner = node.func.value
                if isinstance(owner, ast.Name) and owner.id == "DeltaTable" and len(node.args) >= 2:
                    target_name = resolve_string(node.args[1])
                    return target_name
            return None

        targets = []
        for node in ast.walk(self._tree):
            if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
                continue
            if node.func.attr != "merge":
                continue
            # Only count .merge() when its receiver resolves to DeltaTable.forName(...).
            target_name = resolve_delta_for_name(node.func.value)
            if target_name:
                targets.append(target_name)

        return list(dict.fromkeys(targets))

    # ============================================================
    # TARGET SCHEMA VALIDATION
    # ============================================================

    def _validate_target_schema(self):
        """
        Validate target columns independently for each target.

        The previous implementation combined columns from all targets into
        one list. With multiple targets, a column belonging to one target
        could incorrectly make another target appear complete. This version
        checks each target separately and never infers missing schema.
        """
        targets = self.llm_input.get("targets", [])
        if not targets:
            return

        # A target column may appear in generated code as a qualified name,
        # for example "cust.region_code", rather than as the standalone string
        # "region_code". Normalize string constants to include identifier tokens
        # and the final component of qualified names before comparing coverage.
        # This is a static coverage check only; it does not execute generated code.
        generated_string_constants = self._normalized_string_identifiers(
            self._get_string_constants()
        )

        for target in targets:
            if not isinstance(target, dict):
                continue

            logical_name = (
                target.get("logical_name")
                or target.get("target")
                or target.get("name")
                or "UNKNOWN"
            )
            physical_table = target.get("physical_table") or logical_name

            expected_columns = []
            for key in ("ports", "columns"):
                values = target.get(key, [])
                if not isinstance(values, list):
                    continue

                for value in values:
                    if isinstance(value, dict):
                        name = (
                            value.get("name")
                            or value.get("column_name")
                            or value.get("port_name")
                            or value.get("field_name")
                        )
                    else:
                        name = value if isinstance(value, str) else None

                    if name and str(name) not in expected_columns:
                        expected_columns.append(str(name))

            if not expected_columns:
                self.errors.append(
                    f"Target '{physical_table}' has no target-column metadata. "
                    "Declare the target schema in the XML or provide authoritative "
                    "catalog-enriched target columns before execution."
                )
                continue

            missing_columns = [
                column
                for column in expected_columns
                if str(column).casefold() not in generated_string_constants
            ]

            if missing_columns:
                self.errors.append(
                    f"Target schema mismatch for '{physical_table}': generated "
                    "PySpark does not contain these target columns: "
                    + ", ".join(missing_columns)
                )

    # ============================================================
    # JOIN SAFETY
    # ============================================================

    def _validate_join_safety(self):
        """
        Detect the common LLM failure where a boolean join condition
        is generated without qualified aliases.

        String-key joins such as on="CUSTOMER_ID" are allowed.

        Boolean joins should qualify columns when aliases are used.
        """

        if self._tree is None:
            return

        for node in ast.walk(self._tree):

            if not isinstance(node, ast.Call):
                continue

            if not isinstance(node.func, ast.Attribute):
                continue

            if node.func.attr != "join":
                continue

            join_condition = None

            if len(node.args) >= 2:
                join_condition = node.args[1]

            for keyword in node.keywords:
                if keyword.arg == "on":
                    join_condition = keyword.value

            if join_condition is None:
                continue

            # Simple string-key join is safe.
            if isinstance(join_condition, ast.Constant):
                if isinstance(join_condition.value, str):
                    continue

            unqualified_columns = []

            for child in ast.walk(join_condition):

                if not isinstance(child, ast.Call):
                    continue

                if not isinstance(child.func, ast.Attribute):
                    continue

                if child.func.attr != "col":
                    continue

                if not child.args:
                    continue

                argument = child.args[0]

                if not isinstance(argument, ast.Constant):
                    continue

                if not isinstance(argument.value, str):
                    continue

                column_name = argument.value

                if "." not in column_name:
                    unqualified_columns.append(column_name)

            if unqualified_columns:
                unique_columns = list(
                    dict.fromkeys(unqualified_columns)
                )

                self.errors.append(
                    "Join safety error: boolean join condition "
                    "uses unqualified F.col() references: "
                    + ", ".join(unique_columns)
                    + ". Use DataFrame aliases and qualified "
                    "references such as F.col('l.ID') == "
                    "F.col('r.ID')."
                )

    # ============================================================
    # AST HELPERS
    # ============================================================

    def _get_string_constants(self):
        if self._tree is None:
            return []

        values = []
        for node in ast.walk(self._tree):
            if isinstance(node, ast.Constant) and isinstance(node.value, str):
                values.append(node.value)
        return values

    @staticmethod
    def _normalized_string_identifiers(values):
        """Normalize column-like names from code string literals.

        Examples:
            "cust.region_code" -> includes "cust.region_code" and "region_code"
            "region_code"       -> includes "region_code"

        Identifier tokenization avoids treating qualified references as missing
        merely because the code uses DataFrame-qualified names in select/col calls.
        """
        normalized = set()
        for value in values or []:
            if not isinstance(value, str):
                continue
            text = value.strip()
            if not text:
                continue
            normalized.add(text.casefold())
            # Spark expressions often use qualified names like alias.column.
            for part in re.split(r"\.", text):
                part = part.strip().strip("`\"' ")
                if part and re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", part):
                    normalized.add(part.casefold())
            # Also recognize column identifiers embedded in simple expressions.
            for token in re.findall(r"[A-Za-z_][A-Za-z0-9_]*", text):
                normalized.add(token.casefold())
        return normalized

    def _get_spark_table_reads(self):
        """Return Spark table reads, resolving direct string variables.

        Supported examples:
            spark.table("catalog.schema.table")
            spark.table(table_name)  # table_name = "catalog.schema.table"
            spark.read.table(table_name)
            spark.read.format("delta").table(table_name)

        This is static AST analysis only. It does not execute generated code
        or evaluate arbitrary expressions.
        """
        if self._tree is None:
            return []

        # Resolve direct string assignments, including annotated assignments.
        # Keep the analysis intentionally conservative: only literal strings
        # are accepted; concatenations, function calls, and dynamic expressions
        # are not evaluated.
        string_assignments = {}
        for node in ast.walk(self._tree):
            if isinstance(node, ast.Assign):
                value = node.value
                if isinstance(value, ast.Constant) and isinstance(value.value, str):
                    for target in node.targets:
                        if isinstance(target, ast.Name):
                            string_assignments[target.id] = value.value
            elif isinstance(node, ast.AnnAssign):
                value = node.value
                if (
                    isinstance(node.target, ast.Name)
                    and isinstance(value, ast.Constant)
                    and isinstance(value.value, str)
                ):
                    string_assignments[node.target.id] = value.value

        tables = []
        for node in ast.walk(self._tree):
            table_name = self._extract_spark_table_read(
                node,
                string_assignments=string_assignments,
            )
            if table_name:
                tables.append(table_name)

        return list(dict.fromkeys(tables))

    def _extract_spark_table_read(self, node, string_assignments=None):
        """Extract a recognized Spark table read with a literal or simple variable."""
        if not isinstance(node, ast.Call):
            return None

        if not isinstance(node.func, ast.Attribute):
            return None

        if node.func.attr != "table" or not node.args:
            return None

        parent = node.func.value

        # Accept only Spark's supported table-read call shapes.
        if not (
            self._is_spark_name(parent)
            or self._is_spark_read(parent)
            or self._is_spark_read_format_call(parent)
        ):
            return None

        table_arg = node.args[0]

        # Existing literal form remains supported.
        if isinstance(table_arg, ast.Constant) and isinstance(table_arg.value, str):
            return table_arg.value

        # Resolve direct variable form, e.g. spark.read.table(src_table).
        if isinstance(table_arg, ast.Name):
            return (string_assignments or {}).get(table_arg.id)

        return None

    @staticmethod
    def _is_spark_name(node):
        return (
            isinstance(node, ast.Name)
            and node.id == "spark"
        )

    def _is_spark_read(self, node):
        if not isinstance(node, ast.Attribute):
            return False

        return (
            node.attr == "read"
            and self._is_spark_name(node.value)
        )

    def _is_spark_read_format_call(self, node):
        if not isinstance(node, ast.Call):
            return False

        if not isinstance(node.func, ast.Attribute):
            return False

        if node.func.attr != "format":
            return False

        return self._is_spark_read(node.func.value)

    def _get_save_as_table_targets(self):
        if self._tree is None:
            return []

        targets = []

        for node in ast.walk(self._tree):

            if not isinstance(node, ast.Call):
                continue

            if not isinstance(node.func, ast.Attribute):
                continue

            if node.func.attr != "saveAsTable":
                continue

            if not node.args:
                continue

            target_arg = node.args[0]

            if isinstance(target_arg, ast.Constant):
                if isinstance(target_arg.value, str):
                    targets.append(target_arg.value)

        return list(dict.fromkeys(targets))

    # ============================================================
    # TRANSFORMATION VALIDATION
    # ============================================================

    def _code_without_comments(self):
        """
        Return source with COMMENT tokens removed.

        This prevents explanatory comments such as a target MERGE example
        from making unrelated DATAQUALITY transformations look like
        placeholders. Executable writes are still checked independently by
        the AST-based target validation.
        """
        try:
            tokens = tokenize.generate_tokens(io.StringIO(self.generated_code).readline)
            filtered = (token for token in tokens if token.type != tokenize.COMMENT)
            return tokenize.untokenize(filtered)
        except (tokenize.TokenError, IndentationError):
            # Syntax validation runs first, but keep this helper defensive.
            return self.generated_code

    def _validate_blocked_comments(self):
        """
        A generated script may document unresolved metadata with '# BLOCKED:'.
        Such a script must not be considered executable even if its syntax is
        valid. Report these explicitly without attributing every placeholder
        comment to every DATAQUALITY transformation.
        """
        seen = set()
        for line in self.generated_code.splitlines():
            if re.search(r"^\s*#\s*BLOCKED\s*:", line, re.IGNORECASE):
                message = line.strip().lstrip("#").strip()
                if message and message not in seen:
                    self.errors.append(
                        "Generated code contains an unresolved blocker: " + message
                    )
                    seen.add(message)

    def _validate_transformations(self):
        """Validate that generated code contains enough operations for declared nodes.

        This is deliberately a static structural check, not proof of semantic
        equivalence. Counts are checked by transformation type so one join cannot
        silently satisfy several unrelated LOOKUP/JOINER nodes.
        """
        transformations = self.llm_input.get("transformations", [])
        if isinstance(transformations, dict):
            transformations = list(transformations.values())
        if not isinstance(transformations, list):
            transformations = []

        code = self._code_without_comments()
        code_lower = code.lower()
        placeholder_patterns = (
            "not implemented", "todo:", "todo(", "placeholder",
            "pass through unchanged", "passing through unchanged",
            "raise notimplementederror", "..."
        )
        for marker in placeholder_patterns:
            if marker in code_lower:
                self.errors.append(
                    f"Executable code contains incomplete/placeholder logic: '{marker}'."
                )

        try:
            tree = ast.parse(code)
        except SyntaxError:
            return

        method_counts = {}
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
                method = node.func.attr.casefold()
                method_counts[method] = method_counts.get(method, 0) + 1

        # Normalize parser variants (e.g. Joiner, JOINER, JoinerTransformation).
        aliases = {
            "filter": {"filter", "where"},
            "joiner": {"join"},
            "lookup": {"join"},
            "aggregator": {"groupby"},
            "expression": {"withcolumn", "selectexpr"},
            "router": {"filter", "where"},
            "dataquality": {"withcolumn", "filter", "where", "select", "selectexpr"},
            "storedprocedure": {"call", "sql", "saveastable"},
            "sorter": {"orderBy", "sort"},
            "rank": {"withcolumn", "select"},
            "union": {"union", "unionByName"},
            "distinct": {"distinct", "dropDuplicates"},
            "sequence": {"withcolumn", "select"},
        }
        required_by_type = {}
        display_by_type = {}
        for transformation in transformations:
            if not isinstance(transformation, dict):
                continue
            raw_type = str(
                transformation.get("type")
                or transformation.get("transformation_type")
                or transformation.get("name")
                or ""
            )
            normalized = re.sub(r"[^a-z]", "", raw_type.casefold())
            if normalized.endswith("transformation"):
                normalized = normalized[:-14]
            if normalized not in aliases:
                continue
            required_by_type[normalized] = required_by_type.get(normalized, 0) + 1
            display_by_type.setdefault(normalized, []).append(
                str(transformation.get("name") or raw_type or "UNKNOWN")
            )

        for transformation_type, required_count in required_by_type.items():
            accepted_methods = aliases[transformation_type]
            actual_count = sum(method_counts.get(method.casefold(), 0) for method in accepted_methods)
            if actual_count < required_count:
                names = ", ".join(display_by_type.get(transformation_type, []))
                self.errors.append(
                    f"Transformation coverage: {required_count} {transformation_type.upper()} "
                    f"node(s) ({names}) were declared, but only {actual_count} matching "
                    "PySpark operation(s) were detected. Review each node's implementation."
                )

        # Explicit BLOCKED comments remain a hard failure, but only when the
        # comment names a known transformation (or declares a general blocker).
        blocked_lines = [
            line.strip() for line in self.generated_code.splitlines()
            if re.search(r"^\s*#\s*BLOCKED\s*:", line, re.IGNORECASE)
        ]
        for line in blocked_lines:
            self.errors.append("Generated code contains an explicit blocker: " + line.lstrip("# "))

    # ============================================================
    # MAPPING-AWARE SEMANTIC SAFETY
    # ============================================================

    def _validate_semantic_safety(self):
        """Apply metadata-driven safeguards without mapping-specific column names."""
        transformations = self.llm_input.get("transformations", [])
        if isinstance(transformations, dict):
            transformations = list(transformations.values())
        if not isinstance(transformations, list):
            transformations = []
        targets = self.llm_input.get("targets", [])
        if not isinstance(targets, list):
            targets = []
        code = self._code_without_comments()

        sequence_nodes = [
            item for item in transformations
            if isinstance(item, dict)
            and "sequence" in str(
                item.get("type") or item.get("transformation_type") or item.get("name") or ""
            ).casefold()
        ]
        upsert_targets = [
            item for item in targets
            if isinstance(item, dict)
            and str(item.get("operation", "")).upper() in {"UPSERT", "MERGE"}
        ]
        if sequence_nodes and upsert_targets and re.search(
            r"\b(row_number|monotonically_increasing_id)\s*\(", code, re.I
        ):
            for target in upsert_targets:
                target_name = target.get("physical_table") or target.get("logical_name") or "UNKNOWN"
                merge_keys = target.get("merge_keys") or target.get("key_columns") or []
                if isinstance(merge_keys, str):
                    merge_keys = [part.strip() for part in re.split(r"[,;|]", merge_keys) if part.strip()]
                if not merge_keys and self._target_merge_key_status(target) != "APPROVED_POC_ASSUMPTION":
                    self.errors.append(
                        f"Semantic safety: sequence-generated key feeds UPSERT/MERGE target "
                        f"'{target_name}', but authoritative merge keys are missing."
                    )
                    continue

                # Sequence output field must come from the transformation metadata
                # (ports/output fields/attributes), never from a fixed column name.
                sequence_fields = set()
                for node in sequence_nodes:
                    for key in ("sequence_field", "output_field", "surrogate_key", "field_name"):
                        value = node.get(key)
                        if isinstance(value, str) and value.strip():
                            sequence_fields.add(value.strip())
                    for collection_key in ("ports", "output_ports", "fields", "xml_metadata"):
                        value = node.get(collection_key)
                        sequence_fields.update(self._collect_field_names(value))
                if not sequence_fields:
                    self.errors.append(
                        "Semantic safety: sequence transformation feeds an UPSERT/MERGE, "
                        "but its generated key field is not explicitly identified in metadata."
                    )
                    continue

                key_pattern = "|".join(re.escape(key) for key in sorted(sequence_fields, key=len, reverse=True))
                business_keys = {
                    str(node.get("business_key")).strip()
                    for node in sequence_nodes
                    if isinstance(node.get("business_key"), str) and node.get("business_key").strip()
                }
                has_existing_key_lookup = bool(
                    re.search(r"spark\.table\s*\(", code)
                    and re.search(key_pattern, code, re.I)
                    and re.search(r"\b(existing|matched|target|current)\b", code, re.I)
                    and re.search(r"\.join\s*\(", code)
                    and re.search(r"how\s*=\s*['\"]left['\"]|,\s*['\"]left['\"]\s*\)", code, re.I)
                    and re.search(r"\b(coalesce|when)\s*\(", code, re.I)
                    and re.search(r"\b(isNull|isNotNull|isnull|isnotnull)\s*\(", code, re.I)
                    and re.search(r"\.(filter|where)\s*\(", code, re.I)
                    and (not business_keys or all(re.search(rf"\b{re.escape(key)}\b", code) for key in business_keys))
                )
                if not has_existing_key_lookup:
                    self.errors.append(
                        f"Semantic safety: sequence transformation(s) feed UPSERT/MERGE target "
                        f"'{target_name}', but code does not demonstrate reuse of existing "
                        "surrogate keys before allocating new ones."
                    )

        # DQ handling is only enforceable when XML supplies a disposition/action.
        # Do not assume a particular status-column naming convention or that every
        # quality rule must filter rows; preserve the exact rule metadata for review.
        for node in transformations:
            if not isinstance(node, dict):
                continue
            node_type = str(node.get("type") or node.get("transformation_type") or "").casefold()
            if "dataquality" not in node_type and node_type not in {"dq", "quality"}:
                continue
            rules = node.get("data_quality_rules") or []
            for rule in rules:
                if not isinstance(rule, dict):
                    continue
                action = str(
                    rule.get("failure_action") or rule.get("action")
                    or rule.get("disposition") or ""
                ).strip().casefold()
                if action in {"reject", "quarantine", "discard", "filter", "drop"}:
                    # Require some row-disposition operation, but don't guess the
                    # rule's output status column or exact expression.
                    if not any(token in code for token in (".filter(", ".where(", ".join(")):
                        self.errors.append(
                            f"Semantic safety: DQ rule '{rule.get('name') or rule.get('id') or 'UNKNOWN'}' "
                            f"requires '{action}' disposition, but no corresponding filtering/join "
                            "operation was detected."
                        )

    @classmethod
    def _collect_field_names(cls, value):
        names = set()
        if isinstance(value, dict):
            for key, item in value.items():
                if key in {"name", "field_name", "column_name", "port_name"} and isinstance(item, str):
                    names.add(item)
                names.update(cls._collect_field_names(item))
        elif isinstance(value, list):
            for item in value:
                names.update(cls._collect_field_names(item))
        return names

    # ============================================================
    # DANGEROUS OPERATION VALIDATION
    # ============================================================

    def _validate_dangerous_operations(self):
        dangerous_patterns = {
            "os.system(": "os.system() execution detected.",
            "subprocess.": "subprocess operation detected.",
            "shutil.rmtree(": "shutil.rmtree() deletion detected.",
            "dbutils.fs.rm(": "dbutils.fs.rm() deletion detected.",
            "DROP TABLE": "DROP TABLE operation detected.",
            "DROP DATABASE": "DROP DATABASE operation detected.",
            "TRUNCATE TABLE": "TRUNCATE TABLE operation detected."
        }

        code_upper = self.generated_code.upper()

        for pattern, message in dangerous_patterns.items():
            if pattern.upper() in code_upper:
                self.errors.append(message)

    # ============================================================
    # RESULT
    # ============================================================

    def _build_result(self):
        return {
            "valid": len(self.errors) == 0,
            "errors": self.errors,
            "warnings": self.warnings
        }

    # ============================================================
    # PRINT REPORT
    # ============================================================

    def print_report(self, result):
        print("\nLLM PYSPARK VALIDATION")
        print("-" * 70)

        print(
            "Status :",
            "VALID" if result["valid"] else "INVALID"
        )

        print(
            "Errors :",
            len(result["errors"])
        )

        print(
            "Warnings :",
            len(result["warnings"])
        )

        if result["errors"]:
            print("\nLLM PYSPARK ERRORS")

            for error in result["errors"]:
                print("  ERROR:", error)

        if result["warnings"]:
            print("\nLLM PYSPARK WARNINGS")

            for warning in result["warnings"]:
                print("  WARNING:", warning)
