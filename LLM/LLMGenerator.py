import ast

import json

import re

from openai import OpenAI

class LLMGenerator:

    """

    Generates an IDMC -> Databricks migration plan and executable

    PySpark using Databricks GPT OSS.
    This class is LLM-only.
    """



    def __init__(

        self,

        workspace_url,

        databricks_token,

        model="databricks-gpt-oss-120b",

        max_tokens=10000,

        temperature=0

    ):

        self.workspace_url = workspace_url.rstrip("/")

        self.databricks_token = databricks_token

        self.model = model

        self.max_tokens = max_tokens

        self.temperature = temperature



        self.client = OpenAI(

            api_key=self.databricks_token,

            base_url=f"{self.workspace_url}/serving-endpoints"

        )



    # ============================================================

    # GENERATE

    # ============================================================



    def generate(self, llm_input, validator_factory=None, max_repair_attempts=2):
        """Generate PySpark and concise metadata in separate LLM calls.

        Splitting the response prevents the code from competing with lengthy
        migration-plan and transformation-summary JSON for the same output-token
        budget. PySpark itself remains entirely LLM-generated.
        """
        code_prompt = self._build_split_prompt(llm_input, mode="code")
        code_result = None
        code_error = None

        # One compact retry is allowed for malformed/truncated code JSON.
        for attempt in range(2):
            current_prompt = code_prompt
            if attempt:
                current_prompt += (
                    "\n\nRETRY: Return one complete valid JSON object containing only "
                    "mapping_name, assumptions_or_gaps, and pyspark_code. Keep all "
                    "explanations out of the response. Put the complete code once in "
                    "pyspark_code. Do not invent merge keys, source columns, router "
                    "conditions, or data-quality semantics."
                )
            try:
                code_result = self._parse_response(self._call_llm(current_prompt))
                if not isinstance(code_result, dict):
                    raise ValueError("LLM code response must be a JSON object.")
                if not isinstance(code_result.get("pyspark_code"), str) or not code_result["pyspark_code"].strip():
                    raise ValueError("LLM code response did not contain non-empty pyspark_code.")
                break
            except (ValueError, json.JSONDecodeError) as exc:
                code_error = exc
                if attempt == 1:
                    # A length-truncated whole-mapping response is retried as
                    # smaller LLM-generated target functions instead of asking
                    # for the same large JSON object a third time.
                    if "output-token limit" in str(code_error).lower() or "finish_reason=length" in str(code_error).lower():
                        try:
                            code_result = self._generate_code_in_target_chunks(llm_input)
                            break
                        except Exception as chunk_exc:
                            raise ValueError(
                                "LLM full-mapping generation was truncated and target-chunk "
                                "fallback also failed. No generated PySpark was executed. "
                                "Details: " + str(chunk_exc)
                            ) from chunk_exc
                    raise ValueError(
                        "LLM PySpark-only generation failed after one compact retry. "
                        "No generated PySpark was executed. Details: " + str(code_error)
                    ) from exc

        # Reject syntactically valid but incomplete LLM output before asking for
        # descriptive metadata. If the full-mapping response contains stubs, retry
        # through target-specific LLM requests; never patch the generated code with
        # deterministic templates.
        code_result["pyspark_code"] = self._normalize_pyspark_code(
            code_result["pyspark_code"]
        )
        if self._contains_incomplete_logic(code_result["pyspark_code"]):
            try:
                code_result = self._generate_code_in_target_chunks(llm_input)
            except Exception as chunk_exc:
                raise ValueError(
                    "LLM returned incomplete/placeholder PySpark and target-chunk "
                    "generation did not produce complete functions. No generated "
                    "PySpark was executed. Details: " + str(chunk_exc)
                ) from chunk_exc
            if self._contains_incomplete_logic(code_result.get("pyspark_code", "")):
                raise ValueError(
                    "LLM target-chunk generation still contains incomplete logic "
                    "(placeholder/TODO/pass-only function/NotImplementedError). "
                    "No generated PySpark was executed."
                )

        # Ask for only descriptive metadata after the code is complete. This
        # response cannot truncate or duplicate the generated PySpark.
        summary_prompt = self._build_split_prompt(llm_input, mode="summary")
        summary_result = None
        summary_error = None
        try:
            summary_result = self._parse_response(
                self._call_llm(summary_prompt, max_tokens=min(self.max_tokens, 3000))
            )
            if not isinstance(summary_result, dict):
                raise ValueError("LLM summary response must be a JSON object.")
        except (ValueError, json.JSONDecodeError) as exc:
            summary_error = exc

        # Descriptive fields are not allowed to overwrite generated code.
        result = {
            "mapping_name": code_result.get("mapping_name") or self._mapping_name(llm_input),
            "migration_plan": summary_result.get("migration_plan", []) if summary_result else [],
            "transformation_logic": summary_result.get("transformation_logic", []) if summary_result else [],
            "assumptions_or_gaps": [],
            "pyspark_code": code_result["pyspark_code"],
        }
        code_gaps = code_result.get("assumptions_or_gaps", [])
        summary_gaps = summary_result.get("assumptions_or_gaps", []) if summary_result else []
        if isinstance(code_gaps, list):
            result["assumptions_or_gaps"].extend(code_gaps)
        elif code_gaps:
            result["assumptions_or_gaps"].append(str(code_gaps))
        if isinstance(summary_gaps, list):
            result["assumptions_or_gaps"].extend(summary_gaps)
        elif summary_gaps:
            result["assumptions_or_gaps"].append(str(summary_gaps))
        if summary_error:
            result["assumptions_or_gaps"].append(
                "Descriptive summary generation failed; code was generated separately: " + str(summary_error)
            )
        # De-duplicate gap strings while preserving order.
        seen_gaps = set()
        result["assumptions_or_gaps"] = [
            gap for gap in result["assumptions_or_gaps"]
            if not (str(gap) in seen_gaps or seen_gaps.add(str(gap)))
        ]

        self._validate_response(result)

        if validator_factory is None:
            return result

        attempts = max(0, int(max_repair_attempts))
        validation_history = []
        for attempt in range(attempts + 1):
            validator = validator_factory(result["pyspark_code"], llm_input)
            validation = validator.validate()
            if not isinstance(validation, dict):
                validation = {
                    "valid": False,
                    "errors": ["Validator returned a non-dictionary result."],
                    "warnings": [],
                }
            # Supplement syntax/static checks with mapping-aware semantic safety checks.
            semantic_errors = self._semantic_guard_errors(result["pyspark_code"], llm_input)
            if semantic_errors:
                validation.setdefault("errors", [])
                validation["errors"] = list(validation.get("errors", [])) + semantic_errors
                validation["valid"] = False
            # Fail closed: a validator result is valid only when its flag is true
            # AND its error list is empty. Never trust a contradictory response.
            validation_errors = validation.get("errors", []) or []
            if not isinstance(validation_errors, list):
                validation_errors = [str(validation_errors)]
            validation["errors"] = list(dict.fromkeys(str(e) for e in validation_errors if str(e).strip()))
            validation["warnings"] = list(validation.get("warnings", []) or [])
            validation["valid"] = bool(validation.get("valid")) and not validation["errors"]
            validation_history.append(validation)
            result["validation"] = validation
            result["repair_attempts_used"] = attempt
            result["validation_history"] = validation_history
            if validation["valid"]:
                result["generation_status"] = "VALIDATED"
                return result
            if attempt >= attempts:
                result["generation_status"] = "VALIDATION_FAILED"
                return result
            if self._only_metadata_blockers(validation.get("errors", [])):
                result["generation_status"] = "BLOCKED_BY_METADATA"
                result["repair_attempts_used"] = attempt
                result["blocking_metadata_errors"] = list(validation.get("errors", []))
                return result
            repair_prompt = self._build_repair_prompt(
                llm_input=llm_input,
                previous_result=result,
                validation=validation,
                attempt=attempt + 1,
                max_attempts=attempts,
            )
            # Repair is a code-only phase. The first implementation asked for the
            # full plan + code again, wasting output tokens and frequently truncating.
            # Accept only a real code field, then preserve the already-generated
            # descriptive fields rather than letting the repair response erase them.
            repaired_payload = self._parse_response(
                self._call_llm(repair_prompt, max_tokens=min(self.max_tokens, 8000))
            )
            if not isinstance(repaired_payload, dict):
                raise ValueError("LLM repair response must be a JSON object.")
            repaired_code = repaired_payload.get("pyspark_code")
            if not isinstance(repaired_code, str) or not repaired_code.strip():
                for alias in ("generated_code", "python_code", "code"):
                    candidate = repaired_payload.get(alias)
                    if isinstance(candidate, str) and candidate.strip():
                        repaired_code = candidate
                        break
            if not isinstance(repaired_code, str) or not repaired_code.strip():
                raise ValueError(
                    "LLM repair response did not contain non-empty pyspark_code; "
                    "the previous validated artifact was retained and not executed."
                )
            repaired_code = self._normalize_pyspark_code(repaired_code)
            try:
                ast.parse(repaired_code)
            except SyntaxError as syntax_error:
                raise ValueError(
                    "LLM repair returned invalid Python syntax: " + str(syntax_error)
                ) from syntax_error
            if self._contains_incomplete_logic(repaired_code):
                # Keep it in the repair loop so the next attempt receives the
                # exact validation error instead of treating a stub as repaired.
                validation_history.append({
                    "valid": False,
                    "errors": ["Repair output contains incomplete/placeholder logic."],
                    "warnings": [],
                })
            result = {
                "mapping_name": repaired_payload.get("mapping_name") or result["mapping_name"],
                "migration_plan": result.get("migration_plan", []),
                "transformation_logic": result.get("transformation_logic", []),
                "assumptions_or_gaps": repaired_payload.get(
                    "assumptions_or_gaps", result.get("assumptions_or_gaps", [])
                ),
                "pyspark_code": repaired_code,
            }
        # Defensive terminal state: generation must never fall through without
        # an explicit validation status.
        result.setdefault("generation_status", "VALIDATION_FAILED")
        result.setdefault("validation", {"valid": False, "errors": ["No successful final validation occurred."], "warnings": []})
        if result.get("validation", {}).get("errors"):
            result["validation"]["valid"] = False
        return result

    @staticmethod
    def _semantic_guard_errors(code, llm_input):
        """Reject high-risk generated patterns missed by syntax-only validation."""
        errors = []
        if not isinstance(code, str) or not isinstance(llm_input, dict):
            return errors
        transformations = llm_input.get("transformations", [])
        if isinstance(transformations, dict):
            transformations = list(transformations.values())
        if not isinstance(transformations, list):
            transformations = []
        targets = llm_input.get("targets", [])
        if not isinstance(targets, list):
            targets = []
        sequence_nodes = [item for item in transformations if isinstance(item, dict) and
                          "sequence" in str(item.get("type", item.get("name", ""))).casefold()]
        has_upsert_target = any(isinstance(target, dict) and
                                str(target.get("operation", "")).upper() in {"UPSERT", "MERGE"}
                                for target in targets)
        if sequence_nodes and has_upsert_target and re.search(
                r"\b(row_number|monotonically_increasing_id)\s*\(", code):
            # Permit row_number only as an allocator for NEW keys when existing
            # surrogate keys are looked up and reused for matching business keys.
            has_persistent_allocator = (
                bool(re.search(r"DeltaTable\.forName\s*\(", code))
                and bool(re.search(r"max\s*\(\s*['\"]customer_sk['\"]", code))
                and bool(re.search(r"customer_sk", code))
                and bool(re.search(r"left_anti|leftanti|existing.*customer_sk|customer_sk.*existing", code, re.I | re.S))
            )
            if not has_persistent_allocator:
                names = ", ".join(str(item.get("name", "Sequence")) for item in sequence_nodes)
                errors.append("Semantic safety: sequence transformation(s) " + names +
                              " feed an UPSERT mapping, but generated code assigns a fresh row_number/"
                              "monotonically_increasing_id instead of preserving existing surrogate keys. "
                              "Reuse existing customer_sk by business key and allocate keys only for new IDs; "
                              "otherwise report a blocking metadata gap.")
        gate_nodes = [item for item in transformations if isinstance(item, dict) and
                      "gate" in str(item.get("name", "")).casefold()]
        if gate_nodes:
            try:
                tree = ast.parse(code)
                column_names = set(re.findall(r"withColumn\s*\(\s*['\"]([A-Za-z_][A-Za-z0-9_]*)['\"]", code))
                gate_columns = {name for name in column_names if
                                "dq_status" in name.casefold() or "quality_status" in name.casefold()}
                consumed, persisted = set(), set()
                for node in ast.walk(tree):
                    if not isinstance(node, ast.Call):
                        continue
                    method_name = node.func.attr if isinstance(node.func, ast.Attribute) else ""
                    segment = ast.get_source_segment(code, node) or ""
                    if method_name in {"filter", "where", "when", "otherwise"}:
                        consumed.update(name for name in gate_columns if name in segment)
                    if method_name == "select":
                        persisted.update(name for name in gate_columns if name in segment)
                unused = gate_columns - consumed - persisted
                if unused:
                    errors.append("Semantic safety: DataQuality gate transformation(s) " +
                                  ", ".join(str(item.get("name")) for item in gate_nodes) +
                                  " produce status column(s) " + ", ".join(sorted(unused)) +
                                  " that do not control downstream filtering/routing and are not persisted. "
                                  "Follow the XML-defined disposition; if reject/quarantine/pass-through "
                                  "behavior is not specified, report a blocking metadata gap instead of inventing one.")
            except SyntaxError:
                pass
        return errors

    @staticmethod
    def _only_metadata_blockers(errors):
        """True when another LLM code rewrite cannot safely resolve the failures."""
        if not errors:
            return False
        metadata_markers = (
            "merge_key_status=", "authoritative merge-key metadata is missing",
            "does not define merge keys", "has no target columns/schema",
            "has no target-column metadata", "target schema is unavailable",
            "target-column metadata", "metadata blocker:",
        )
        return all(
            any(marker in str(error).casefold() for marker in metadata_markers)
            for error in errors
        )

    def _generate_code_in_target_chunks(self, llm_input):
        """
        Fallback when the full-mapping PySpark response is truncated.

        PySpark is still generated by the configured LLM. Each target gets a
        separate, bounded LLM request; the resulting functions are assembled
        into one Python module. This does not resolve missing merge keys.
        """
        targets = llm_input.get("targets", []) if isinstance(llm_input, dict) else []
        if not isinstance(targets, list) or not targets:
            raise ValueError(
                "Full PySpark response was truncated and target-chunk fallback "
                "cannot run because target metadata is missing."
            )

        functions = []
        gaps = []
        # Build a compact mapping context for each request. Reusing the full
        # prompt (including repository-wide specialized metadata and every target)
        # can itself force a one-target response to hit the output limit.
        for index, target in enumerate(targets, start=1):
            if not isinstance(target, dict):
                continue
            target_name = (
                target.get("physical_table")
                or target.get("target")
                or target.get("name")
                or f"target_{index}"
            )
            function_name = f"migrate_target_{index}"
            compact_context = {
                "mapping": llm_input.get("mapping", {}),
                "graph": llm_input.get("graph", {}),
                "sources": llm_input.get("sources", []),
                "transformations": llm_input.get("transformations", []),
                "links": llm_input.get("links", []),
                "target": target,
                "data_quality_rules": llm_input.get("data_quality_rules", []),
                "xml_mapping_metadata": llm_input.get("xml_mapping_metadata", {}),
                "blocking_issues": llm_input.get("blocking_issues", []),
                "databricks": llm_input.get("databricks", {}),
            }
            compact_json = json.dumps(
                compact_context, ensure_ascii=False, separators=(",", ":"), default=str
            )
            chunk_prompt = f"""
You are an expert IDMC-to-Databricks migration engineer. Generate PySpark code
using ONLY the normalized metadata below. The code must be generated by you as
the LLM; do not describe a deterministic template or invent metadata.

SELECTED TARGET: {target_name}
REQUIRED FUNCTION: {function_name}(spark)

Return exactly one valid JSON object with these fields:
{{"mapping_name":"...","assumptions_or_gaps":[],"pyspark_code":"..."}}

The pyspark_code value must contain a complete, syntactically valid Python module
defining the required function. Generate code for this target only. Keep the
response concise. Do not include markdown fences or prose outside JSON.
Use Spark DataFrame APIs; do not use pandas or sample data. Preserve only
metadata-supported expressions, joins, filters, aggregations, lookup logic,
router conditions, and DQ rules. Do not invent source columns or business rules.
If this target is UPSERT/MERGE and merge_keys are absent, do not write it and
record that as a blocking gap. Never substitute append/overwrite for MERGE.

EXACT TARGET NAME RULE — CRITICAL:
- The selected target's exact physical_table value is the only valid Delta target name.
- For this request, the selected target metadata is:
  {json.dumps(target, ensure_ascii=False, separators=(',', ':'), default=str)}
- Use the exact physical_table string from that metadata in DeltaTable.forName(spark, ...).
- Never shorten a three-part name such as "idmc_poc.curated.customer_low_value"
  to "curated.customer_low_value" or change its catalog/schema/table components.
- A target-name variable is allowed only if its assigned string equals physical_table exactly.
- Before returning code, compare the DeltaTable.forName target name with physical_table character-for-character.
- Do not hide writes in generic helper functions or pass target names through parameters. Emit the
  target-specific DeltaTable.forName(...).alias("t").merge(...) chain directly in the required function.
- Treat merge_key_status values exactly POC_ASSUMPTION, ASSUMED, UNVERIFIED, or UNKNOWN as unapproved. APPROVED_POC_ASSUMPTION is allowed only for this POC, must be disclosed as an assumption, and must not be described as authoritative.
- Do not use Column '+' for string concatenation; use F.concat/F.concat_ws when metadata supports it.
- Never assign a fresh row_number() to every UPSERT row. For a declared Sequence transformation, preserve existing keys by business key and allocate sequence values only for new keys using existing persistent target key values; otherwise block writes.
- A DQ gate cannot be a calculated-but-unused status column. For this POC, if the XML defines a gate and no quarantine target exists, filter out rows failing the supplied gate rules, count rejected rows, and disclose this disposition as a POC assumption.
- For outer-join metrics used by this customer segmentation router, treat missing order metrics as zero as an explicit POC assumption, while preserving XML router thresholds.
- Validate uniqueness of lookup keys and source merge keys before target writes; fail closed on duplicates.

For UPSERT/MERGE with approved keys, call .merge(...) only on DeltaTable.forName(spark, exact_physical_table),
optionally after .alias(...). Never call .merge(...) on a DataFrame. Use the source DataFrame
as the first argument to DeltaTable.merge(...), and build the predicate only from merge_keys.
Do not call the generated function.

NORMALIZED MAPPING CONTEXT (JSON):
{compact_json}
"""
            last_error = None
            chunk_result = None
            for attempt, budget in enumerate((min(self.max_tokens, 6000), min(self.max_tokens, 4000), min(self.max_tokens, 2500))):
                prompt = chunk_prompt
                if attempt:
                    prompt += (
                        "\\nRETRY: Your previous response did not satisfy the output "
                        "contract. Return exactly one JSON object with the exact key "
                        '"pyspark_code" containing a non-empty string of complete Python '
                        "source. Do not use code/generated_code/python_code as the key. "
                        f"Define {function_name}(spark). Use the exact physical_table value from the selected "
                        "target metadata in DeltaTable.forName; never shorten catalog.schema.table. "
                        "If the target is UPSERT and merge_keys are missing, include a function that clearly records "
                        "the blocker and does not write to the target; do not invent keys. "
                        "Keep the function concise and valid. Do not emit placeholder, "
                        "TODO, pass-only function, or NotImplementedError. Implement the "
                        "available transformations for this target even when the final "
                        "write is blocked by missing merge keys."
                    )
                try:
                    chunk_result = self._parse_response(
                        self._call_llm(prompt, max_tokens=budget)
                    )
                    if not isinstance(chunk_result, dict):
                        raise ValueError("Target chunk response must be a JSON object.")
                    # Accept common field-name drift from the serving model, but
                    # normalize the result to the contract used by the pipeline.
                    code = chunk_result.get("pyspark_code")
                    if not isinstance(code, str) or not code.strip():
                        for alias in ("generated_code", "python_code", "code"):
                            candidate = chunk_result.get(alias)
                            if isinstance(candidate, str) and candidate.strip():
                                code = candidate
                                chunk_result["pyspark_code"] = candidate
                                break
                    if not isinstance(code, str) or not code.strip():
                        keys = ", ".join(sorted(str(k) for k in chunk_result.keys()))
                        raise ValueError(
                            "Target chunk response has no non-empty pyspark_code "
                            f"(returned keys: {keys or 'none'})."
                        )
                    code = self._normalize_pyspark_code(code)
                    chunk_result["pyspark_code"] = code
                    ast.parse(code)
                    if self._contains_incomplete_logic(code):
                        raise ValueError(
                            "Target chunk contains placeholder or incomplete logic."
                        )
                    if not re.search(rf"def\s+{re.escape(function_name)}\s*\(", code):
                        raise ValueError(
                            f"Target chunk did not define required function {function_name}."
                        )
                    break
                except (ValueError, json.JSONDecodeError, SyntaxError) as exc:
                    last_error = exc
                    chunk_result = None
            if chunk_result is None:
                raise ValueError(
                    f"LLM target-chunk generation failed for '{target_name}'. "
                    "No generated PySpark was executed. Details: " + str(last_error)
                )
            functions.append(chunk_result["pyspark_code"].strip())
            chunk_gaps = chunk_result.get("assumptions_or_gaps", [])
            if isinstance(chunk_gaps, list):
                gaps.extend(str(item) for item in chunk_gaps)
            elif chunk_gaps:
                gaps.append(str(chunk_gaps))

        if not functions:
            raise ValueError("No target-specific PySpark functions were generated.")

        assembled = (
            "# LLM-generated target-specific migration functions.\n"
            "# Functions are not invoked by this module.\n\n"
            + "\n\n".join(functions)
        )
        ast.parse(assembled)
        return {
            "mapping_name": self._mapping_name(llm_input),
            "assumptions_or_gaps": list(dict.fromkeys(gaps)),
            "pyspark_code": assembled,
        }

    @staticmethod
    def _contains_incomplete_logic(code):
        """Return True for common LLM stubs while allowing legitimate blocker comments."""
        if not isinstance(code, str) or not code.strip():
            return True
        if re.search(
            r"\b(placeholder|notimplementederror|todo\s*:|implement\s+me|stub\s+function)\b",
            code,
            flags=re.IGNORECASE,
        ):
            return True
        try:
            tree = ast.parse(code)
        except SyntaxError:
            return True
        for node in ast.walk(tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                body = list(node.body)
                if body and isinstance(body[0], ast.Expr) and isinstance(
                    getattr(body[0], "value", None), (ast.Str, ast.Constant)
                ) and isinstance(getattr(body[0].value, "value", getattr(body[0].value, "s", None)), str):
                    body = body[1:]
                if not body:
                    return True
                if len(body) == 1 and isinstance(body[0], ast.Pass):
                    return True
                if len(body) == 1 and isinstance(body[0], ast.Raise):
                    exc = body[0].exc
                    if isinstance(exc, ast.Call):
                        func = exc.func
                        if (isinstance(func, ast.Name) and func.id == "NotImplementedError") or (
                            isinstance(func, ast.Attribute) and func.attr == "NotImplementedError"
                        ):
                            return True
        return False

    @staticmethod
    def _mapping_name(llm_input):
        """Best-effort mapping name extraction without inventing one."""
        if isinstance(llm_input, dict):
            for key in ("mapping_name", "name"):
                value = llm_input.get(key)
                if isinstance(value, str) and value.strip():
                    return value.strip()
            for parent_key in ("mapping", "mapping_metadata", "xml_mapping_metadata"):
                parent = llm_input.get(parent_key)
                if isinstance(parent, dict):
                    for key in ("mapping_name", "name"):
                        value = parent.get(key)
                        if isinstance(value, str) and value.strip():
                            return value.strip()
        return "unknown_mapping"

    @staticmethod
    def _compact_mapping_input(llm_input):
        """Keep only mapping-scoped evidence in each LLM request.

        Repository-wide specialized metadata can exceed the model context window
        even when the selected mapping itself is small. The parser's normalized
        mapping graph, sources, transformations, targets, links, DQ rules and
        blocking issues remain the primary evidence for code generation.
        """
        if not isinstance(llm_input, dict):
            return llm_input

        allowed = (
            "mapping", "mapping_name", "graph", "sources", "transformations",
            "targets", "links", "data_quality_rules", "xml_mapping_metadata",
            "blocking_issues", "databricks", "runtime_parameters",
            "parameters", "mapping_task", "mapping_task_metadata",
        )
        compact = {key: llm_input[key] for key in allowed if key in llm_input}

        # Drop repository-wide / raw XML payloads accidentally nested in otherwise
        # mapping-scoped metadata, while retaining expressions and column metadata.
        excluded_key_fragments = (
            "raw_xml", "full_xml", "repository_xml", "specialized_metadata",
            "metadata_relationships", "ai_recommendations", "asset_folders",
            "taskflows", "application_integration_processes", "api_catalog",
            "data_ingestion_jobs", "cross_reference_validation",
        )
        def prune(value):
            if isinstance(value, dict):
                result = {}
                for key, item in value.items():
                    normalized = str(key).lower()
                    if any(fragment in normalized for fragment in excluded_key_fragments):
                        continue
                    result[key] = prune(item)
                return result
            if isinstance(value, list):
                return [prune(item) for item in value]
            return value

        return prune(compact)

    def _build_split_prompt(self, llm_input, mode):
        """Build a bounded mapping-scoped prompt for one response phase."""
        llm_input = self._compact_mapping_input(llm_input)
        prompt = self._build_prompt(llm_input)

        task_start = prompt.find("Your task is to produce:")
        core_start = prompt.find("CORE MIGRATION RULES", task_start)
        if task_start >= 0 and core_start > task_start:
            if mode == "code":
                replacement = (
                    "Your task is ONLY to generate complete executable Databricks "
                    "PySpark code and concise blocking gaps. Do not produce a migration "
                    "plan or transformation summaries in this response.\n\n"
                    "OUTPUT RULES: Keep the JSON compact. Include the complete code only "
                    "once in pyspark_code. Never invent missing metadata or business rules.\n\n"
                )
            else:
                replacement = (
                    "Your task is ONLY to produce a concise migration plan, concise "
                    "transformation summaries, and assumptions_or_gaps. Do NOT output "
                    "PySpark code. Keep migration_plan to at most 16 short items and "
                    "transformation_logic to at most 10 short items.\n\n"
                )
            prompt = prompt[:task_start] + replacement + prompt[core_start:]

        schema_start = prompt.find("REQUIRED JSON")
        metadata_start = prompt.find("IDMC METADATA", schema_start)
        if schema_start >= 0 and metadata_start > schema_start:
            if mode == "code":
                schema = """REQUIRED JSON\n\nReturn ONLY this JSON object, with no Markdown fences:\n\n{\n  "mapping_name": "mapping name from supplied metadata",\n  "assumptions_or_gaps": [],\n  "pyspark_code": "complete executable Databricks PySpark source"\n}\n\n============================================================\n\n"""
            else:
                schema = """REQUIRED JSON\n\nReturn ONLY this JSON object, with no Markdown fences. Do not include a pyspark_code field:\n\n{\n  "mapping_name": "mapping name from supplied metadata",\n  "migration_plan": [{"step": 1, "node": "node name", "type": "transformation type", "description": "short description"}],\n  "transformation_logic": [{"node": "node name", "type": "transformation type", "logic": "short summary without code"}],\n  "assumptions_or_gaps": []\n}\n\n============================================================\n\n"""
            prompt = prompt[:schema_start] + schema + prompt[metadata_start:]

        if mode == "code":
            prompt += (
                "\nFINAL OVERRIDE: This is the CODE phase. Output only mapping_name, "
                "assumptions_or_gaps, and pyspark_code. Do not include plan or summary "
                "fields. The supplied blocking_issues are authoritative. If unresolved "
                "merge keys or other blockers exist, do not write those targets and "
                "record the blockers.\n"
            )
        else:
            prompt += (
                "\nFINAL OVERRIDE: This is the SUMMARY phase. Output only mapping_name, "
                "migration_plan, transformation_logic, and assumptions_or_gaps. Never "
                "include PySpark source code or code snippets. Do not invent semantics.\n"
            )
        prompt += '\n\nTARGET WRITE SAFETY AND TRANSFORMATION COVERAGE CONTRACT:\n- Emit complete Python source with one clearly named top-level migration function. Do not emit\n  disconnected snippets, pseudocode, placeholder functions, or target-write shells.\n- Read every declared source using its exact physical_table or resolved file path. Build the\n  transformation DataFrames in graph dependency order and preserve the required row grain.\n- For EVERY transformation node, implement the supplied expression, filter, lookup/join,\n  aggregation, routing, or DQ rule in executable code, OR report the precise missing metadata\n  as a blocker. Do not satisfy coverage with an unrelated operation elsewhere in the script.\n- Follow graph links and input/output ports to determine which DataFrame feeds each node and target.\n  Do not invent expressions, columns, route thresholds, merge keys, or target schemas.\n- For every target, emit its declared write when metadata is sufficient. If metadata is insufficient,\n  explicitly block that target and report the exact gap; do not silently omit it or claim success.\n- Never hide target writes inside a generic helper or pass target names through function parameters.\n  Emit each target write explicitly with its exact physical_table string for static validation. For MAP100 the target literals are idmc_poc.curated.customer_gold, idmc_poc.curated.customer_standard, and idmc_poc.curated.customer_low_value; all three require an explicit DeltaTable.forName(...).merge(...).execute() statement.\n- MERGE KEY STATUS: use only supplied merge_keys. A status exactly POC_ASSUMPTION, ASSUMED, UNVERIFIED, or UNKNOWN is not approved. APPROVED_POC_ASSUMPTION is permitted for this POC because the supplied XML explicitly marks it approved; record it as a POC assumption and keep actual execution disabled until verified. Never claim it is authoritative.\n- For every UPSERT/MERGE target with supplied keys and an allowed status, emit an explicit DeltaTable.forName(spark, exact_physical_table).alias("t").merge(source_df.alias("s"), predicate_from_only_supplied_keys).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute(). Keep the exact target table literal visible in the code. Never substitute append/overwrite or omit a target.\n- Use F.concat/F.concat_ws for supported string concatenation; preserve supplied null semantics.\n- SEQUENCE SAFETY: when a Sequence node declares startValue/incrementBy and the mapping UPSERTs by a business key, preserve existing customer_sk for existing business keys by reading current keys from declared target tables. Allocate keys only for new IDs, starting at max(existing customer_sk)+incrementBy, in deterministic business-key order. Detect duplicate business keys and conflicting existing surrogate keys before writes. This allocator assumes a single-writer POC; document that concurrent runs require a transactional central sequence/key registry. Never assign a fresh row_number to every row. If reliable existing-key lookup is impossible, block writes and explain why.\n- DATA-QUALITY GATES: implement exact XML rules and ensure DQ result controls downstream flow. If XML defines a DQ gate but no quarantine target exists, for this POC treat the gate as a pass filter, count/report rejected rows, and disclose this disposition as a POC assumption in assumptions_or_gaps. Do not calculate dq_status and ignore it. Persist DQ status only if target schema supports it.\n- NULL / ROUTING SAFETY: for this customer segmentation POC, treat missing order metrics from the left join as zero before applying the declared router thresholds, and disclose this as a POC assumption. Do not change XML thresholds.\n- LOOKUP / MERGE SAFETY: validate lookup-key uniqueness and source uniqueness for MERGE keys before writes; fail closed on duplicates.\n- A syntactically valid script is not enough: do not claim success if any transformation or target\n  is missing, metadata blockers remain, or validation fails.\n'
        return prompt

    def _build_repair_prompt(
        self, llm_input, previous_result, validation, attempt, max_attempts
    ):
        """
        Ask the LLM to repair its own code. Do not patch generated code with
        deterministic templates or silently weaken validation.
        """
        # Keep the repair payload bounded. The prior implementation serialized the
        # entire result, including validation history and summaries, on every retry.
        # That made later repairs progressively larger and more likely to truncate.
        repair_context = {
            "previous_result": {
                "mapping_name": previous_result.get("mapping_name"),
                "assumptions_or_gaps": previous_result.get("assumptions_or_gaps", []),
                "pyspark_code": previous_result.get("pyspark_code", ""),
            },
            "validation_errors": validation.get("errors", []),
            "validation_warnings": validation.get("warnings", []),
            "repair_attempt": attempt,
            "maximum_repair_attempts": max_attempts,
        }
        return (
            self._build_split_prompt(llm_input, mode="code")
            + "\n\nREPAIR THE PREVIOUS GENERATED RESULT\n"
            + "Return ONLY a JSON object containing mapping_name, assumptions_or_gaps, "
              "and pyspark_code. Do not return migration_plan or transformation_logic. "
              "The pyspark_code value must be the complete corrected Python module.\n"
            + "The previous PySpark failed validation. Correct the actual "
              "pyspark_code and any related plan/gap fields. Do not merely "
              "describe the fix. Preserve all supplied mapping semantics. "
              "Do not suppress or work around a validator error by removing "
              "a required source, transformation, or target. Do not invent "
              "business rules, route thresholds, merge keys, or target schemas. "
              "If required metadata is missing, explicitly record the gap and "
              "do not claim the mapping is fully executable. Never use row_number or "
              "monotonically_increasing_id as a durable UPSERT surrogate key. Do not leave "
              "DQ gate status unused, invent null-routing defaults, or ignore duplicate lookup/merge keys; "
              "block writes when required behavior is not defined. Fix each validation error by "
              "implementing the actual required transformation or target write; do not add "
              "unrelated operations merely to satisfy keyword checks. Preserve graph dependency "
              "order and source row grain. Return the same required JSON structure and complete "
              "PySpark source.\n"
            + "DELTA MERGE REPAIR RULE: For every UPSERT/MERGE target with supplied merge_keys, "
              "the receiver of .merge(...) MUST be a DeltaTable object obtained from "
              "DeltaTable.forName(spark, exact_target_name), optionally followed by .alias(...). "
              "Never call .merge(...) on a DataFrame such as df.alias(...). Use the exact "
              "physical_table and a predicate built only from that target's merge_keys. Do not "
              "repair validation by removing a target or weakening validation.\n\n"
            + json.dumps(repair_context, indent=2, ensure_ascii=False)
        )

    # ============================================================

    # PROMPT

    # ============================================================



    def _build_prompt(self, llm_input):

        metadata_json = json.dumps(

            llm_input,

            indent=2,

            ensure_ascii=False

        )



        target_schema = self._extract_target_schema(llm_input)



        if target_schema:

            target_schema_text = "\n".join(

                f"- {item['name']} -> {item['datatype']}"

                for item in target_schema

            )

        else:

            target_schema_text = (

                "Target column datatype metadata is not available."

            )



        prompt = f"""

You are an expert data engineer specializing in

Informatica Intelligent Data Management Cloud (IDMC)

to Databricks migrations.



You are given normalized metadata extracted from an IDMC mapping XML. The payload may include specialized_metadata (mapping tasks, taskflows, runtime parameters, data-quality rules, ingestion jobs, APIs, asset folders, repository metadata, metadata relationships, AI recommendations, and cross-reference validation), data_quality_rules, xml_mapping_metadata, and blocking_issues. Use specialized_metadata as additional evidence when it is present, but do not treat unrelated assets as part of the selected mapping unless the metadata establishes that relationship. These are authoritative evidence from the XML; use exact expressions and report missing semantics explicitly.



Your task is to produce:



1. A concise migration execution plan (one short item per graph node).
2. Concise transformation-level summaries (no copied code).
3. Executable Databricks PySpark code, included only once.

OUTPUT SIZE RULES:
- Keep migration_plan descriptions to 8 words or fewer.
- Keep transformation_logic summaries to 1-2 short sentences each; never put code snippets there.
- Put all executable source code only in pyspark_code.
- Do not repeat full source code or transformation expressions in other JSON fields.
- Keep assumptions_or_gaps concise and list unresolved blockers explicitly.
- Return exactly one complete JSON object with no Markdown fences.



============================================================

CORE MIGRATION RULES

============================================================



- Use ONLY the supplied metadata.

- Do not invent sources.

- Do not invent target tables.

- Do not invent transformation nodes.

- Preserve graph dependencies.

- Preserve the supplied graph execution order.

- Preserve source-to-target mappings.

- Preserve transformation types.

- Preserve expressions, filters, lookups, joins and aggregations

  whenever they are present in the metadata.

- Use Spark DataFrame APIs.

- Use Databricks-compatible PySpark.

- Do not use pandas.

- Do not create fake/sample data.

- Do not hardcode business data.

- Read each source from its supplied physical_table OR explicitly configured file location.
- Write every target declared by the mapping; do not assume there is only one target.
- Honor each target's operation metadata (INSERT/APPEND/OVERWRITE/UPSERT/MERGE).
- The input field blocking_issues is authoritative. Never silently resolve or ignore those issues.
- Never invent source columns, target columns, merge keys, router thresholds/conditions, or DQ rules.
- For UPSERT/MERGE targets with no supplied merge_keys, do not substitute append/saveAsTable and do not invent customer_sk or another key.
- For a Router with output groups but no conditions, do not invent numeric thresholds; record a blocking gap.
- Use data_quality_rules and transformation-level data_quality_rules to implement only the exact supplied expressions. If a rule's meaning cannot be translated safely, record a blocker; never add a static PASS flag.
- When the XML does not declare source fields, include a runtime source-schema guard that reports the actual missing columns before any target write. Do not assume the source schema matches transformation references.
- A result with unresolved blocking issues must not be marked executable/successful.
- The generated code must be executable Databricks Python.
- Never convert a logical name or file path into a guessed table identifier.
- Never pass a file path such as "landing/customer.csv" to spark.table().
- For file sources without physical_table, use spark.read only; never spark.table(file_path).
  Resolve relative paths only against explicit databricks.source_file_base_path metadata.
  Require an explicit format; infer only from a clear file extension if metadata allows.
  If path or format cannot be resolved, record a blocker in assumptions_or_gaps; do not invent
  a base path, connection, format, or Bronze table name.



============================================================

SOURCE TABLE RULES

============================================================



For every source that has a physical_table value:

- Read that exact physical table.
- Do not replace physical_table with the logical source name.

For a source with location_type="file" and file_path but no physical_table:

- It is a file source, not a Unity Catalog table; read it with spark.read.format(...).load(...).
- Do not call spark.table(file_path).
- Require an explicit format and a resolved absolute path/URI before reading the file.
  if both are present in metadata.
- If the path cannot be resolved from configured metadata, record a blocker.
  add an assumptions_or_gaps entry and do not fabricate a substitute.



Accept valid forms such as:



    spark.table("catalog.schema.table")



    spark.read.table("catalog.schema.table")



    spark.read.format("delta").table("catalog.schema.table")



============================================================

JOIN SAFETY RULES - VERY IMPORTANT

============================================================



Spark joins can create ambiguous references when both sides

contain the same column name.



Therefore:



- Whenever a join condition is an expression, ALWAYS alias both

  DataFrames.

- Use fully qualified columns in the join condition.

- Use fully qualified columns in the post-join select.

- Do not use an unqualified F.col("COLUMN") for a column that can

  exist on both sides of a join.

- Do not carry duplicate join-key columns forward unnecessarily.

- After a join, explicitly select the columns required by the next

  transformation.

- Prefer this pattern:



    df_result = (

        df_left.alias("l")

        .join(

            df_right.alias("r"),

            F.col("l.CUSTOMER_ID") == F.col("r.CUSTOMER_ID"),

            "inner"

        )

        .select(

            F.col("l.CUSTOMER_ID").alias("CUSTOMER_ID"),

            F.col("l.CUSTOMER_NAME").alias("CUSTOMER_NAME"),

            F.col("r.ORDER_ID").alias("ORDER_ID")

        )

    )



- Never rely on Spark to guess which duplicate column is intended.



If a join uses a simple string key such as:



    on="CUSTOMER_ID"



it is acceptable, but explicitly select the required columns

afterward.



============================================================

TARGET SCHEMA RULES - VERY IMPORTANT

============================================================



The final DataFrame written to the target MUST match the target

metadata in BOTH:



1. Column names

2. Datatypes



The target schema extracted from the supplied metadata is:



{target_schema_text}



Rules:



- Preserve target column names exactly.

- Preserve target column order whenever target metadata is available.

- Preserve target datatypes exactly whenever datatype metadata

  is available.

- Do not write intermediate transformation column names to the target.

- If an expression creates an intermediate name, rename it before

  the final write when the target expects another name.

- Before saveAsTable(), explicitly select the target columns in

  the target order.

- Do not use mergeSchema.

- Do not force overwriteSchema or overwrite; select write behavior based on target operation.

  mode("overwrite") ONLY when target metadata explicitly specifies OVERWRITE.

  Never use overwrite for UPSERT/MERGE targets.

- Target operation semantics are authoritative; never replace UPSERT/MERGE with overwrite.

  Use explicit target datatype casts where target metadata is available.

- The final target DataFrame should contain only the required

  target columns.



============================================================

TARGET DATATYPE RULES - CRITICAL

============================================================



The target datatype metadata is authoritative.



The generated PySpark MUST explicitly convert/cast transformation

results whenever Spark's natural datatype differs from the target

datatype.



Examples:



1. COUNT



Spark:



    F.count("ORDER_ID")



naturally produces BIGINT/LONG.



If target datatype is INTEGER, generate:



    F.count("ORDER_ID").cast("int").alias("TOTAL_ORDERS")



2. SUM



If target expects DECIMAL(18,2), generate an explicit cast:



    F.sum("ORDER_AMOUNT") \\

        .cast("decimal(18,2)") \\

        .alias("TOTAL_REVENUE")



Do not assume Spark's inferred DOUBLE type is acceptable when the

target expects DECIMAL.



3. DATE



If target expects DATE and source is STRING or TIMESTAMP, explicitly

convert it:



    F.to_date("ORDER_DATE").alias("LAST_ORDER_DATE")



or:



    F.to_date(F.col("ORDER_DATE")).alias("LAST_ORDER_DATE")



4. TIMESTAMP



If target expects TIMESTAMP, explicitly convert where required:



    F.to_timestamp("EVENT_DATE").alias("EVENT_DATE")



5. INTEGER / BIGINT



Use explicit casts when the target datatype requires a specific

integer width:



    .cast("int")



or:



    .cast("bigint")



6. DECIMAL



Use the exact precision and scale from target metadata whenever

they are supplied.



Example:



    .cast("decimal(18,2)")



7. STRING



If the target expects STRING and the source/expression produces

another datatype, explicitly cast where necessary:



    .cast("string")



8. BOOLEAN



If target expects BOOLEAN, convert the expression to a Spark

boolean rather than relying on implicit conversion.



IMPORTANT:



Do NOT blindly cast every column.



Only apply casts/conversions where required by the target metadata

or where the expression naturally produces an incompatible datatype.



The final DataFrame schema must conform to the target schema.



============================================================

TARGET TYPE VALIDATION BEFORE WRITE

============================================================



Before the final saveAsTable():



- Make sure every target column exists.

- Make sure every target column is in the correct order.

- Make sure every target column has the target datatype.

- Explicitly cast or convert columns where necessary.

- Do not rely on implicit Spark datatype conversion.

- Do not modify the generated DataFrame to accommodate an old physical

  target schema. The IDMC target metadata is the source of truth.

- Do not use mergeSchema.

- Do not force overwriteSchema or overwrite; select write behavior based on target operation.

  For UPSERT/MERGE, use DeltaTable.merge with metadata-backed keys; never overwrite.

  If merge keys are absent, record a blocker rather than inventing keys.



Preferred pattern:



    df_final = (

        df_previous

        .select(

            F.col("CUSTOMER_ID").cast("string").alias("CUSTOMER_ID"),

            F.col("CUSTOMER_NAME").cast("string").alias("CUSTOMER_NAME"),

            F.col("TOTAL_ORDERS").cast("int").alias("TOTAL_ORDERS"),

            F.col("TOTAL_REVENUE")

                .cast("decimal(18,2)")

                .alias("TOTAL_REVENUE"),

            F.to_date(F.col("LAST_ORDER_DATE"))

                .alias("LAST_ORDER_DATE")

        )

    )



Only use casts that are supported by the actual target metadata.



============================================================

TRANSFORMATION RULES

============================================================



SOURCE_QUALIFIER:



- Read the corresponding source physical table.



EXPRESSION:



- Translate supplied expressions into PySpark.

- Use withColumn/select/selectExpr as appropriate.

- Preserve the expression semantics.



FILTER:



- Translate the supplied filter condition into DataFrame

  filter/where.



JOINER:



- Translate join type and join condition.

- Use aliases and qualified columns as described above.



LOOKUP:



- Translate lookup source and lookup condition.

- Preserve lookup type when supplied.

- Explicitly select the required columns after the lookup.



AGGREGATOR:



- Translate group-by fields and aggregate expressions.

- Preserve count, sum, max, min, avg and other supplied functions.

- Check the resulting Spark datatype against the target datatype.

- Apply explicit casts when required by the target schema.



STORED_PROCEDURE:



- Do NOT call the original stored procedure.

- Read the stored procedure definition and business rules from

  the metadata.

- Convert the stored procedure logic itself into equivalent PySpark.

- The converted PySpark logic MUST actually execute.

- Do not merely describe the stored procedure.

- Do not label the implementation as "emulation".

- Use comments such as:



    # Migrated Stored Procedure Logic

    # Original IDMC procedure: <procedure name>



OTHER TRANSFORMATION TYPES:



- Preserve and translate their supplied metadata as accurately

  as possible.

- If the semantics cannot be determined, record the gap in

  assumptions_or_gaps instead of inventing business logic.



============================================================

EXECUTION ORDER

============================================================



- The graph is authoritative for dependency order.

- Do not arbitrarily reorder transformations based only on their

  semantic meaning.

- A transformation may only consume data produced by its

  dependencies.

- The generated code must follow a dependency-safe order.



============================================================

PYSPARK CODE QUALITY

============================================================



- Return actual Python source code in pyspark_code.

- Use real newline characters.

- Do not return repr() output.

- Do not return the code as a quoted Python string.

- Do not put markdown fences inside pyspark_code.

- Import required PySpark functions explicitly.

- Use parenthesized multiline method chaining.



Correct:



    df = (

        df

        .select(...)

        .withColumn(...)

    )



Incorrect:



    df = df.select(...)

        .withColumn(...)



- Do not use unsupported local Python processing for data.

- Do not create fake data.

- Do not use subprocess, os.system, shell commands, or destructive

  filesystem operations.



============================================================

FINAL TARGET WRITE

============================================================



Write EVERY declared target using its exact physical table and its declared operation.

MANDATORY operation rules:
- For operation UPSERT or MERGE, the receiver of .merge(...) MUST be a DeltaTable object obtained from DeltaTable.forName(spark, exact_target), optionally followed by .alias("t"). Use: DeltaTable.forName(spark, exact_target).alias("t").merge(source_df.alias("s"), <predicate made only from target.merge_keys>).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute().
- NEVER call .merge(...) on a DataFrame (for example df.alias("s").merge(...)); DataFrame.merge is not the Delta Lake MERGE API. The source DataFrame is the argument to DeltaTable.merge(...).
- The exact target name may be a string literal or a simple string variable, but it must equal the target's physical_table metadata exactly, character-for-character.
- Never shorten or omit catalog/schema components. For example, if physical_table is "idmc_poc.curated.customer_low_value", do not use "curated.customer_low_value" in DeltaTable.forName.
- Never use .mode("append").saveAsTable(), insert append, overwrite, or saveAsTable as a substitute for UPSERT/MERGE.
- If merge_keys is absent or empty, do not invent a key and do not generate an append fallback. Record a blocking item in assumptions_or_gaps and leave that target unwritten. The migration must remain non-executable until the real key is supplied.
- For APPEND/INSERT, use append only when that exact operation is declared.
- For OVERWRITE, use overwrite only when that exact operation is declared.
- Do not generate a generic saveAsTable example for UPSERT/MERGE targets.
- Target schemas and column mappings must come from metadata; never infer target schema from an existing physical table and silently override IDMC metadata.

Before each write, validate the target DataFrame columns against the supplied target columns. If target columns are absent from metadata, record a blocker and do not claim the target is executable.



============================================================

MISSING INFORMATION

============================================================



If metadata is incomplete:



- Do not invent business logic.

- Clearly record the issue in assumptions_or_gaps.

- Generate code only when the intended semantics are sufficiently

  clear.

- If target datatype metadata is unavailable, record that limitation

  in assumptions_or_gaps and do not invent a datatype.



============================================================

REQUIRED JSON

============================================================



Return ONLY valid JSON.



Required structure:



{{

    "mapping_name": "mapping name",



    "migration_plan": [

        {{

            "step": 1,

            "node": "node name",

            "type": "transformation type",

            "description": "what this step does"

        }}

    ],



    "transformation_logic": [

        {{

            "node": "node name",

            "type": "transformation type",

            "logic": "PySpark implementation logic"

        }}

    ],



    "assumptions_or_gaps": [

        "missing or ambiguous metadata"

    ],



    "pyspark_code": "complete executable PySpark code"

}}



============================================================

IDMC METADATA

============================================================



{metadata_json}

"""



        prompt += '\n\nTARGET WRITE SAFETY AND STATIC VALIDATION CONTRACT:\n- Never hide target writes inside a generic helper such as merge_into(target_table, df),\n  and never pass a target name through a function parameter. Emit each target write as an\n  explicit DeltaTable.forName(spark, "exact.catalog.schema.table").alias("t").merge(...)\n  chain in the main migration function, so static validation can resolve the exact target.\n- Do not use a merge key whose merge_key_status (at target level or attributes) is\n  POC_ASSUMPTION, ASSUMED, UNVERIFIED, or UNKNOWN. Such a key is not authoritative: leave\n  that target unwritten and list it as a blocking gap. Do not silently promote the status.\n- For approved merge keys, generate one explicit MERGE per declared target and use only that\n  target\'s supplied key columns in its predicate. Do not remove targets to avoid validation.\n- For string concatenation in PySpark, use F.concat/F.concat_ws or an explicitly approved\n  expression; do not use Column \'+\' to concatenate strings. Preserve null semantics from metadata\n  and report a gap if null handling is unspecified.\n- Do not claim success if any required target write is blocked or validation fails.\n'
        return prompt



    # ============================================================

    # TARGET SCHEMA EXTRACTION

    # ============================================================



    @staticmethod

    def _extract_target_schema(llm_input):

        """

        Extract target column names and datatypes from normalized

        IDMC metadata.



        Supported metadata patterns include:



            {

                "name": "TOTAL_REVENUE",

                "datatype": "DECIMAL",

                "precision": 18,

                "scale": 2

            }



        or:



            {

                "name": "TOTAL_REVENUE",

                "data_type": "DECIMAL(18,2)"

            }



        or:



            {

                "name": "TOTAL_REVENUE",

                "type": "DECIMAL"

            }

        """



        columns = []



        targets = llm_input.get("targets", [])



        if not isinstance(targets, list):

            return columns



        for target in targets:

            if not isinstance(target, dict):

                continue



            # ----------------------------------------------------

            # Ports

            # ----------------------------------------------------



            ports = target.get("ports", [])



            if isinstance(ports, list):

                for port in ports:

                    if not isinstance(port, dict):

                        continue



                    item = LLMGenerator._parse_target_column(port)



                    if item:

                        LLMGenerator._append_unique_target_column(

                            columns,

                            item

                        )



            # ----------------------------------------------------

            # Columns

            # ----------------------------------------------------



            target_columns = target.get("columns", [])



            if isinstance(target_columns, list):

                for column in target_columns:



                    if isinstance(column, dict):

                        item = LLMGenerator._parse_target_column(

                            column

                        )

                    else:

                        item = {

                            "name": str(column),

                            "datatype": "UNKNOWN"

                        }



                    if item:

                        LLMGenerator._append_unique_target_column(

                            columns,

                            item

                        )



        return columns



    @staticmethod

    def _parse_target_column(column):

        """

        Normalize one target column definition.

        """



        if not isinstance(column, dict):

            return None



        name = (

            column.get("name")

            or column.get("column_name")

            or column.get("port_name")

        )



        if not name:

            return None



        datatype = (

            column.get("datatype")

            or column.get("data_type")

            or column.get("type")

            or column.get("sql_type")

            or column.get("spark_type")

        )



        # --------------------------------------------------------

        # Handle nested datatype definitions

        # --------------------------------------------------------



        if isinstance(datatype, dict):

            datatype_name = (

                datatype.get("name")

                or datatype.get("type")

                or datatype.get("datatype")

                or datatype.get("data_type")

            )



            precision = (

                datatype.get("precision")

                or column.get("precision")

            )



            scale = (

                datatype.get("scale")

                or column.get("scale")

            )



            if datatype_name:

                datatype = str(datatype_name)



                if (

                    str(datatype).upper() in {

                        "DECIMAL",

                        "NUMERIC"

                    }

                    and precision is not None

                    and scale is not None

                ):

                    datatype = (

                        f"{datatype}({precision},{scale})"

                    )



        if datatype is None:

            datatype = "UNKNOWN"



        datatype = str(datatype).strip()



        # --------------------------------------------------------

        # Add precision / scale if supplied separately

        # --------------------------------------------------------



        precision = column.get("precision")

        scale = column.get("scale")



        upper_datatype = datatype.upper()



        if (

            upper_datatype in {"DECIMAL", "NUMERIC"}

            and precision is not None

            and scale is not None

        ):

            datatype = (

                f"{datatype}({precision},{scale})"

            )



        return {

            "name": str(name),

            "datatype": datatype

        }



    @staticmethod

    def _append_unique_target_column(columns, item):

        """

        Add a target column while preserving target metadata order.

        If the same column appears multiple times, keep the first

        occurrence unless the first occurrence has UNKNOWN datatype.

        """



        name = item["name"]



        for existing in columns:

            if existing["name"] == name:



                if (

                    existing["datatype"] == "UNKNOWN"

                    and item["datatype"] != "UNKNOWN"

                ):

                    existing["datatype"] = item["datatype"]



                return



        columns.append(item)



    # ============================================================

    # BACKWARD-COMPATIBLE TARGET COLUMN EXTRACTION

    # ============================================================



    @staticmethod

    def _extract_target_columns(llm_input):

        """

        Return only target column names.



        Kept for compatibility with existing code that may call

        _extract_target_columns().

        """



        target_schema = LLMGenerator._extract_target_schema(

            llm_input

        )



        return [

            item["name"]

            for item in target_schema

        ]



    # ============================================================

    # CALL LLM

    # ============================================================



    def _call_llm(self, prompt, max_tokens=None):

        requested_max_tokens = max_tokens if max_tokens is not None else self.max_tokens
        response = self.client.chat.completions.create(

            model=self.model,

            messages=[

                {

                    "role": "system",

                    "content": (

                        "You are an expert IDMC to Databricks "

                        "migration engineer. "

                        "Return strictly valid JSON. "

                        "Target schema datatypes are authoritative. "

                        "Generated PySpark must explicitly conform "

                        "to those target datatypes."

                    )

                },

                {

                    "role": "user",

                    "content": prompt

                }

            ],

            temperature=self.temperature,

            max_tokens=requested_max_tokens

        )



        if not response.choices:

            raise RuntimeError("LLM returned no choices.")



        choice = response.choices[0]
        content = choice.message.content
        finish_reason = getattr(choice, "finish_reason", None)

        if finish_reason == "length":
            raise ValueError(
                f"LLM response hit the output-token limit (finish_reason={finish_reason}, "
                f"requested max_tokens={requested_max_tokens}). The generator will retry "
                "with a compact JSON response instruction."
            )

        if content is None:

            raise RuntimeError("LLM returned empty content.")



        if isinstance(content, list):

            text_parts = []



            for item in content:

                if isinstance(item, str):

                    text_parts.append(item)



                elif isinstance(item, dict):

                    if "text" in item:

                        text_parts.append(str(item["text"]))



                else:

                    item_text = getattr(item, "text", None)



                    if item_text:

                        text_parts.append(str(item_text))



            content = "".join(text_parts)



        if not isinstance(content, str):

            content = str(content)



        content = content.strip()



        if not content:

            raise RuntimeError("LLM returned empty content.")



        return content



    # ============================================================

    # PARSE RESPONSE

    # ============================================================



    def _parse_response(self, response):

        if isinstance(response, dict):

            return response



        if not isinstance(response, str):

            response = str(response)



        text = response.strip()



        # Remove markdown JSON fences if the model ignores

        # the instruction and returns them.

        text = re.sub(

            r"^\s*\`\`\`json\s*",

            "",

            text,

            flags=re.IGNORECASE

        )



        text = re.sub(

            r"^\s*\`\`\`\s*",

            "",

            text

        )



        text = re.sub(

            r"\s*\`\`\`\s*$",

            "",

            text

        )



        text = text.strip()



        try:

            return json.loads(text)



        except json.JSONDecodeError:

            pass



        # Fallback: extract the outer JSON object.

        start = text.find("{")

        end = text.rfind("}")



        if start == -1 or end == -1:

            raise ValueError(

                "LLM did not return JSON.\n\n"

                f"Response:\n{text}"

            )



        candidate = text[start:end + 1]



        try:

            return json.loads(candidate)



        except json.JSONDecodeError as error:

            raise ValueError(

                "Unable to parse LLM JSON response.\n\n"

                f"Response:\n{text}\n\n"

                f"Error:\n{error}"

            )



    # ============================================================

    # NORMALIZE PYSPARK

    # ============================================================



    @staticmethod

    def _normalize_pyspark_code(pyspark_code):

        if not isinstance(pyspark_code, str):

            raise ValueError("pyspark_code must be a string.")



        code = pyspark_code.strip()



        code = re.sub(

            r"^\s*\`\`\`(?:python|py|pyspark)?\s*",

            "",

            code,

            flags=re.IGNORECASE

        )



        code = re.sub(

            r"\s*\`\`\`\s*$",

            "",

            code

        )



        code = code.strip()



        # Some model responses contain the entire program as one string,
        # with literal backslash-n sequences instead of real line breaks.
        # Decode those sequences only when the source is effectively one line,
        # so normal Python string literals and regex escapes are not altered.
        if "\\n" in code and code.count("\n") <= 2:
            code = code.replace("\\\\r\\\\n", "\n")
            code = code.replace("\\\\n", "\n")
            code = code.replace("\\\\t", "\t")
            code = code.replace("\\\\r", "\n")
            code = code.replace("\\n", "\n")
            code = code.replace("\\t", "\t")
            code = code.replace("\\r", "\n")

        # Handle accidental escaped line-continuation sequences.
        code = code.replace("\\\\\r\n", "\n")
        code = code.replace("\\\\\n", "\n")
        code = code.replace("\\\\\r", "\n")
        code = code.replace("\\\\\t", "\t")

        # Remove accidental quotes around the entire source.

        if (

            len(code) >= 2

            and (

                (code.startswith('"') and code.endswith('"'))

                or

                (code.startswith("'") and code.endswith("'"))

            )

        ):

            code = code[1:-1]



        return code.strip()



    # ============================================================

    # RESPONSE VALIDATION

    # ============================================================



    def _validate_response(self, result):

        if not isinstance(result, dict):

            raise ValueError(

                "LLM response must be a JSON object."

            )



        required_fields = [

            "mapping_name",

            "migration_plan",

            "transformation_logic",

            "assumptions_or_gaps",

            "pyspark_code"

        ]



        missing_fields = [

            field

            for field in required_fields

            if field not in result

        ]



        if missing_fields:

            raise ValueError(

                "LLM response missing fields: "

                + ", ".join(missing_fields)

            )



        if not isinstance(result["mapping_name"], str):

            raise ValueError(

                "mapping_name must be a string."

            )



        if not isinstance(result["migration_plan"], list):

            raise ValueError(

                "migration_plan must be a list."

            )



        if not isinstance(

            result["transformation_logic"],

            list

        ):

            raise ValueError(

                "transformation_logic must be a list."

            )



        if not isinstance(

            result["assumptions_or_gaps"],

            list

        ):

            raise ValueError(

                "assumptions_or_gaps must be a list."

            )



        pyspark_code = result["pyspark_code"]



        if not isinstance(pyspark_code, str):

            raise ValueError(

                "pyspark_code must be a string."

            )



        if not pyspark_code.strip():

            raise ValueError(

                "LLM generated empty PySpark code."

            )



        normalized_code = self._normalize_pyspark_code(

            pyspark_code

        )



        if not normalized_code:

            raise ValueError(

                "LLM generated empty PySpark code after normalization."

            )



        result["pyspark_code"] = normalized_code



        try:

            ast.parse(

                normalized_code,

                filename="<llm_generated_pyspark>"

            )



        except SyntaxError as error:

            preview = normalized_code[:3000]



            raise ValueError(

                "LLM generated invalid Python syntax:\n"

                f"{error}\n\n"

                "Generated PySpark preview:\n"

                f"{preview}"

            )



    # ============================================================

    # PRINT RESULT

    # ============================================================



    @staticmethod

    def print_result(result):

        print("\nLLM GENERATION RESULT")

        print("=" * 70)



        print(

            "Mapping :",

            result.get("mapping_name")

        )



        print(

            "Migration Steps :",

            len(result.get("migration_plan", []))

        )



        print(

            "Transformation Logic :",

            len(result.get("transformation_logic", []))

        )



        print(

            "Assumptions / Gaps :",

            len(result.get("assumptions_or_gaps", []))

        )



        print("PySpark Code : GENERATED")



        assumptions = result.get(

            "assumptions_or_gaps",

            []

        )



        if assumptions:

            print("\nASSUMPTIONS / GAPS")



            for gap in assumptions:

                print("  -", gap)



        print("\nMIGRATION PLAN")



        for step in result.get("migration_plan", []):

            print(

                f"  {step.get('step')}. "

                f"{step.get('node')} | "

                f"{step.get('type')}"

            )


    # ============================================================
    # TASKFLOW ANALYSIS (SEPARATE FROM MAPPING/PYSPARK GENERATION)
    # ============================================================

    @staticmethod
    def _compact_taskflow_input(taskflow_input):
        """Bound Taskflow evidence while retaining every link record."""
        if not isinstance(taskflow_input, dict):
            raise TypeError("taskflow_input must be a dictionary.")

        parser_evidence = taskflow_input.get("parser_evidence") or {}
        graph = taskflow_input.get("orchestration_graph") or {}
        validation = taskflow_input.get("validation") or {}
        if not isinstance(parser_evidence, dict) or not isinstance(graph, dict):
            raise ValueError("Taskflow input is missing parser_evidence/orchestration_graph.")

        def compact_service(item):
            if not isinstance(item, dict):
                return {"value": str(item)[:300]}
            keys = (
                "id", "name", "title", "service_name", "service_type",
                "parent_id", "flow_id"
            )
            return {key: item.get(key) for key in keys if item.get(key) is not None}

        def compact_link(item):
            if not isinstance(item, dict):
                return {"raw": str(item)[:500]}
            keys = (
                "link_id", "flow_id", "owner_id", "owner_type",
                "container_context", "source_id", "target_id", "edge_type",
                "condition_task_references", "source_resolution",
                "source_confirmed_by_xml", "target_confirmed_by_xml", "reason"
            )
            result = {key: item.get(key) for key in keys if item.get(key) is not None}
            condition = item.get("condition_text")
            if condition:
                result["condition_text"] = str(condition)[:1500]
            return result

        def compact_edge(item):
            if not isinstance(item, dict):
                return {"raw": str(item)[:300]}
            return {
                key: item.get(key)
                for key in ("source_id", "target_id", "link_id", "edge_type",
                            "source_resolution", "confirmed", "source_confirmed_by_xml")
                if item.get(key) is not None
            }

        links = graph.get("links") or []
        if not links:
            links = (graph.get("inferred_dependency_edges") or []) + (
                graph.get("unresolved_links") or []
            )
        compact_links = [compact_link(item) for item in links]

        # Preserve every link. Fail closed rather than silently sending a partial graph.
        expected_link_count = len(links)
        if len(compact_links) != expected_link_count:
            raise ValueError("Taskflow compaction did not preserve all link records.")

        compact = {
            "asset_type": taskflow_input.get("asset_type", "IDMC_TASKFLOW"),
            "taskflow": taskflow_input.get("taskflow") or {},
            "parser_evidence": {
                "parameters": parser_evidence.get("parameters", []),
                "temp_fields": parser_evidence.get("temp_fields", []),
                "services": [
                    compact_service(item)
                    for item in (parser_evidence.get("services") or [])
                ],
                "flow_summaries": parser_evidence.get("flow_summaries", []),
                "node_type_counts": parser_evidence.get("node_type_counts", {}),
            },
            "orchestration_graph": {
                "containment_edges": [
                    compact_edge(item)
                    for item in (graph.get("containment_edges") or [])
                ],
                "inferred_dependency_edges": [
                    compact_link(item)
                    for item in (graph.get("inferred_dependency_edges") or [])
                ],
                "condition_dependency_evidence": graph.get(
                    "condition_dependency_evidence", []
                ),
                "links": compact_links,
                "unresolved_links": [
                    compact_link(item)
                    for item in (graph.get("unresolved_links") or [])
                ],
            },
            "validation": validation,
            "known_limitations": taskflow_input.get("known_limitations", []),
            "input_integrity": {
                "link_count_supplied": expected_link_count,
                "link_count_retained": len(compact_links),
                "all_links_retained": len(compact_links) == expected_link_count,
            },
        }
        if not compact["input_integrity"]["all_links_retained"]:
            raise ValueError("Not all Taskflow links were retained during compaction.")
        return compact

    def generate_taskflow_analysis(self, taskflow_input, max_tokens=6000):
        """
        Ask the configured Databricks model for Taskflow migration analysis.
        This method does not generate mapping PySpark and does not execute anything.
        Existing generate() behavior remains unchanged.
        """
        compact_input = self._compact_taskflow_input(taskflow_input)
        evidence_json = json.dumps(
            compact_input, ensure_ascii=False, separators=(",", ":"), default=str
        )

        prompt = f"""
You are a senior Informatica IDMC-to-Databricks migration architect.

Analyze the supplied IDMC TASKFLOW export. This is an orchestration analysis only.
Do not generate PySpark mapping code. Do not execute or claim to execute any task.

EVIDENCE RULES:
- Use only the supplied evidence. Separate XML facts, inferred relationships,
  condition-reference evidence, and assumptions.
- Every supplied link record is included. Account for the full link inventory
  using counts and classifications; do not claim a source relationship is XML-confirmed.
- The XML links have id and targetId but no sourceId. A preceding-activity
  relationship is inferred, never XML-confirmed.
- Condition references to $temp task outputs are evidence of condition inputs,
  not proof of a link's source.
- The XML is a Taskflow export, not a complete mapping-definition export.
  Mapping tasks may be named, but their internal mapping transformations,
  columns, SQL, merge keys, and business rules are not supplied here.
- Do not invent missing task names, execution order, branch outcomes, schemas,
  parameters, command behavior, or mapping logic.
- Identify unresolved links and explain the impact of missing source information.
- Distinguish data-task services from command-task services.
- Discuss exclusive/parallel containers, event/catch handlers, assignments,
  conditions and taskflow parameters only where present in the evidence.
- Recommend a Databricks Jobs/Workflows design at orchestration level.
  Treat command tasks as external-command integration candidates requiring
  implementation details/credentials/host context; do not fabricate commands.
- State clearly what can be migrated from this XML and what requires mapping
  exports or additional evidence.
- Do not say migration is complete or production-ready unless the evidence proves it.

Return ONLY one valid JSON object with exactly these top-level keys:
{{
  "taskflow_summary": {{
    "taskflow_name": "",
    "taskflow_guid": "",
    "asset_type": "IDMC_TASKFLOW",
    "observed_counts": {{}},
    "purpose_and_scope": ""
  }},
  "orchestration_analysis": {{
    "confirmed_xml_facts": [],
    "inferred_relationships": [],
    "condition_reference_evidence": [],
    "branch_and_container_analysis": [],
    "error_handling_analysis": [],
    "link_accounting": {{
      "total_links_received": 0,
      "links_with_inferred_sources": 0,
      "links_with_unresolved_sources": 0,
      "links_with_missing_targets": 0,
      "source_ids_present_in_xml": false
    }}
  }},
  "service_migration_plan": [
    {{
      "service_name": "",
      "service_type": "",
      "observed_role": "",
      "databricks_migration_candidate": "",
      "required_evidence_or_blocker": ""
    }}
  ],
  "databricks_target_design": {{
    "workflow_structure": [],
    "task_dependencies": [],
    "parameters_and_configuration": [],
    "retry_and_failure_handling": [],
    "external_command_handling": [],
    "implementation_notes": []
  }},
  "assumptions_and_gaps": [],
  "readiness_status": {{
    "status": "REVIEW_REQUIRED",
    "can_design_orchestration": true,
    "can_generate_exact_mapping_logic_from_this_xml": false,
    "can_execute_migration": false,
    "blocking_gaps": []
  }},
  "next_actions": []
}}

Keep the response concise but specific. Use empty arrays where evidence is absent.
Do not add markdown fences or prose outside JSON.

TASKFLOW EVIDENCE JSON:
{evidence_json}
"""

        response_text = self._call_llm(
            prompt, max_tokens=min(int(max_tokens), self.max_tokens)
        )
        result = self._parse_response(response_text)
        if not isinstance(result, dict):
            raise ValueError("Taskflow LLM response must be a JSON object.")

        required_keys = {
            "taskflow_summary", "orchestration_analysis",
            "service_migration_plan", "databricks_target_design",
            "assumptions_and_gaps", "readiness_status", "next_actions"
        }
        missing = sorted(required_keys - set(result))
        if missing:
            raise ValueError(
                "Taskflow LLM response is missing required keys: "
                + ", ".join(missing)
            )

        # Attach traceability metadata from the actual input, not model claims.
        result["_generation_metadata"] = {
            "model": self.model,
            "taskflow_name": (compact_input.get("taskflow") or {}).get("name"),
            "input_link_count": compact_input["input_integrity"]["link_count_retained"],
            "all_input_links_retained": compact_input["input_integrity"]["all_links_retained"],
            "taskflow_execution_performed": False,
            "pyspark_generation_performed": False,
        }
        return result

    @staticmethod
    def print_taskflow_result(result):
        """Print a compact summary of a Taskflow analysis response."""
        summary = result.get("taskflow_summary") or {}
        orchestration = result.get("orchestration_analysis") or {}
        link_accounting = orchestration.get("link_accounting") or {}
        readiness = result.get("readiness_status") or {}

        print("\\nTASKFLOW LLM ANALYSIS")
        print("=" * 70)
        print("Taskflow:", summary.get("taskflow_name"))
        print("Services in migration plan:", len(result.get("service_migration_plan", [])))
        print("Links received:", link_accounting.get("total_links_received"))
        print("Inferred-source links:", link_accounting.get("links_with_inferred_sources"))
        print("Unresolved-source links:", link_accounting.get("links_with_unresolved_sources"))
        print("Readiness:", readiness.get("status"))
        print("Execution performed: NO")
        print("\\nASSUMPTIONS / GAPS")
        for item in result.get("assumptions_and_gaps", []):
            print("-", item)
        print("\\nNEXT ACTIONS")
        for item in result.get("next_actions", []):
            print("-", item)
