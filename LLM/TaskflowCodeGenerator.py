"""Best-effort IDMC Taskflow XML -> Databricks PySpark code generator.

This generator uses parsed Taskflow evidence available in the supplied XML.
It generates evidence-backed PySpark where possible and reports missing evidence
instead of inventing business logic. It does NOT claim equivalence to the original
IDMC mapping or count unassigned operations as code coverage.

Expected input: TaskflowLLMInputBuilder.build(parsed_taskflow, taskflow_graph)
Expected output: generated_<taskflow>.py + taskflow_generation_report.json
"""
from __future__ import annotations

import ast
import json
import re
from pathlib import Path
from typing import Any, Dict, List, Tuple

from openai import OpenAI


DEFAULT_OUTPUT_DIR = "/Volumes/idmc_poc/bronze/idmc_files"


class TaskflowCodeGenerator:
    def __init__(
        self,
        workspace_url: str,
        databricks_token: str,
        model: str = "databricks-gpt-oss-120b",
        max_tokens: int = 24000,
        temperature: float = 0.1,
    ) -> None:
        if not workspace_url or not databricks_token:
            raise ValueError("workspace_url and databricks_token are required")
        self.workspace_url = workspace_url.rstrip("/")
        self.model = model
        self.max_tokens = int(max_tokens)
        self.temperature = float(temperature)
        self.client = OpenAI(
            api_key=databricks_token,
            base_url=f"{self.workspace_url}/serving-endpoints",
        )

    @staticmethod
    def _dict(value: Any) -> Dict[str, Any]:
        return value if isinstance(value, dict) else {}

    @staticmethod
    def _list(value: Any) -> List[Any]:
        return value if isinstance(value, list) else []

    @staticmethod
    def _clean(value: Any) -> Any:
        """Remove raw XML blobs but retain all parsed semantic evidence."""
        if isinstance(value, dict):
            return {
                str(k): TaskflowCodeGenerator._clean(v)
                for k, v in value.items()
                if str(k).lower() not in {"raw_xml", "xml_text", "source_xml"}
            }
        if isinstance(value, list):
            return [TaskflowCodeGenerator._clean(v) for v in value]
        if isinstance(value, tuple):
            return [TaskflowCodeGenerator._clean(v) for v in value]
        if isinstance(value, (str, int, float, bool)) or value is None:
            return value
        return str(value)

    @staticmethod
    def _safe_identifier(value: Any, fallback: str = "task") -> str:
        text = re.sub(r"[^A-Za-z0-9_]+", "_", str(value or ""))
        text = re.sub(r"_+", "_", text).strip("_").lower()
        if not text:
            text = fallback
        if text[0].isdigit():
            text = "n_" + text
        return text

    @classmethod
    def compact_input(cls, taskflow_input: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize the builder payload without dropping parsed operations."""
        if not isinstance(taskflow_input, dict):
            raise TypeError("taskflow_input must be a dictionary")

        # Builder's current shape is {taskflow, parser_evidence, orchestration_graph}.
        taskflow = cls._dict(taskflow_input.get("taskflow"))
        parser = cls._dict(taskflow_input.get("parser_evidence"))
        graph = cls._dict(taskflow_input.get("orchestration_graph"))

        metadata = cls._dict(parser.get("taskflow_metadata"))
        if not metadata:
            metadata = cls._dict(taskflow_input.get("parsed_taskflow"))
        if not metadata:
            metadata = taskflow

        services = cls._list(parser.get("services"))
        graph_nodes = cls._list(parser.get("graph_nodes"))
        if not services:
            services = [
                n for n in graph_nodes
                if isinstance(n, dict) and n.get("element_type") == "service"
            ]
        if not graph_nodes:
            graph_nodes = cls._list(graph.get("nodes"))

        operations = cls._list(parser.get("operations"))
        if not operations:
            operations = cls._list(metadata.get("operations"))
        control_nodes = cls._list(parser.get("control_flow_nodes"))
        if not control_nodes:
            control_nodes = [
                n for n in graph_nodes
                if isinstance(n, dict)
                and str(n.get("element_type", "")).lower() in {
                    "control", "control_flow", "decision", "assignment", "condition"
                }
            ]

        links = cls._list(graph.get("links"))
        containment = cls._list(graph.get("containment_edges"))
        inferred = cls._list(graph.get("inferred_dependency_edges"))
        condition_edges = cls._list(graph.get("condition_dependency_evidence"))
        unresolved = cls._list(graph.get("unresolved_links"))
        parameters = cls._list(parser.get("parameters"))
        temp_fields = cls._list(parser.get("temp_fields"))
        flows = cls._list(parser.get("flow_summaries"))

        # Use the builder's values if they were supplied separately.
        compact = {
            "asset_type": taskflow_input.get("asset_type", "IDMC_TASKFLOW"),
            "taskflow": cls._clean(taskflow),
            "taskflow_metadata": cls._clean(metadata),
            "services": cls._clean(services),
            "operations": cls._clean(operations),
            "control_flow_nodes": cls._clean(control_nodes),
            "graph_nodes": cls._clean(graph_nodes),
            "parameters": cls._clean(parameters),
            "temp_fields": cls._clean(temp_fields),
            "flows": cls._clean(flows),
            "links": cls._clean(links),
            "containment_edges": cls._clean(containment),
            "inferred_dependency_edges": cls._clean(inferred),
            "condition_dependency_evidence": cls._clean(condition_edges),
            "unresolved_links": cls._clean(unresolved),
            "validation": cls._clean(cls._dict(taskflow_input.get("validation"))),
            "input_integrity": cls._clean(cls._dict(taskflow_input.get("input_integrity"))),
            "known_limitations": cls._clean(cls._list(taskflow_input.get("known_limitations"))),
        }

        # Stable IDs are assigned for reporting only; original objects remain intact.
        operation_inventory = []
        for index, operation in enumerate(operations, start=1):
            op = cls._dict(operation)
            op_id = (op.get("id") or op.get("guid") or op.get("name")
                     or op.get("operation_id") or f"operation_{index:03d}")
            operation_inventory.append({"ref": str(op_id), "index": index, "operation": op})
        compact["operation_inventory"] = operation_inventory

        compact["counts"] = {
            "service_count": len(services),
            "data_task_count": sum(cls._is_data_task(s) for s in services),
            "command_task_count": sum(cls._is_command_task(s) for s in services),
            "operation_count": len(operations),
            "control_flow_node_count": len(control_nodes),
            "graph_node_count": len(graph_nodes),
            "parameter_count": len(parameters),
            "temp_field_count": len(temp_fields),
            "link_count": len(links),
            "containment_edge_count": len(containment),
            "inferred_dependency_count": len(inferred),
            "condition_dependency_count": len(condition_edges),
            "unresolved_link_count": len(unresolved),
        }
        return compact

    @staticmethod
    def _service_type(service: Dict[str, Any]) -> str:
        values = " ".join(str(service.get(k, "")) for k in
                           ("service_name", "service_type", "type", "name", "title")).lower()
        if "icsexecutedatatask" in values or "data_task" in values or "mappingtask" in values:
            return "DATA_TASK"
        if "icsexecutecommandtask" in values or "command_task" in values:
            return "COMMAND_TASK"
        return "OTHER_SERVICE"

    @classmethod
    def _is_data_task(cls, service: Any) -> bool:
        return isinstance(service, dict) and cls._service_type(service) == "DATA_TASK"

    @classmethod
    def _is_command_task(cls, service: Any) -> bool:
        return isinstance(service, dict) and cls._service_type(service) == "COMMAND_TASK"

    @classmethod
    def _service_manifest(cls, compact: Dict[str, Any]) -> List[Dict[str, Any]]:
        manifest = []
        seen = set()
        for index, service in enumerate(compact["services"], start=1):
            if not isinstance(service, dict):
                continue
            sid = str(service.get("id") or service.get("guid") or f"service_{index:02d}")
            title = (service.get("title") or service.get("name")
                     or service.get("service_name") or sid)
            function = "execute_" + cls._safe_identifier(title, f"service_{index:02d}")
            if function in seen:
                function += "_" + cls._safe_identifier(sid)
            seen.add(function)
            manifest.append({
                "service_id": sid,
                "title": str(title),
                "service_type": cls._service_type(service),
                "function_name": function,
                "service": service,
            })
        return manifest

    def _call_llm(self, prompt: str) -> str:
        response = self.client.chat.completions.create(
            model=self.model,
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You are a senior Informatica IDMC to Databricks migration engineer. "
                        "Your task is to generate best-effort, runnable PySpark migration code "
                        "from the supplied Taskflow XML parser evidence. Infer reasonable "
                        "implementation details when the XML is incomplete, but clearly label "
                        "every inference and assumption. Never return generic orchestration "
                        "wrappers instead of PySpark transformations."
                    ),
                },
                {"role": "user", "content": prompt},
            ],
            temperature=self.temperature,
            max_tokens=self.max_tokens,
        )
        if not response.choices:
            raise RuntimeError("Databricks model returned no choices")
        choice = response.choices[0]
        if getattr(choice, "finish_reason", None) == "length":
            raise ValueError("LLM response was truncated; increase max_tokens or reduce evidence size")
        content = choice.message.content
        if isinstance(content, list):
            content = "".join(
                part if isinstance(part, str) else str(getattr(part, "text", ""))
                for part in content
            )
        if not isinstance(content, str) or not content.strip():
            raise ValueError("Databricks model returned empty content")
        return content.strip()

    @staticmethod
    def _extract_code(response: str) -> str:
        match = re.search(r"```(?:python|pyspark|py)?\s*(.*?)```", response.strip(),
                          flags=re.IGNORECASE | re.DOTALL)
        return (match.group(1).strip() if match else response.strip()) + "\n"

    @classmethod
    def _build_prompt(cls, compact: Dict[str, Any], manifest: List[Dict[str, Any]]) -> str:
        taskflow_name = compact["taskflow"].get("name") or "IDMC_TASKFLOW"
        data_tasks = [m for m in manifest if m["service_type"] == "DATA_TASK"]
        command_tasks = [m for m in manifest if m["service_type"] == "COMMAND_TASK"]
        evidence_json = json.dumps(compact, ensure_ascii=False, default=str)
        manifest_json = json.dumps(manifest, ensure_ascii=False, default=str)
        return f'''Generate the COMPLETE PySpark implementation for IDMC Taskflow: {taskflow_name}

IMPORTANT BUSINESS REQUIREMENT
The objective is BEST-EFFORT CODE GENERATION, not perfect semantic reconstruction.
Use every useful clue in the supplied Taskflow XML evidence. Infer likely transformations,
source/target handling, parameter usage, conditional behavior and orchestration where
reasonable. Do not stop merely because mapping XML is unavailable. The generated code
may be approximate; disclose assumptions and measure coverage honestly.

OUTPUT CONTRACT
Return ONLY Python source code (no markdown fences). Generate actual PySpark DataFrame
code, not a generic Python task-dispatch framework. Import:
  from pyspark.sql import SparkSession, DataFrame
  from pyspark.sql import functions as F
  from pyspark.sql.window import Window

REQUIRED CODE CONTENT
1. Generate one function for EVERY manifest service using the exact function_name shown.
2. For EVERY DATA_TASK service, generate a real PySpark function that:
   - accepts spark and params;
   - attempts to resolve source table/path and target table/path from task/service metadata,
     taskflow parameters, operation names, and other available clues;
   - loads source data with spark.table or spark.read;
   - generates the most plausible transformation chain supported or suggested by the
     parsed operation names, expressions, assignments, conditions and task properties;
   - uses real DataFrame APIs: select, withColumn, filter, join, groupBy/agg, window,
     dropDuplicates, unionByName, etc. as appropriate;
   - writes output to the resolved target when a target can be determined or supplied
     in params; otherwise returns the transformed DataFrame and records target as an
     assumption/configuration requirement;
   - avoids raising NotImplementedError merely because some fields are missing. Use
     configurable params and reasonable inferred defaults, with explicit comments.
3. Generate functions for COMMAND_TASK services too. Do not execute shell commands;
   expose a safe command specification/configuration placeholder and report it.
4. Preserve all parsed operation IDs/names in comments in generated functions using this
   exact marker format, one marker per operation:
       # IDMC_OPERATION_REF: <operation-ref> | STATUS:IMPLEMENTED
       # IDMC_OPERATION_REF: <operation-ref> | STATUS:INFERRED
       # IDMC_OPERATION_REF: <operation-ref> | STATUS:UNSUPPORTED
   Use IMPLEMENTED when an operation is directly represented, INFERRED when approximated,
   UNSUPPORTED only when no reasonable PySpark representation can be generated.
   Include a marker for EVERY operation inventory entry. Do not omit operation markers.
5. Include a module-level ASSUMPTIONS list describing inferred source/target names,
   columns, joins, filters, aggregation rules, expressions and orchestration assumptions.
6. Include a module-level UNSUPPORTED_ITEMS list.
7. Include a module-level SERVICE_IMPLEMENTATION_STATUS dictionary keyed by service ID,
   with GENERATED / PARTIAL / UNSUPPORTED and a short explanation.
8. Include taskflow parameter and temporary-field metadata in a PARAMS_METADATA constant.
9. Include a main function that accepts spark and params. It may execute generated task
   functions only in the order suggested by available flow/link/condition evidence. If
   ordering is ambiguous, do not invent a definitive sequence: run independent data-task
   functions only when caller supplies task_order, otherwise return a plan and assumptions.
10. Do not include a fake `select('*')` as the only transformation. Do not claim data
    validation or successful execution. Do not use eval, exec, compile, __import__,
    subprocess, os.system, or shell execution.
11. Make the file syntactically valid and include all functions and operation markers.

SERVICE MANIFEST:
{manifest_json}

COMPLETE PARSED TASKFLOW EVIDENCE (authoritative; use all of it):
{evidence_json}
'''


    @classmethod
    def _service_evidence(cls, compact, manifest_item, data_task_index, total_data_tasks):
        service = manifest_item.get("service", {})
        sid = str(manifest_item.get("service_id", ""))
        title = str(manifest_item.get("title", ""))

        def mentions(item):
            blob = json.dumps(item, ensure_ascii=False, default=str).lower()
            return bool((sid and sid.lower() in blob) or (title and title.lower() in blob))

        operations = compact.get("operations", [])
        matched_ops = [op for op in operations if mentions(op)]
        explicit_match = bool(matched_ops)
        # Never assign operations to a service by arbitrary list slicing.
        # The Taskflow export may not expose operation ownership. In that case,
        # leave the operation set empty and report the ownership gap explicitly.

        return {
            "taskflow": compact.get("taskflow", {}),
            "service": service,
            "service_manifest": manifest_item,
            "parameters": compact.get("parameters", []),
            "temp_fields": compact.get("temp_fields", []),
            "operations_for_this_task": matched_ops,
            "control_flow_evidence": [x for x in compact.get("control_flow_nodes", []) if mentions(x)][:35],
            "related_graph_nodes": [x for x in compact.get("graph_nodes", []) if mentions(x)][:35],
            "related_links": ([x for x in compact.get("links", []) if mentions(x)] or compact.get("links", [])[:10])[:15],
            "related_inferred_dependencies": ([x for x in compact.get("inferred_dependency_edges", []) if mentions(x)] or compact.get("inferred_dependency_edges", [])[:6])[:10],
            "related_condition_dependencies": ([x for x in compact.get("condition_dependency_evidence", []) if mentions(x)] or compact.get("condition_dependency_evidence", [])[:6])[:10],
            "related_unresolved_links": ([x for x in compact.get("unresolved_links", []) if mentions(x)] or compact.get("unresolved_links", [])[:6])[:10],
            "global_counts": compact.get("counts", {}),
            "known_limitations": compact.get("known_limitations", []),
            "operation_assignment_note": (
                "Explicit service-operation match." if explicit_match else
                "No explicit service-operation ownership found; no operations assigned to this service."
            ),
        }

    @classmethod
    def _build_service_prompt(cls, compact, manifest_item, evidence_slice):
        evidence_json = json.dumps(evidence_slice, ensure_ascii=False, default=str)
        service_type = manifest_item["service_type"]
        function_name = manifest_item["function_name"]
        title = manifest_item["title"]
        if service_type == "DATA_TASK":
            instructions = """
Generate PySpark only when the supplied evidence identifies the required
source/target and transformation semantics. Do not invent table names, paths,
columns, join keys, filters, aggregation rules, merge keys or write modes.
If essential evidence is absent, return a structured BLOCKED or PARTIAL result
with the exact missing evidence and configuration needed. Do not fabricate a
DataFrame transformation merely to make the function look complete. If a
limited transformation is directly supported, implement only that part and
list its assumptions explicitly. Never claim execution or data validation.
"""
        elif service_type == "COMMAND_TASK":
            instructions = """
Generate a command-task migration function with configurable command
specification and safe Databricks task interface. Never execute shell commands,
subprocess, os.system or external commands. Return inferred intent and
configuration requirements as structured metadata.
"""
        else:
            instructions = """
Generate the best-effort service implementation supported by this evidence.
Use PySpark DataFrame APIs if data processing is indicated; otherwise return
a structured result describing inferred behavior and assumptions.
"""
        return f"""
Generate ONE function for an IDMC-to-Databricks migration.
Taskflow: {compact.get("taskflow", {}).get("name", "IDMC_TASKFLOW")}
Service: {title}
Service ID: {manifest_item["service_id"]}
Service type: {service_type}
Required function name: {function_name}

GOAL: maximize useful generation while preserving evidence integrity.
Only implement transformations supported by the supplied evidence. Do not
invent missing business logic. Return BLOCKED/PARTIAL with precise gaps when
the XML is insufficient.

{instructions}

OUTPUT RULES:
- Return ONLY one complete Python function named {function_name}.
- No imports, module constants, main(), metadata dumps, markdown fences,
  or other service functions.
- Signature must accept spark and params=None.
- Final module provides SparkSession, DataFrame, F, and Window imports globally.
- No eval, exec, compile, __import__, subprocess, os.system or shell execution.
- Unknown source/target/table/path values must be overridable through params.
- Do not claim execution or data validation has occurred.

SERVICE-SCOPED PARSED XML EVIDENCE:
{evidence_json}
"""


    @staticmethod
    def _operation_marker_results(code: str, compact: Dict[str, Any]) -> Dict[str, Any]:
        markers = {}
        for ref, status in re.findall(
            r"IDMC_OPERATION_REF:\s*(.*?)\s*\|\s*STATUS:\s*(IMPLEMENTED|INFERRED|UNSUPPORTED|UNASSIGNED)",
            code,
            flags=re.IGNORECASE,
        ):
            markers[str(ref).strip()] = status.upper()
        expected = [str(x["ref"]) for x in compact["operation_inventory"]]
        present = [ref for ref in expected if ref in markers]
        implemented = sum(markers.get(ref) == "IMPLEMENTED" for ref in expected)
        inferred = sum(markers.get(ref) == "INFERRED" for ref in expected)
        unsupported = sum(markers.get(ref) == "UNSUPPORTED" for ref in expected)
        unassigned = sum(markers.get(ref) == "UNASSIGNED" for ref in expected)
        missing = [ref for ref in expected if ref not in markers]
        total = len(expected)
        represented = implemented + inferred + unsupported
        return {
            "total_operations": total,
            "operations_with_code_marker": len(present),
            "implemented_operations": implemented,
            "inferred_operations": inferred,
            "unsupported_operations": unsupported,
            "unassigned_operations": unassigned,
            "unmarked_operations": len(missing),
            "operation_representation_percent": round((represented / total) * 100, 2) if total else 100.0,
            "strict_implementation_percent": round((implemented / total) * 100, 2) if total else 100.0,
            "missing_operation_refs": missing,
        }

    @classmethod
    def validate_code(cls, code: str, compact: Dict[str, Any], manifest: List[Dict[str, Any]]) -> Dict[str, Any]:
        errors: List[str] = []
        warnings: List[str] = []
        try:
            tree = ast.parse(code)
        except SyntaxError as exc:
            return {
                "status": "FAIL", "valid": False,
                "errors": [f"Python syntax error line {exc.lineno}: {exc.msg}"],
                "warnings": [], "checks": {"python_syntax": "FAIL"},
            }

        function_nodes = [
            n for n in ast.walk(tree)
            if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))
        ]
        function_names = {n.name for n in function_nodes}
        # A fallback function returning GENERATION_FAILED is a failure record,
        # not a successfully generated service implementation.
        failure_stub_functions = {
            fn.name for fn in function_nodes
            if any(
                isinstance(node, ast.Constant)
                and node.value == "GENERATION_FAILED"
                for node in ast.walk(fn)
            )
        }
        missing_functions = [m["function_name"] for m in manifest
                             if m["function_name"] not in function_names]
        imports_pyspark = any(
            isinstance(n, ast.ImportFrom) and (n.module or "").startswith("pyspark")
            for n in ast.walk(tree)
        ) or any(
            isinstance(n, ast.Import) and any(a.name.startswith("pyspark") for a in n.names)
            for n in ast.walk(tree)
        )
        dataframe_methods = {"select", "withColumn", "filter", "where", "join", "groupBy",
                             "agg", "dropDuplicates", "unionByName", "orderBy"}
        found_methods = {
            n.func.attr for n in ast.walk(tree)
            if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)
            and n.func.attr in dataframe_methods
        }
        unsafe = []
        forbidden_names = {"eval", "exec", "compile", "__import__"}
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id in forbidden_names:
                unsafe.append(node.func.id)
            if isinstance(node, ast.Import) and any(a.name == "subprocess" for a in node.names):
                unsafe.append("subprocess import")
            if isinstance(node, ast.ImportFrom) and node.module == "subprocess":
                unsafe.append("subprocess import")
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
                if node.func.attr in {"system", "popen", "Popen"}:
                    unsafe.append(node.func.attr)
                if (node.func.attr == "run" and isinstance(node.func.value, ast.Name)
                        and node.func.value.id == "subprocess"):
                    unsafe.append("subprocess.run")

        operation_results = cls._operation_marker_results(code, compact)
        total_services = len(manifest)
        generated_services = sum(
            m["function_name"] in function_names
            and m["function_name"] not in failure_stub_functions
            for m in manifest
        )
        data_tasks = [m for m in manifest if m["service_type"] == "DATA_TASK"]
        generated_data_tasks = sum(
            m["function_name"] in function_names
            and m["function_name"] not in failure_stub_functions
            for m in data_tasks
        )
        command_tasks = [m for m in manifest if m["service_type"] == "COMMAND_TASK"]
        generated_command_tasks = sum(
            m["function_name"] in function_names
            and m["function_name"] not in failure_stub_functions
            for m in command_tasks
        )

        if not imports_pyspark:
            errors.append("Generated source does not import PySpark")
        if missing_functions:
            warnings.append("Missing service functions: " + ", ".join(missing_functions))
        if unsafe:
            errors.append("Unsafe code constructs detected: " + ", ".join(sorted(set(unsafe))))
        if not found_methods:
            warnings.append("No common PySpark DataFrame transformation methods detected")
        if operation_results["unmarked_operations"]:
            warnings.append(
                f"{operation_results['unmarked_operations']} parsed operations have no code-generation marker"
            )
        if operation_results.get("unassigned_operations"):
            warnings.append(
                f"{operation_results['unassigned_operations']} parsed operations have no verified service ownership and are not counted as code coverage"
            )
        if operation_results["unsupported_operations"]:
            warnings.append(
                f"{operation_results['unsupported_operations']} operations are explicitly unsupported"
            )

        syntax_ok = True
        valid = not errors
        status = "FAIL" if not valid else ("PARTIAL" if warnings else "PASS")
        return {
            "status": status,
            "valid": valid,
            "errors": errors,
            "warnings": warnings,
            "checks": {
                "python_syntax": "PASS" if syntax_ok else "FAIL",
                "pyspark_import": "PASS" if imports_pyspark else "FAIL",
                "service_function_coverage": "PASS" if not missing_functions else "PARTIAL",
                "dataframe_transformations": "PASS" if found_methods else "REVIEW_REQUIRED",
                "unsafe_execution": "FAIL" if unsafe else "PASS",
                "operation_traceability": (
                    "PASS" if not operation_results["unmarked_operations"]
                    and not operation_results.get("unassigned_operations")
                    else "PARTIAL"
                ),
                "task_execution": "NOT_RUN",
                "data_validation": "NOT_RUN",
            },
            "service_count": total_services,
            "generated_service_functions": generated_services,
            "service_function_coverage_percent": round(generated_services / total_services * 100, 2) if total_services else 100.0,
            "data_task_count": len(data_tasks),
            "generated_data_task_functions": generated_data_tasks,
            "data_task_code_generation_percent": round(generated_data_tasks / len(data_tasks) * 100, 2) if data_tasks else 100.0,
            "command_task_count": len(command_tasks),
            "generated_command_task_functions": generated_command_tasks,
            "pyspark_dataframe_methods_found": sorted(found_methods),
            "operation_coverage": operation_results,
            "execution_performed": False,
            "data_validation_performed": False,
        }

    @classmethod
    def _append_metadata_and_report(cls, generated_code: str, compact: Dict[str, Any],
                                    manifest: List[Dict[str, Any]]) -> Tuple[str, Dict[str, Any]]:
        validation = cls.validate_code(generated_code, compact, manifest)
        # JSON is used as a safely escaped string literal; generated code does not eval it.
        metadata_json = json.dumps(compact, ensure_ascii=False, default=str)
        report_json = json.dumps(validation, ensure_ascii=False, default=str)
        suffix = f'''\n\n# ----------------------------------------------------------------------
# AUTHORITATIVE PARSED TASKFLOW METADATA AND GENERATION REPORT
# Appended by TaskflowCodeGenerator; do not edit generated counts manually.
# ----------------------------------------------------------------------
import json as _json
TASKFLOW_METADATA = _json.loads({metadata_json!r})
GENERATION_VALIDATION = _json.loads({report_json!r})

PARAMS_METADATA = {{
    "input_parameters": TASKFLOW_METADATA.get("parameters", []),
    "temporary_fields": TASKFLOW_METADATA.get("temp_fields", []),
}}

ASSUMPTIONS = list(globals().get("ASSUMPTIONS", []))
UNSUPPORTED_ITEMS = list(globals().get("UNSUPPORTED_ITEMS", []))


def migration_report():
    counts = TASKFLOW_METADATA.get("counts", {{}})
    validation = GENERATION_VALIDATION
    operation = validation.get("operation_coverage", {{}})
    return {{
        "taskflow": TASKFLOW_METADATA.get("taskflow", {{}}),
        "parsed_counts": counts,
        "generated_counts": {{
            "service_functions": validation.get("generated_service_functions", 0),
            "data_task_functions": validation.get("generated_data_task_functions", 0),
            "command_task_functions": validation.get("generated_command_task_functions", 0),
            "operations_with_code_markers": operation.get("operations_with_code_marker", 0),
            "implemented_operations": operation.get("implemented_operations", 0),
            "inferred_operations": operation.get("inferred_operations", 0),
            "unsupported_operations": operation.get("unsupported_operations", 0),
        }},
        "coverage_percentages": {{
            "service_function_coverage": validation.get("service_function_coverage_percent", 0),
            "data_task_code_generation": validation.get("data_task_code_generation_percent", 0),
            "operation_representation": operation.get("operation_representation_percent", 0),
            "strict_operation_implementation": operation.get("strict_implementation_percent", 0),
        }},
        "assumptions": ASSUMPTIONS,
        "unsupported_items": UNSUPPORTED_ITEMS,
        "validation": validation,
        "execution_performed": False,
        "data_validation_performed": False,
    }}
'''
        final_code = generated_code.rstrip() + suffix
        # Validate the final combined file too.
        ast.parse(final_code)
        return final_code, validation



    def generate_taskflow_code(
        self,
        taskflow_input: Dict[str, Any],
        output_dir: str | Path = DEFAULT_OUTPUT_DIR,
        max_retries: int = 1,
    ) -> Dict[str, Any]:
        """Generate service-scoped code and save it only in the UC Volume."""
        expected_volume = DEFAULT_OUTPUT_DIR.rstrip("/")
        requested_output = str(output_dir).rstrip("/")

        if requested_output != expected_volume:
            raise ValueError(
                f"output_dir must be exactly {expected_volume!r}; "
                f"received {requested_output!r}. Workspace paths are rejected."
            )

        compact = self.compact_input(taskflow_input)
        manifest = self._service_manifest(compact)
        if not manifest:
            raise ValueError(
                "No Taskflow services were found. Check TaskflowLLMInputBuilder output."
            )

        data_manifest = [m for m in manifest if m["service_type"] == "DATA_TASK"]
        total_data_tasks = len(data_manifest)
        generated_functions = []
        generation_errors = []
        per_service_results = []
        data_task_index = 0

        for item in manifest:
            current_data_index = data_task_index
            if item["service_type"] == "DATA_TASK":
                data_task_index += 1

            evidence_slice = self._service_evidence(
                compact, item, current_data_index, total_data_tasks
            )
            prompt = self._build_service_prompt(compact, item, evidence_slice)
            code_piece = ""
            last_error = None

            for attempt in range(max(0, int(max_retries)) + 1):
                attempt_prompt = prompt
                if attempt and last_error:
                    attempt_prompt += (
                        "\nRETRY: Return one complete syntactically valid function named "
                        + item["function_name"] + ". Previous error: " + last_error
                    )
                try:
                    code_piece = self._extract_code(self._call_llm(attempt_prompt))
                    parsed = ast.parse(code_piece)
                    found = {
                        n.name for n in ast.walk(parsed)
                        if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))
                    }
                    if item["function_name"] not in found:
                        raise ValueError(
                            "LLM response missing function " + item["function_name"]
                        )
                    function_node = next(
                        n for n in ast.walk(parsed)
                        if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))
                        and n.name == item["function_name"]
                    )
                    meaningful = [
                        n for n in function_node.body
                        if not (
                            isinstance(n, ast.Expr)
                            and isinstance(n.value, ast.Constant)
                            and isinstance(n.value.value, str)
                        )
                    ]
                    if not meaningful:
                        raise ValueError("Generated service function has an empty body")
                    break
                except Exception as exc:
                    last_error = str(exc)
                    code_piece = ""
                    if attempt >= max_retries:
                        generation_errors.append({
                            "service_id": item["service_id"],
                            "service": item["title"],
                            "service_type": item["service_type"],
                            "error": last_error,
                        })

            if code_piece:
                generated_functions.append(
                    "\n# " + "=" * 76 + "\n"
                    + "# IDMC SERVICE: " + item["title"]
                    + " | ID: " + item["service_id"] + "\n"
                    + "# TYPE: " + item["service_type"] + "\n"
                    + code_piece.strip() + "\n"
                )
                per_service_results.append({
                    "service_id": item["service_id"],
                    "service": item["title"],
                    "service_type": item["service_type"],
                    "function_name": item["function_name"],
                    "status": "GENERATED",
                    "evidence_operation_count": len(
                        evidence_slice["operations_for_this_task"]
                    ),
                })
            else:
                generated_functions.append(
                    "\ndef " + item["function_name"] + "(spark, params=None):\n"
                    "    return {\n"
                    "        'status': 'GENERATION_FAILED',\n"
                    "        'service_id': " + repr(item["service_id"]) + ",\n"
                    "        'service': " + repr(item["title"]) + ",\n"
                    "        'error': " + repr(last_error) + ",\n"
                    "    }\n"
                )
                per_service_results.append({
                    "service_id": item["service_id"],
                    "service": item["title"],
                    "service_type": item["service_type"],
                    "function_name": item["function_name"],
                    "status": "GENERATION_FAILED",
                    "error": last_error,
                })

        # Keep every parsed operation traceable, but do not claim that it was
        # implemented or inferred into code unless it is actually associated
        # with a generated service implementation.
        operation_markers = ["\n# IDMC operation traceability inventory\n"]
        for op in compact["operation_inventory"]:
            operation_markers.append(
                "# IDMC_OPERATION_REF: " + op["ref"] + " | STATUS:UNASSIGNED\n"
            )

        module_header = r'''# -*- coding: utf-8 -*-
# Best-effort IDMC Taskflow to Databricks PySpark migration artifact.
# Generated from Taskflow XML only. Mapping-level transformations may be absent.
# Inferred code is approximate and is not proven equivalent to original IDMC logic.
# Review assumptions and unsupported items before execution. Importing this module
# does not execute any task.
from typing import Any, Dict
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

ASSUMPTIONS = [
    "Service code is generated independently using service-scoped Taskflow evidence.",
    "Where service-operation ownership is absent, operation evidence may be balanced across data-task prompts.",
    "Inferred source/target names, columns, joins, filters, expressions and write modes require review.",
    "Taskflow XML does not contain the complete mapping definition export.",
]
UNSUPPORTED_ITEMS = []
'''

        generated_code = (
            module_header
            + "\n".join(generated_functions)
            + "\n".join(operation_markers)
        )

        validation = self.validate_code(generated_code, compact, manifest)
        validation["per_service_generation"] = per_service_results
        validation["service_generation_failures"] = len(generation_errors)
        validation["generation_strategy"] = "SERVICE_SCOPED_LLM_CHUNKING"
        validation["llm_requests"] = len(manifest)

        final_code, final_validation = self._append_metadata_and_report(
            generated_code, compact, manifest
        )
        final_validation["per_service_generation"] = per_service_results
        final_validation["service_generation_failures"] = len(generation_errors)
        final_validation["generation_strategy"] = "SERVICE_SCOPED_LLM_CHUNKING"
        final_validation["llm_requests"] = len(manifest)

        # Append the complete generation metrics to the generated module.
        validation_literal = json.dumps(
            final_validation, ensure_ascii=False, default=str
        )
        marker = "GENERATION_VALIDATION = _json.loads("
        marker_start = final_code.find(marker)
        if marker_start >= 0:
            line_end = final_code.find("\n", marker_start)
            final_code = (
                final_code[:marker_start]
                + marker
                + repr(validation_literal)
                + ")"
                + (final_code[line_end:] if line_end >= 0 else "")
            )

        ast.parse(final_code)

        output = Path(expected_volume)
        output.mkdir(parents=True, exist_ok=True)

        taskflow_name = compact.get("taskflow", {}).get("name") or "idmc_taskflow"
        file_name = "generated_" + self._safe_identifier(taskflow_name) + ".py"
        code_path = output / file_name
        report_path = output / "taskflow_generation_report.json"
        metadata_path = output / "taskflow_generation_metadata.json"

        report = {
            "taskflow_name": taskflow_name,
            "generated_file": str(code_path),
            "output_directory": str(output),
            "model": self.model,
            "generation_method": "SERVICE_SCOPED_LLM_CHUNKING",
            "parsed_counts": compact["counts"],
            "generation_metrics": final_validation,
            "per_service_generation": per_service_results,
            "generation_errors": generation_errors,
            "assumptions": [
                "Operations are matched to a service by explicit references when available.",
                "Unmatched operations may be partitioned across data-task prompts as an inference.",
                "Generated PySpark is best-effort and is not verified equivalent to original IDMC mapping behavior.",
                "Task execution and data validation have not been performed by this generator.",
            ],
            "unsupported_items": [
                {
                    "service_id": item.get("service_id"),
                    "service": item.get("service"),
                    "reason": item.get("error", "No evidence-backed implementation was generated.")
                }
                for item in per_service_results
                if item.get("status") == "GENERATION_FAILED"
            ] + [
                "Operations without explicit service ownership remain UNASSIGNED; "
                "they are not counted as implemented or inferred."
            ],
            "execution_performed": False,
            "data_validation_performed": False,
        }

        # Write directly to /Volumes; never to /Workspace or local fallback.
        code_path.write_text(final_code, encoding="utf-8")
        report_path.write_text(
            json.dumps(report, indent=2, ensure_ascii=False, default=str),
            encoding="utf-8",
        )
        metadata_path.write_text(
            json.dumps(compact, indent=2, ensure_ascii=False, default=str),
            encoding="utf-8",
        )

        missing = [
            str(path) for path in (code_path, report_path, metadata_path)
            if not path.is_file()
        ]
        if missing:
            raise IOError(
                "Could not verify generated Volume files: " + ", ".join(missing)
            )

        report["report_file"] = str(report_path)
        report["metadata_file"] = str(metadata_path)
        report["generated_file_size_bytes"] = code_path.stat().st_size
        report["output_verified"] = True
        return report



if __name__ == "__main__":
    raise SystemExit("Import TaskflowCodeGenerator in Databricks and call generate_taskflow_code().")
