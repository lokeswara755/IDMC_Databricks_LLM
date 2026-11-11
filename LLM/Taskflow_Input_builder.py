
"""
Taskflow_Input_builder.py

Builds complete, evidence-based LLM input for IDMC Taskflow
to Databricks migration.

- Uses actual top-level parser parameter definitions.
- Preserves all temporary fields, services, operations and links.
- Preserves inferred and unresolved dependencies separately.
- Validates metadata counts against parser and graph summaries.
- Excludes raw XML to avoid unnecessarily large LLM prompts.
- Does not invent mapping transformations or business logic.
"""

from __future__ import annotations

import json
from typing import Any, Dict, List


def _as_dict(value: Any) -> Dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _as_list(value: Any) -> List[Any]:
    return value if isinstance(value, list) else []


def _clean_raw_xml(value: Any) -> Any:
    """
    Recursively remove raw XML representations while preserving
    all useful parsed metadata.
    """
    if isinstance(value, dict):
        return {
            key: _clean_raw_xml(item)
            for key, item in value.items()
            if key not in {
                "raw_xml",
                "xml",
                "xml_text",
                "source_xml",
            }
        }

    if isinstance(value, list):
        return [_clean_raw_xml(item) for item in value]

    return value


class TaskflowLLMInputBuilder:
    """
    Creates a complete and traceable LLM input payload
    from IDMC Taskflow parser and graph-builder outputs.
    """

    def build(
        self,
        parsed_taskflow: Dict[str, Any],
        taskflow_graph: Dict[str, Any],
    ) -> Dict[str, Any]:

        if not isinstance(parsed_taskflow, dict):
            raise TypeError(
                "parsed_taskflow must be a dictionary."
            )

        if not isinstance(taskflow_graph, dict):
            raise TypeError(
                "taskflow_graph must be a dictionary."
            )

        # --------------------------------------------------
        # 1. Extract actual parser output
        # --------------------------------------------------

        taskflow = _as_dict(
            parsed_taskflow.get("taskflow", parsed_taskflow)
        )

        parser_summary = _as_dict(
            parsed_taskflow.get("summary")
        )

        graph_summary = _as_dict(
            taskflow_graph.get("summary")
        )

        if not taskflow:
            raise ValueError(
                "Taskflow metadata is missing from parser output."
            )

        # --------------------------------------------------
        # 2. Extract parameters and temporary fields
        # IMPORTANT:
        # Do not recursively search operation-level inputs.
        # --------------------------------------------------

        parameters = _as_list(
            taskflow.get("input_parameters")
        )

        temp_fields = _as_list(
            taskflow.get("temp_fields")
        )

        # --------------------------------------------------
        # 3. Extract services from graph nodes
        # --------------------------------------------------

        nodes = _as_list(
            taskflow_graph.get("nodes")
        )

        services = []

        for node in nodes:

            if not isinstance(node, dict):
                continue

            if node.get("element_type") != "service":
                continue

            services.append({
                "id": node.get("id"),
                "name": node.get("name"),
                "title": node.get("title"),
                "service_name": node.get("service_name"),
                "service_type": node.get("service_type") or node.get("type"),
                "parent_id": node.get("parent_id"),
                "flow_id": node.get("flow_id"),
            })

        # Fallback to parser services if graph nodes
        # do not contain service records.
        if not services:
            services = _as_list(
                taskflow.get("services")
            )

        # --------------------------------------------------
        # 4. Extract graph evidence
        # --------------------------------------------------

        containment_edges = _as_list(
            taskflow_graph.get("containment_edges")
        )

        dependency_edges = _as_list(
            taskflow_graph.get("dependency_edges")
        )

        condition_dependency_edges = _as_list(
            taskflow_graph.get("condition_dependency_edges")
        )

        unresolved_links = _as_list(
            taskflow_graph.get("unresolved_links")
        )

        # Prefer complete link inventory.
        links = _as_list(
            taskflow_graph.get("link_inventory")
        )

        # Fallback for graph-builder versions that do not
        # expose link_inventory.
        if not links:

            links = (
                dependency_edges
                + unresolved_links
            )

        # --------------------------------------------------
        # 5. Validate metadata counts
        # --------------------------------------------------

        expected_parameters = parser_summary.get(
            "parameter_count"
        )

        expected_temp_fields = parser_summary.get(
            "temp_field_count"
        )

        expected_services = parser_summary.get(
            "service_count"
        )

        expected_links = parser_summary.get(
            "link_count"
        )

        expected_operations = parser_summary.get("operation_count")
        expected_control_nodes = parser_summary.get("control_flow_node_count")

        count_checks = {
            "parameters": {
                "expected": expected_parameters,
                "actual": len(parameters),
            },
            "temp_fields": {
                "expected": expected_temp_fields,
                "actual": len(temp_fields),
            },
            "services": {
                "expected": expected_services,
                "actual": len(services),
            },
            "links": {
                "expected": expected_links,
                "actual": len(links),
            },
            "operations": {
                "expected": expected_operations,
                "actual": len(_as_list(taskflow.get("operations"))),
            },
            "control_flow_nodes": {
                "expected": expected_control_nodes,
                "actual": len(_as_list(taskflow.get("control_flow_nodes"))),
            },
        }

        count_mismatches = []

        for name, counts in count_checks.items():

            expected = counts["expected"]
            actual = counts["actual"]

            if expected is not None and expected != actual:
                count_mismatches.append(
                    f"{name}: expected {expected}, found {actual}"
                )

        if count_mismatches:
            raise ValueError(
                "Taskflow metadata count validation failed: "
                + "; ".join(count_mismatches)
            )

        # --------------------------------------------------
        # 6. Preserve all parsed Taskflow metadata
        # --------------------------------------------------

        taskflow_metadata = _clean_raw_xml(taskflow)

        # --------------------------------------------------
        # 7. Build final LLM payload
        # --------------------------------------------------

        payload = {
            "asset_type": "IDMC_TASKFLOW",

            "taskflow": {
                "name": (
                    taskflow.get("name")
                    or parsed_taskflow.get(
                        "asset_metadata", {}
                    ).get("name")
                ),
                "guid": (
                    taskflow.get("guid")
                    or parsed_taskflow.get(
                        "asset_metadata", {}
                    ).get("guid")
                ),
                "mime_type": (
                    parsed_taskflow.get(
                        "asset_metadata", {}
                    ).get("mimetype")
                    or parser_summary.get("mime_type")
                ),
                "description": taskflow.get("description"),
            },

            "parser_evidence": {

                "parameters": _clean_raw_xml(parameters),

                "temp_fields": _clean_raw_xml(temp_fields),

                "services": _clean_raw_xml(services),

                "flow_summaries": _clean_raw_xml(
                    _as_list(taskflow_graph.get("flows"))
                ),

                "node_type_counts": _as_dict(
                    graph_summary.get("node_type_counts")
                ),

                "taskflow_metadata": taskflow_metadata,

                "graph_nodes": _clean_raw_xml(nodes),

                "operations": _clean_raw_xml(
                    _as_list(taskflow.get("operations"))
                ),

                "control_flow_nodes": _clean_raw_xml(
                    _as_list(
                        taskflow.get("control_flow_nodes")
                    )
                ),
            },

            "orchestration_graph": {

                "containment_edges": _clean_raw_xml(
                    containment_edges
                ),

                "inferred_dependency_edges": _clean_raw_xml(
                    dependency_edges
                ),

                "condition_dependency_evidence": _clean_raw_xml(
                    condition_dependency_edges
                ),

                "links": _clean_raw_xml(links),

                "unresolved_links": _clean_raw_xml(
                    unresolved_links
                ),

                "graph_summary": graph_summary,
            },

            "validation": {

                "graph_status": taskflow_graph.get(
                    "validation_status",
                    "REVIEW_REQUIRED",
                ),

                "graph_summary": graph_summary,

                "source_ids_present_in_xml": False,

                "dependency_edges_are_inferred": True,

                "taskflow_execution_performed": False,

                "metadata_count_checks": count_checks,

                "metadata_count_validation": (
                    "PASS" if not count_mismatches
                    else "REVIEW_REQUIRED"
                ),
            },

            "input_integrity": {

                "parameter_count": len(parameters),

                "temp_field_count": len(temp_fields),

                "service_count": len(services),

                "link_count": len(links),

                "graph_node_count": len(nodes),

                "taskflow_operation_count": len(
                    _as_list(taskflow.get("operations"))
                ),

                "containment_edge_count": len(
                    containment_edges
                ),

                "dependency_edge_count": len(
                    dependency_edges
                ),

                "condition_dependency_edge_count": len(
                    condition_dependency_edges
                ),

                "unresolved_link_count": len(
                    unresolved_links
                ),

                "all_metadata_counts_validated": (
                    not count_mismatches
                ),

                "all_links_preserved": (
                    len(links) == (
                        expected_links
                        if expected_links is not None
                        else len(links)
                    )
                ),
            },

            "known_limitations": [

                "The supplied XML is an IDMC Taskflow export, not a mapping-definition export.",

                "Link elements contain id and targetId but no sourceId.",

                "Dependency sources inferred by graph analysis must not be treated as XML-confirmed relationships.",

                "Mapping task references do not include complete mapping transformation definitions in this XML.",

                "Source-to-target schemas, SQL, merge keys and business rules must not be invented.",

                "Command task implementations may require external scripts, executables or connection details not included in this export.",

                "Taskflow execution and data validation have not been performed.",

                "Generated migration logic must identify unsupported operations and assumptions explicitly.",
            ],
        }

        return payload

    # ======================================================
    # LLM PROMPT
    # ======================================================

    @staticmethod
    def to_prompt(payload: Dict[str, Any]) -> str:

        if not isinstance(payload, dict):
            raise TypeError(
                "payload must be a dictionary."
            )

        evidence = json.dumps(
            payload,
            indent=2,
            ensure_ascii=False,
            default=str,
        )

        return f"""
You are a senior Informatica IDMC-to-Databricks migration engineer.

Your task is to analyze the supplied IDMC TASKFLOW metadata and
generate a Databricks migration implementation using PySpark
where the available evidence supports it.

IMPORTANT:
This is a TASKFLOW export, not a mapping-definition export.

OBJECTIVES:

1. Analyze all 15 services and their service types.

2. Analyze all input parameters and temporary fields.

3. Analyze all 99 Taskflow links.

4. Analyze all containment, inferred dependency and
   condition dependency relationships.

5. Analyze assignments, conditions, containers, catch
   handlers, operations and control-flow nodes.

6. Identify which Taskflow operations can be translated
   into Databricks Jobs, Workflows, notebooks or Python tasks.

7. Generate useful PySpark code for operations where
   sufficient source evidence exists.

8. Generate orchestration code for supported Taskflow
   control-flow operations.

9. Identify command tasks that require external scripts,
   shell commands or unavailable connection information.

10. Generate task-level migration results and clearly
    distinguish SUCCESS, PARTIAL, FAIL and BLOCKED.

EVIDENCE RULES:

- Use all supplied parser metadata.
- Preserve all 37 input parameters.
- Preserve all 42 temporary fields.
- Preserve all 15 services.
- Preserve all 99 links.
- Do not silently remove operations or dependencies.
- Do not treat inferred dependency edges as confirmed XML facts.
- Do not invent missing source IDs.
- Do not invent source or target table schemas.
- Do not invent mapping transformations, SQL or merge keys.
- Do not generate fake Spark operations merely to satisfy validation.
- Do not use NotImplementedError as a substitute for reporting
  a task's actual migration status.
- When information is missing, document the gap and its impact.
- Generate the maximum useful implementation supported by
  the supplied XML.

OUTPUT EXPECTATIONS:

Generate a complete Python migration module.

Include:
1. Taskflow metadata.
2. Parameter configuration.
3. Temporary-field configuration.
4. Service definitions.
5. Dependency and condition analysis.
6. Task execution handlers where implementable.
7. Error handling and execution logging.
8. Task-level migration status.
9. Migration summary.
10. Explicit assumptions and limitations.

Do not claim that migration execution or data validation
has succeeded unless it has actually been performed.

TASKFLOW EVIDENCE:

{evidence}
"""

