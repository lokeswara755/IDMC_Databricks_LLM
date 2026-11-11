"""Build a hierarchical, read-only graph from parsed IDMC Taskflow metadata.

The supplied Taskflow XML links contain id and targetId, but no sourceId.
This module therefore separates:
  * confirmed containment edges,
  * inferred predecessor/dependency edges,
  * condition-reference evidence,
  * unresolved links.

No Taskflow operation is executed.
"""
from __future__ import annotations

from collections import Counter
from typing import Any
import re


STRUCTURAL_TAGS = {
    "taskflow", "flow", "start", "end", "eventContainer", "service",
    "container", "assignment", "catch", "link", "invoke", "switch",
    "while", "forEach",
}
ACTIVITY_TAGS = {
    "start", "end", "eventContainer", "service", "container",
    "assignment", "invoke", "switch", "while", "forEach",
}
CONDITION_REF_RE = re.compile(r"\$temp\.([A-Za-z_][A-Za-z0-9_]*)")


def _local_name(tag: Any) -> str:
    if not isinstance(tag, str):
        return str(tag)
    return tag.rsplit("}", 1)[-1] if "}" in tag else tag.split(":")[-1]


def _text(node: dict[str, Any]) -> str:
    parts = []
    if node.get("text"):
        parts.append(str(node["text"]))
    for child in node.get("children") or []:
        value = _text(child)
        if value:
            parts.append(value)
    return " ".join(parts).strip()


class TaskflowGraphBuilder:
    """Build a graph from TaskflowParser output without executing it."""

    def build(self, parsed_taskflow: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(parsed_taskflow, dict):
            raise TypeError("parsed_taskflow must be a dictionary.")

        taskflow = parsed_taskflow.get("taskflow", parsed_taskflow)
        raw_root = taskflow.get("raw_xml")
        if not isinstance(raw_root, dict):
            raise ValueError(
                "taskflow.raw_xml is missing. Pass the complete TaskflowParser output."
            )

        nodes: dict[str, dict[str, Any]] = {}
        containment_edges: list[dict[str, Any]] = []
        dependency_edges: list[dict[str, Any]] = []
        condition_dependency_edges: list[dict[str, Any]] = []
        unresolved_links: list[dict[str, Any]] = []
        duplicate_ids: list[str] = []
        flows: list[dict[str, Any]] = []
        link_inventory: list[dict[str, Any]] = []

        def node_id_of(node: dict[str, Any]) -> str | None:
            attrs = node.get("attributes") or {}
            value = attrs.get("id") or attrs.get("GUID")
            return str(value) if value is not None else None

        # Index every structural element and preserve its XML hierarchy.
        def visit(node: dict[str, Any], parent_id: str | None = None,
                  current_flow_id: str | None = None, sibling_index: int | None = None):
            tag = _local_name(node.get("tag", ""))
            attrs = node.get("attributes") or {}
            this_id = node_id_of(node)

            if tag == "flow" and this_id:
                current_flow_id = this_id

            effective_parent = parent_id
            if tag in STRUCTURAL_TAGS and this_id:
                if this_id in nodes:
                    duplicate_ids.append(this_id)
                else:
                    title = None
                    service_name = None
                    for child in node.get("children") or []:
                        ctag = _local_name(child.get("tag", ""))
                        if ctag == "title":
                            title = _text(child)
                        elif ctag == "serviceName":
                            service_name = _text(child)
                    nodes[this_id] = {
                        "id": this_id,
                        "element_type": tag,
                        "title": title,
                        "name": attrs.get("name"),
                        "type": attrs.get("type"),
                        "service_name": service_name,
                        "parent_id": parent_id,
                        "flow_id": current_flow_id,
                        "sibling_index": sibling_index,
                        "attributes": attrs,
                    }
                    if parent_id:
                        containment_edges.append({
                            "source_id": parent_id, "target_id": this_id,
                            "edge_type": "CONTAINS", "confirmed": True,
                        })
                effective_parent = this_id

            if tag == "flow":
                children = node.get("children") or []
                flows.append({
                    "flow_id": this_id,
                    "parent_id": parent_id,
                    "child_count": len(children),
                    "activity_count": sum(
                        _local_name(c.get("tag", "")) in ACTIVITY_TAGS for c in children
                    ),
                    "link_count": sum(
                        _local_name(c.get("tag", "")) == "link" for c in children
                    ),
                })

            for index, child in enumerate(node.get("children") or []):
                visit(child, effective_parent, current_flow_id, index)

        visit(raw_root)

        # Walk the raw XML again to retain each link's immediate owner and sibling context.
        def walk_links(node: dict[str, Any], parent: dict[str, Any] | None = None,
                       sibling_index: int | None = None,
                       nearest_flow_id: str | None = None,
                       container_stack: tuple[str, ...] = ()):
            tag = _local_name(node.get("tag", ""))
            attrs = node.get("attributes") or {}
            this_id = node_id_of(node)

            if tag == "flow" and this_id:
                nearest_flow_id = this_id
            if tag == "container" and this_id:
                container_stack = container_stack + (this_id,)

            if tag == "link":
                link_id = attrs.get("id")
                target_id = attrs.get("targetId")
                target_id = str(target_id) if target_id is not None else None
                parent_tag = _local_name(parent.get("tag", "")) if parent else None
                parent_id = node_id_of(parent) if parent else None
                siblings = (parent or {}).get("children") or []

                # Only infer a predecessor when the link is directly in a flow
                # and a preceding sibling is an activity. Never label it XML-confirmed.
                source_candidate = None
                resolution = "UNRESOLVED"
                if parent_tag == "flow" and sibling_index is not None:
                    for previous in reversed(siblings[:sibling_index]):
                        ptag = _local_name(previous.get("tag", ""))
                        pid = node_id_of(previous)
                        if ptag in ACTIVITY_TAGS and pid and pid in nodes:
                            source_candidate = pid
                            resolution = "INFERRED_PREVIOUS_ACTIVITY_IN_SAME_FLOW"
                            break

                condition_nodes = [
                    c for c in node.get("children") or []
                    if _local_name(c.get("tag", "")) == "condition"
                ]
                condition_text = " ".join(_text(c) for c in condition_nodes).strip()
                condition_task_names = sorted(set(CONDITION_REF_RE.findall(condition_text)))
                target_exists = bool(target_id and target_id in nodes)
                source_exists = bool(source_candidate and source_candidate in nodes)

                record = {
                    "link_id": str(link_id) if link_id is not None else None,
                    "flow_id": nearest_flow_id,
                    "owner_id": parent_id,
                    "owner_type": parent_tag,
                    "container_context": list(container_stack),
                    "source_id": source_candidate,
                    "target_id": target_id,
                    "edge_type": attrs.get("type") or "FLOW_LINK",
                    "condition_text": condition_text or None,
                    "condition_task_references": condition_task_names,
                    "source_resolution": resolution,
                    "source_confirmed_by_xml": False,
                    "target_confirmed_by_xml": target_exists,
                }
                link_inventory.append(record)

                if source_exists and target_exists:
                    dependency_edges.append({
                        **record,
                        "edge_type": "INFERRED_DEPENDENCY",
                    })
                else:
                    reason = (
                        "TARGET_NOT_FOUND" if not target_exists
                        else "SOURCE_NOT_INFERABLE"
                    )
                    unresolved_links.append({**record, "reason": reason})

                # Condition references are evidence only. Resolve names to likely
                # service nodes by service_name/name/title; never treat as link source.
                for task_name in condition_task_names:
                    matches = [
                        n for n in nodes.values()
                        if task_name.lower() in {
                            str(n.get("service_name") or "").lower(),
                            str(n.get("name") or "").lower(),
                            str(n.get("title") or "").lower(),
                        }
                    ]
                    for match in matches:
                        condition_dependency_edges.append({
                            "source_id": match["id"],
                            "target_link_id": str(link_id) if link_id is not None else None,
                            "edge_type": "CONDITION_REFERENCE_ONLY",
                            "evidence": task_name,
                            "confirmed_as_link_source": False,
                        })

            for index, child in enumerate(node.get("children") or []):
                walk_links(child, node, index, nearest_flow_id, container_stack)

        walk_links(raw_root)

        type_counts = Counter(n["element_type"] for n in nodes.values())
        service_counts = Counter(
            n["service_name"] for n in nodes.values() if n.get("service_name")
        )

        summary = {
            "node_count": len(nodes),
            "containment_edge_count": len(containment_edges),
            "dependency_edge_count": len(dependency_edges),
            "condition_dependency_edge_count": len(condition_dependency_edges),
            "link_count": len(link_inventory),
            "unresolved_link_count": len(unresolved_links),
            "duplicate_node_id_count": len(set(duplicate_ids)),
            "flow_count": len(flows),
            "node_type_counts": dict(sorted(type_counts.items())),
            "service_type_counts": dict(sorted(service_counts.items())),
            "dependency_sources_inferred": True,
            "condition_references_are_source_proof": False,
            "execution_performed": False,
        }

        return {
            "format": "IDMC_TASKFLOW_GRAPH",
            "taskflow_name": taskflow.get("name"),
            "taskflow_guid": taskflow.get("guid"),
            "nodes": list(nodes.values()),
            "containment_edges": containment_edges,
            "dependency_edges": dependency_edges,
            "condition_dependency_edges": condition_dependency_edges,
            "link_inventory": link_inventory,
            "unresolved_links": unresolved_links,
            "duplicate_node_ids": sorted(set(duplicate_ids)),
            "flows": flows,
            "summary": summary,
        }


class TaskflowGraphValidator:
    """Validate graph references; inferred edges remain review-only."""

    def validate(self, graph: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(graph, dict):
            raise TypeError("graph must be a dictionary.")

        nodes = graph.get("nodes") or []
        node_ids = [n.get("id") for n in nodes]
        node_id_set = set(node_ids)
        duplicates = sorted({
            value for value in node_ids
            if value and node_ids.count(value) > 1
        })

        missing_containment = [
            e for e in graph.get("containment_edges", [])
            if e.get("source_id") not in node_id_set
            or e.get("target_id") not in node_id_set
        ]
        missing_dependencies = [
            e for e in graph.get("dependency_edges", [])
            if e.get("source_id") not in node_id_set
            or e.get("target_id") not in node_id_set
        ]
        unresolved = graph.get("unresolved_links") or []
        link_inventory = graph.get("link_inventory") or []

        checks = {
            "node_ids_present": all(node_ids),
            "node_ids_unique": not duplicates,
            "containment_references_valid": not missing_containment,
            "dependency_references_valid": not missing_dependencies,
            "all_links_have_targets": all(
                item.get("target_confirmed_by_xml") for item in link_inventory
            ),
            "all_link_sources_confirmed": False,
        }
        # The XML schema supplied has no sourceId on links. Therefore source
        # confirmation cannot pass, even if predecessor inference is possible.
        status = "PASS" if (
            checks["node_ids_present"]
            and checks["node_ids_unique"]
            and checks["containment_references_valid"]
            and checks["dependency_references_valid"]
            and checks["all_links_have_targets"]
            and not unresolved
        ) else "REVIEW_REQUIRED"

        return {
            "status": status,
            "checks": checks,
            "duplicate_node_ids": duplicates,
            "missing_containment_edges": missing_containment,
            "missing_dependency_edges": missing_dependencies,
            "link_count": len(link_inventory),
            "unresolved_link_count": len(unresolved),
            "unresolved_links": unresolved,
            "note": (
                "Link source IDs are absent from the supplied XML. "
                "Predecessor edges are inferred; condition references are "
                "evidence only. No Taskflow operations were executed."
            ),
        }
