"""Read-only parser for Informatica PowerCenter repository XML exports.

The parser deliberately separates session/workflow metadata from mapping
lineage. A SESSION's MAPPINGNAME is a reference; it is not a mapping definition.
"""
from __future__ import annotations

from collections import Counter
from pathlib import Path
import hashlib
import xml.etree.ElementTree as ET


def element_to_dict(element):
    """Preserve an XML element recursively without modifying the source tree."""
    result = {
        "tag": element.tag,
        "attributes": dict(element.attrib),
        "children": [element_to_dict(child) for child in list(element)],
    }
    if element.text and element.text.strip():
        result["text"] = element.text.strip()
    if element.tail and element.tail.strip():
        result["tail"] = element.tail.strip()
    return result


def _attrs(element):
    return dict(element.attrib) if element is not None else {}


def _children(element, tag):
    return [child for child in list(element) if child.tag == tag]


def _attributes(element):
    return [dict(child.attrib) for child in _children(element, "ATTRIBUTE")]


class PowerCenterRepositoryParser:
    """Parse POWERMART -> REPOSITORY -> FOLDER exports into JSON-safe metadata."""

    def parse(self, root):
        if root is None or root.tag.upper() != "POWERMART":
            raise ValueError("PowerCenter parser expects a POWERMART root element.")

        repository_element = next(
            (child for child in list(root) if child.tag == "REPOSITORY"), None
        )
        if repository_element is None:
            raise ValueError("POWERMART XML does not contain a direct REPOSITORY element.")

        folders = [self._parse_folder(item) for item in _children(repository_element, "FOLDER")]
        mapping_refs = []
        for folder in folders:
            mapping_refs.extend(folder["mapping_references"])

        root_counts = Counter(child.tag for child in list(root))
        repo_counts = Counter(child.tag for child in list(repository_element))
        workflow_sessions = sum(len(w["sessions"]) for f in folders for w in f["workflows"])
        folder_sessions = sum(len(f["sessions"]) for f in folders)
        workflows = [w for f in folders for w in f["workflows"]]
        all_sessions = [s for f in folders for s in f["sessions"]]
        all_sessions.extend(s for w in workflows for s in w["sessions"])
        transformation_type_counts = Counter(
            item.get("transformation_type") or "UNKNOWN"
            for session in all_sessions
            for item in session["transformation_instances"]
        )
        connection_reference_count = sum(
            len(extension["connection_references"])
            for session in all_sessions
            for extension in session["session_extensions"]
        ) + sum(
            len(extension["connection_references"])
            for workflow in workflows
            for task_instance in workflow["task_instances"]
            for extension in task_instance["session_extensions"]
        )
        session_extension_count = sum(
            len(session["session_extensions"]) for session in all_sessions
        ) + sum(
            len(task_instance["session_extensions"])
            for workflow in workflows
            for task_instance in workflow["task_instances"]
        )
        mapping_definition_count = sum(1 for item in root.iter() if item.tag.upper() == "MAPPING")
        return {
            "format": "POWERCENTER",
            "format_version": "POWERMART",
            "repository": {
                "name": repository_element.get("NAME"),
                "version": repository_element.get("VERSION"),
                "codepage": repository_element.get("CODEPAGE"),
                "database_type": repository_element.get("DATABASETYPE"),
                "attributes": _attrs(repository_element),
            },
            "name": repository_element.get("NAME"),
            "xml_header": {
                "creation_date": root.get("CREATION_DATE"),
                "repository_version": root.get("REPOSITORY_VERSION"),
                "attributes": _attrs(root),
                "direct_child_counts": dict(root_counts),
            },
            "folders": folders,
            "mapping_references": mapping_refs,
            "summary": {
                "folder_count": len(folders),
                "direct_repository_child_counts": dict(repo_counts),
                "session_count": folder_sessions,
                "folder_session_count": folder_sessions,
                "workflow_session_count": workflow_sessions,
                "total_session_count": folder_sessions + workflow_sessions,
                "workflow_count": len(workflows),
                "workflow_task_instance_count": sum(len(w["task_instances"]) for w in workflows),
                "workflow_link_count": sum(len(w["workflow_links"]) for w in workflows),
                "workflow_variable_count": sum(len(w["workflow_variables"]) for w in workflows),
                "session_transformation_instance_count": sum(len(s["transformation_instances"]) for s in all_sessions),
                "session_extension_count": session_extension_count,
                "connection_reference_count": connection_reference_count,
                "transformation_type_counts": dict(transformation_type_counts),
                "mapping_reference_count": len(mapping_refs),
                "unique_mapping_reference_count": len({m["mapping_name"] for m in mapping_refs}),
                "mapping_definition_count": mapping_definition_count,
                "mapping_definitions_present": mapping_definition_count > 0,
            },
            "limitations": [
                "This export contains session references (SESSION/@MAPPINGNAME) but no MAPPING definition elements.",
                "Mapping-level source-to-transformation-to-target lineage cannot be reconstructed from session instances alone.",
                "Session transformation instances are configuration/runtime references, not a substitute for mapping definitions and connector links.",
            ],
            "source_integrity": {
                "parser_is_read_only": True,
                "xml_tree_mutated": False,
            },
        }

    def _parse_folder(self, folder):
        direct_sessions = [self._parse_session(item, "FOLDER") for item in _children(folder, "SESSION")]
        workflows = [self._parse_workflow(item) for item in _children(folder, "WORKFLOW")]
        folder_tasks = [self._parse_task(item) for item in _children(folder, "TASK")]
        configs = [self._parse_config(item) for item in _children(folder, "CONFIG")]
        mapping_refs = []
        for session in direct_sessions:
            if session.get("mapping_name"):
                mapping_refs.append(self._mapping_reference(session, "FOLDER_SESSION"))
        for workflow in workflows:
            for session in workflow["sessions"]:
                if session.get("mapping_name"):
                    mapping_refs.append(self._mapping_reference(session, "WORKFLOW_SESSION"))
        return {
            "name": folder.get("NAME"),
            "attributes": _attrs(folder),
            "sessions": direct_sessions,
            "workflows": workflows,
            "tasks": folder_tasks,
            "configs": configs,
            "mapping_references": mapping_refs,
            "raw_extensions": [element_to_dict(c) for c in list(folder)
                               if c.tag not in {"SESSION", "WORKFLOW", "TASK", "CONFIG"}],
            "child_counts": dict(Counter(c.tag for c in list(folder))),
        }

    def _parse_session(self, session, scope):
        instances = []
        for item in _children(session, "SESSTRANSFORMATIONINST"):
            instances.append({
                "session_instance_name": item.get("SINSTANCENAME"),
                "transformation_name": item.get("TRANSFORMATIONNAME"),
                "transformation_type": item.get("TRANSFORMATIONTYPE"),
                "pipeline": item.get("PIPELINE"),
                "stage": item.get("STAGE"),
                "is_repartition_point": item.get("ISREPARTITIONPOINT"),
                "partition_type": item.get("PARTITIONTYPE"),
                "attributes": _attrs(item),
                "details": [element_to_dict(c) for c in list(item)],
            })
        extensions = []
        for item in _children(session, "SESSIONEXTENSION"):
            connection_refs = [dict(c.attrib) for c in _children(item, "CONNECTIONREFERENCE")]
            extensions.append({
                "name": item.get("NAME"),
                "instance_name": item.get("SINSTANCENAME"),
                "subtype": item.get("SUBTYPE"),
                "transformation_type": item.get("TRANSFORMATIONTYPE"),
                "type": item.get("TYPE"),
                "connection_references": connection_refs,
                "attributes": _attributes(item),
                "raw": element_to_dict(item),
            })
        return {
            "scope": scope,
            "name": session.get("NAME"),
            "mapping_name": session.get("MAPPINGNAME"),
            "description": session.get("DESCRIPTION"),
            "is_valid": session.get("ISVALID"),
            "reusable": session.get("REUSABLE"),
            "version_number": session.get("VERSIONNUMBER"),
            "sort_order": session.get("SORTORDER"),
            "attributes": _attrs(session),
            "transformation_instances": instances,
            "session_extensions": extensions,
            "config_references": [dict(c.attrib) for c in _children(session, "CONFIGREFERENCE")],
            "metadata_extensions": [element_to_dict(c) for c in _children(session, "METADATAEXTENSION")],
            "other_children": [element_to_dict(c) for c in list(session)
                               if c.tag not in {"ATTRIBUTE", "SESSTRANSFORMATIONINST", "SESSIONEXTENSION", "CONFIGREFERENCE", "METADATAEXTENSION"}],
            "session_attributes": _attributes(session),
            "child_counts": dict(Counter(c.tag for c in list(session))),
        }

    @staticmethod
    def _mapping_reference(session, scope):
        return {
            "mapping_name": session.get("mapping_name"),
            "session_name": session.get("name"),
            "scope": scope,
            "session_validity": session.get("is_valid"),
            "mapping_definition_available_in_this_export": False,
        }

    def _parse_task_instance(self, task_instance):
        session_extensions = []
        for item in _children(task_instance, "SESSIONEXTENSION"):
            session_extensions.append({
                "name": item.get("NAME"),
                "instance_name": item.get("SINSTANCENAME"),
                "subtype": item.get("SUBTYPE"),
                "transformation_type": item.get("TRANSFORMATIONTYPE"),
                "type": item.get("TYPE"),
                "connection_references": [dict(c.attrib) for c in _children(item, "CONNECTIONREFERENCE")],
                "attributes": _attributes(item),
                "raw": element_to_dict(item),
            })
        return {
            "attributes": _attrs(task_instance),
            "session_extensions": session_extensions,
            "raw": element_to_dict(task_instance),
        }

    def _parse_workflow(self, workflow):
        sessions = [self._parse_session(item, "WORKFLOW") for item in _children(workflow, "SESSION")]
        task_instances = [self._parse_task_instance(item) for item in _children(workflow, "TASKINSTANCE")]
        links = [dict(item.attrib) for item in _children(workflow, "WORKFLOWLINK")]
        tasks = [self._parse_task(item) for item in _children(workflow, "TASK")]
        variables = [dict(item.attrib) for item in _children(workflow, "WORKFLOWVARIABLE")]
        schedulers = [element_to_dict(item) for item in _children(workflow, "SCHEDULER")]
        graph_nodes = [{"name": item.get("NAME"), "task_name": item.get("TASKNAME"),
                        "task_type": item.get("TASKTYPE"), "attributes": _attrs(item)}
                       for item in _children(workflow, "TASKINSTANCE")]
        return {
            "name": workflow.get("NAME"),
            "description": workflow.get("DESCRIPTION"),
            "is_enabled": workflow.get("ISENABLED"),
            "is_valid": workflow.get("ISVALID"),
            "version_number": workflow.get("VERSIONNUMBER"),
            "attributes": _attrs(workflow),
            "task_instances": task_instances,
            "workflow_links": links,
            "tasks": tasks,
            "workflow_variables": variables,
            "sessions": sessions,
            "schedulers": schedulers,
            "metadata_extensions": [element_to_dict(c) for c in _children(workflow, "METADATAEXTENSION")],
            "workflow_graph": {"nodes": graph_nodes, "edges": links},
            "child_counts": dict(Counter(c.tag for c in list(workflow))),
            "raw_unclassified_children": [element_to_dict(c) for c in list(workflow)
                if c.tag not in {"TASKINSTANCE", "WORKFLOWLINK", "TASK", "WORKFLOWVARIABLE", "SESSION", "SCHEDULER", "METADATAEXTENSION", "ATTRIBUTE"}],
        }

    @staticmethod
    def _parse_task(task):
        return {
            "name": task.get("NAME"),
            "type": task.get("TYPE"),
            "reusable": task.get("REUSABLE"),
            "version_number": task.get("VERSIONNUMBER"),
            "attributes": _attrs(task),
            "task_attributes": _attributes(task),
            "value_pairs": [dict(c.attrib) for c in _children(task, "VALUEPAIR")],
            "raw": element_to_dict(task),
        }

    @staticmethod
    def _parse_config(config):
        return {
            "name": config.get("NAME"),
            "description": config.get("DESCRIPTION"),
            "is_default": config.get("ISDEFAULT"),
            "version_number": config.get("VERSIONNUMBER"),
            "attributes": _attrs(config),
            "config_attributes": _attributes(config),
            "raw": element_to_dict(config),
        }
