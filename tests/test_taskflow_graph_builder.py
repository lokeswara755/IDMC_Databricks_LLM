import json
import os
import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from parser.xmlreader import XMLReader
from parser.repository_parser import RepositoryParser
from parser.taskflow_graph_builder import (
    TaskflowGraphBuilder,
    TaskflowGraphValidator,
)


def test_taskflow_graph_builds_from_actual_xml():
    xml_path = os.environ.get("IDMC_TASKFLOW_XML")
    if not xml_path:
        pytest.skip("Set IDMC_TASKFLOW_XML to the actual taskflow XML path.")

    root = XMLReader(xml_path).read()
    parsed = RepositoryParser().parse(root)

    graph = TaskflowGraphBuilder().build(parsed)
    validation = TaskflowGraphValidator().validate(graph)

    assert graph["format"] == "IDMC_TASKFLOW_GRAPH"
    assert graph["taskflow_name"] == "wf_s_m_TIM_STG_UNQ_WRKSTN_NOMTCH_SYNC"
    assert graph["summary"]["node_count"] > 0
    assert graph["summary"]["flow_count"] > 0
    assert graph["summary"]["dependency_edge_count"] > 0
    assert graph["summary"]["execution_performed"] is False

    # The XML's links have targetId but no sourceId. The validator must not
    # silently claim all inferred dependencies are confirmed.
    assert validation["checks"]["all_link_sources_confirmed"] is False
    assert validation["status"] == "REVIEW_REQUIRED"

    print(json.dumps(graph["summary"], indent=2))
    print(json.dumps({
        "validation_status": validation["status"],
        "checks": validation["checks"],
        "unresolved_link_count": validation["unresolved_link_count"],
    }, indent=2))
