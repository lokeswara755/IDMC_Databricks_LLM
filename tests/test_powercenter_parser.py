"""Run: python test_powercenter_parser.py /path/to/your_export.XML"""
import sys
import xml.etree.ElementTree as ET
from parser.repository_parser import RepositoryParser


def main(xml_path):
    root = ET.parse(xml_path).getroot()
    before = ET.tostring(root, encoding="utf-8")
    result = RepositoryParser().parse(root)
    after = ET.tostring(root, encoding="utf-8")

    assert result["format"] == "POWERCENTER"
    summary = result["summary"]
    assert summary["folder_count"] == 1
    assert summary["folder_session_count"] == 4
    assert summary["workflow_count"] == 1
    assert summary["workflow_session_count"] == 1
    assert summary["total_session_count"] == 5
    assert summary["workflow_task_instance_count"] == 15
    assert summary["workflow_link_count"] == 14
    assert summary["workflow_variable_count"] == 160
    assert summary["session_transformation_instance_count"] == 52
    assert summary["session_extension_count"] == 37
    assert summary["connection_reference_count"] == 25
    assert summary["mapping_reference_count"] == 5
    assert summary["unique_mapping_reference_count"] == 5
    assert summary["mapping_definition_count"] == 0
    assert before == after, "Parser mutated the XML tree."

    print("PASS - PowerCenter XML parser checks")
    print(summary)
    for ref in result["mapping_references"]:
        print(ref["mapping_name"], "<-", ref["session_name"], "[", ref["scope"], "]")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("Usage: python test_powercenter_parser.py /path/to/export.XML")
    main(sys.argv[1])
