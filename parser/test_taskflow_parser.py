import os
import sys
from pathlib import Path
import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from parser.xmlreader import XMLReader
from parser.repository_parser import RepositoryParser


def test_actual_idmc_taskflow_xml_is_detected_and_parsed():
    xml_path = os.environ.get('IDMC_TASKFLOW_XML')
    if not xml_path:
        pytest.skip('Set IDMC_TASKFLOW_XML to the supplied taskflow XML path.')
    root = XMLReader(xml_path).read()
    parsed = RepositoryParser().parse(root)
    assert parsed['format'] == 'IDMC_TASKFLOW'
    assert parsed['asset_metadata']['mimetype'] == 'application/xml+taskflow'
    assert parsed['taskflow']['name'] == 'wf_s_m_TIM_STG_UNQ_WRKSTN_NOMTCH_SYNC'
    assert parsed['summary']['parameter_count'] == 37
    assert parsed['summary']['temp_field_count'] == 42
    assert parsed['summary']['service_count'] == 15
    assert parsed['summary']['data_task_count'] == 5
    assert parsed['summary']['command_task_count'] == 10
    assert parsed['summary']['link_count'] == 99
    assert parsed['summary']['operation_count'] == 276
    assert parsed['summary']['mapping_definition_included'] is False
    assert parsed['taskflow']['flow'] is not None
    assert parsed['taskflow']['raw_xml']['tag'] == 'taskflow'
