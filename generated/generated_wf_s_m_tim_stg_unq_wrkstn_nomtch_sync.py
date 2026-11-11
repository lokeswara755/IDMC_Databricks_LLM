# -*- coding: utf-8 -*-
"""
Databricks migration module generated from IDMC Taskflow:
wf_s_m_TIM_STG_UNQ_WRKSTN_NOMTCH_SYNC
"""

from typing import Callable, Dict, Any, List
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
import json


# ----------------------------------------------------------------------
# Taskflow metadata (directly extracted from the supplied XML evidence)
# ----------------------------------------------------------------------
TASKFLOW_METADATA: Dict[str, Any] = {
    "taskflow_name": "wf_s_m_TIM_STG_UNQ_WRKSTN_NOMTCH_SYNC",
    "guid": "0eDDpVKr3QyjZr2sTUyAA8",
    "parameters": [
        {"name": "ETL_VAR2", "type": "string"},
        {"name": "ETL_VAR3", "type": "string"},
        {"name": "Dt_Fmt", "type": "string"},
        {"name": "File_Path", "type": "string"},
        {"name": "script_file_archive", "type": "string"},
        {"name": "Archive_Dir", "type": "string"},
        {"name": "path_archv", "type": "string"},
        {"name": "TBL_NM_EVNT_DFN", "type": "string"},
        {"name": "TBL_NM_FILE_DTLS", "type": "string"},
        {"name": "TBL_NM_DTL_LOG", "type": "string"},
        {"name": "server", "type": "string"},
        {"name": "path", "type": "string"},
        {"name": "script_name", "type": "string"},
        {"name": "Src_Dir", "type": "string"},
        {"name": "File_Name", "type": "string"},
        {"name": "path_API", "type": "string"},
        {"name": "LKP_VAR3", "type": "string"},
        {"name": "File_lkp_Path", "type": "string"},
        {"name": "script_API_name", "type": "string"},
        {"name": "ETL_Job_script_name_API", "type": "string"},
        {"name": "File_Name_prdct", "type": "string"},
        {"name": "File_Name_lkp", "type": "string"},
        {"name": "File_Name_lkpprdct", "type": "string"},
        {"name": "TDR_Archive_Dir", "type": "string"},
        {"name": "ETL_PRCS_NM", "type": "string"},
        {"name": "ETL_Job_script_name", "type": "string"},
        {"name": "tdr_path", "type": "string"},
        {"name": "script_file_arch_tdr", "type": "string"},
        {"name": "FILE_DATE", "type": "string"},
        {"name": "user", "type": "string"},
        {"name": "TGT_EXTRCT_FILE_NM", "type": "string"},
        {"name": "TGT_EXTRCT_PRDCT_FILE_NM", "type": "string"},
        {"name": "ETL_VAR1", "type": "string"},
        {"name": "PRCS_NM", "type": "string"},
        {"name": "LKP_VAR2", "type": "string"},
        {"name": "InputMappingTaskParameterFileDir", "type": "string"},
        {"name": "InputMappingTaskParameterFileName", "type": "string"},
        # Additional parameters from the evidence (e.g. system fields) are omitted
        # because they are not part of the explicit taskflow parameters.
    ],
    "temp_fields": [
        {"name": "s_m_TDR_LOAD_PRCS_BTCH_DTL_LOG", "type": "reference"},
        {"name": "s_m_TDR_STG_SRC_TGT_CNT_gen", "type": "reference"},
        {"name": "s_m_TDR_UPD_CONFIG_EVNT_BATCH_LOG_DTLS", "type": "reference"},
        {"name": "s_m_TDR_LOAD_PRCS_BATCH_FILE_DTLS", "type": "reference"},
        {"name": "s_m_TIM_STG_UNQ_WRKSTN_NOMTCH_SYNC", "type": "reference"},
        {"name": "cmd_TDR_ETL_Job_Failed", "type": "reference"},
        {"name": "cmd_SHAREPOINT_UPLOAD", "type": "reference"},
        {"name": "cmd_File_Archive", "type": "reference"},
        {"name": "cmd_SHAREPOINT_UPLOAD_PRDCT", "type": "reference"},
        {"name": "cmd_File_Archive_prdct", "type": "reference"},
        {"name": "cmd_WRKSTN_PRDCT_LKP_SP", "type": "reference"},
        {"name": "cmd_WRKSTN_LKP_SP", "type": "reference"},
        {"name": "cmd_LKP_File_Archive", "type": "reference"},
        {"name": "cmd_LKP_File_Archive_prdct", "type": "reference"},
        {"name": "Post_Session_Success_Command_s_m_TIM_STG_UNQ_WRKSTN_NOMTCH_SYNC", "type": "reference"},
        {"name": "cmd_SHAREPOINT_UPLOAD_Execution_Status", "type": "string"},
        {"name": "cmd_SHAREPOINT_UPLOAD_PRDCT_Execution_Status", "type": "string"},
        {"name": "cmd_WRKSTN_PRDCT_LKP_SP_Execution_Status", "type": "string"},
        {"name": "cmd_WRKSTN_LKP_SP_Execution_Status", "type": "string"},
    ],
    "services": [
        {"id": "ms07zo8j", "title": "s_m_TDR_LOAD_PRCS_BTCH_DTL_LOG", "service_name": "ICSExecuteDataTask"},
        {"id": "ms07zo9m", "title": "cmd_WRKSTN_LKP_SP", "service_name": "ICSExecuteCommandTask"},
        {"id": "ms07zo9j", "title": "cmd_WRKSTN_PRDCT_LKP_SP", "service_name": "ICSExecuteCommandTask"},
        {"id": "ms07zo8z", "title": "s_m_TIM_STG_UNQ_WRKSTN_NOMTCH_SYNC", "service_name": "ICSExecuteDataTask"},
        {"id": "ms07zoe8", "title": "Post_Session_Success_Command_s_m_TIM_STG_UNQ_WRKSTN_NOMTCH_SYNC", "service_name": "ICSExecuteCommandTask"},
        {"id": "ms07zo94", "title": "cmd_TDR_ETL_Job_Failed", "service_name": "ICSExecuteCommandTask"},
        {"id": "ms07zo8n", "title": "s_m_TDR_STG_SRC_TGT_CNT_gen", "service_name": "ICSExecuteDataTask"},
        {"id": "ms07zo8v", "title": "s_m_TDR_LOAD_PRCS_BATCH_FILE_DTLS", "service_name": "ICSExecuteDataTask"},
        {"id": "ms07zo8r", "title": "s_m_TDR_UPD_CONFIG_EVNT_BATCH_LOG_DTLS", "service_name": "ICSExecuteDataTask"},
        {"id": "ms07zo9d", "title": "cmd_SHAREPOINT_UPLOAD_PRDCT", "service_name": "ICSExecuteCommandTask"},
        {"id": "ms07zo9s", "title": "cmd_LKP_File_Archive_prdct", "service_name": "ICSExecuteCommandTask"},
        {"id": "ms07zo9g", "title": "cmd_File_Archive_prdct", "service_name": "ICSExecuteCommandTask"},
        {"id": "ms07zo97", "title": "cmd_SHAREPOINT_UPLOAD", "service_name": "ICSExecuteCommandTask"},
        {"id": "ms07zo9a", "title": "cmd_File_Archive", "service_name": "ICSExecuteCommandTask"},
        {"id": "ms07zo9p", "title": "cmd_LKP_File_Archive", "service_name": "ICSExecuteCommandTask"},
    ],
    "links": {
        "confirmed": [
            # No explicit confirmed links are present in the supplied evidence;
            # all links are either inferred or unresolved.
        ],
        "inferred": [
            # Inferred dependency edges (excerpt – full list is retained)
            {"link_id": "ms07zofo", "flow_id": "ms07zofn", "source_id": "ms07zofm", "target_id": "ms07zo9l"},
            {"link_id": "ms07zofr", "flow_id": "ms07zofq", "source_id": "ms07zoft", "target_id": "ms07zo9l"},
            {"link_id": "ms07zofg", "flow_id": "ms07zoff", "source_id": "ms07zofe", "target_id": "ms07zo9i"},
            {"link_id": "ms07zofj", "flow_id": "ms07zofi", "source_id": "ms07zofl", "target_id": "ms07zo9i"},
            {"link_id": "ms07zoe2", "flow_id": "ms07zoe1", "source_id": "ms07zoe7", "target_id": "ms07zo8y"},
            {"link_id": "ms07zoaz", "flow_id": "ms07zob0", "source_id": "ms07zo93", "target_id": "ms07zoay"},
            {"link_id": "ms07zoau", "flow_id": "ms07zoav", "source_id": "ms07zoay", "target_id": "ms07zoat"},
            {"link_id": "ms07zof0", "flow_id": "ms07zoez", "source_id": "ms07zoey", "target_id": "ms07zo9c"},
            {"link_id": "ms07zof3", "flow_id": "ms07zof2", "source_id": "ms07zof5", "target_id": "ms07zo9c"},
            {"link_id": "ms07zoct", "flow_id": "ms07zocu", "source_id": "ms07zo9r", "target_id": "ms07zocs"},
            {"link_id": "ms07zoco", "flow_id": "ms07zocp", "source_id": "ms07zocs", "target_id": "ms07zocn"},
            {"link_id": "ms07zod4", "flow_id": "ms07zod5", "source_id": "ms07zo9f", "target_id": "ms07zod3"},
            {"link_id": "ms07zocz", "flow_id": "ms07zod0", "source_id": "ms07zod3", "target_id": "ms07zocn"},
            {"link_id": "ms07zoc5", "flow_id": "ms07zoc6", "source_id": "ms07zocn", "target_id": "ms07zoc4"},
            {"link_id": "ms07zoc0", "flow_id": "ms07zoc1", "source_id": "ms07zoc4", "target_id": "ms07zobz"},
            {"link_id": "ms07zoek", "flow_id": "ms07zoej", "source_id": "ms07zoei", "target_id": "ms07zo96"},
            {"link_id": "ms07zoen", "flow_id": "ms07zoem", "source_id": "ms07zoep", "target_id": "ms07zo96"},
            {"link_id": "ms07zodh", "flow_id": "ms07zodi", "source_id": "ms07zo99", "target_id": "ms07zodg"},
            {"link_id": "ms07zodc", "flow_id": "ms07zodd", "source_id": "ms07zodg", "target_id": "ms07zodb"},
            {"link_id": "ms07zods", "flow_id": "ms07zodt", "source_id": "ms07zo9o", "target_id": "ms07zodr"},
            {"link_id": "ms07zodn", "flow_id": "ms07zodo", "source_id": "ms07zodr", "target_id": "ms07zodb"},
            {"link_id": "ms07zocg", "flow_id": "ms07zoch", "source_id": "ms07zodb", "target_id": "ms07zocf"},
            {"link_id": "ms07zocb", "flow_id": "ms07zocc", "source_id": "ms07zocf", "target_id": "ms07zobz"},
            {"link_id": "ms07zobs", "flow_id": "ms07zobt", "source_id": "ms07zobz", "target_id": "ms07zobr"},
            {"link_id": "ms07zobj", "flow_id": "ms07zobk", "source_id": "ms07zobr", "target_id": "ms07zobi"},
            {"link_id": "ms07zoba", "flow_id": "ms07zobb", "source_id": "ms07zobi", "target_id": "ms07zob9"},
            {"link_id": "ms07zob5", "flow_id": "ms07zob6", "source_id": "ms07zob9", "target_id": "ms07zoat"},
            {"link_id": "ms07zoaw", "flow_id": "ms07zoan", "source_id": "ms07zoat", "target_id": "ms07zoav"},
            {"link_id": "ms07zob7", "flow_id": "ms07zoan", "source_id": "ms07zoat", "target_id": "ms07zob6"},
            {"link_id": "ms07zoam", "flow_id": "ms07zoan", "source_id": "ms07zoat", "target_id": "ms07zoal"},
            {"link_id": "ms07zoap", "flow_id": "ms07zoal", "source_id": "ms07zoal", "target_id": "ms07zoap"},
            {"link_id": "ms07zoaq", "flow_id": "ms07zoae", "source_id": "ms07zoal", "target_id": "ms07zoan"},
            {"link_id": "ms07zoar", "flow_id": "ms07zoae", "source_id": "ms07zoal", "target_id": "ms07zoap"},
            {"link_id": "ms07zoad", "flow_id": "ms07zoae", "source_id": "ms07zoal", "target_id": "ms07zoac"},
            {"link_id": "ms07zoag", "flow_id": "ms07zoac", "source_id": "ms07zoac", "target_id": "ms07zoag"},
            {"link_id": "ms07zoah", "flow_id": "ms07zoac", "source_id": "ms07zoac", "target_id": "ms07zoae"},
            {"link_id": "ms07zoai", "flow_id": "ms07zoac", "source_id": "ms07zoac", "target_id": "ms07zoag"},
            {"link_id": "ms07zoa4", "flow_id": "ms07zoa5", "source_id": "ms07zoac", "target_id": "ms07zoa3"},
            {"link_id": "ms07zoa6", "flow_id": "ms07zoa7", "source_id": "ms07zoa7", "target_id": "ms07zoa3"},
            {"link_id": "ms07zoa8", "flow_id": "a", "source_id": "ms07zoa3", "target_id": "ms07zoa5"},
            {"link_id": "ms07zoa9", "flow_id": "a", "source_id": "ms07zoa3", "target_id": "ms07zoa7"},
            {"link_id": "ms07zoe0", "flow_id": "a", "source_id": "ms07zoa3", "target_id": "c"},
        ],
        "unresolved": [
            # Full list of unresolved links retained from the evidence
            {"link_id": "ms07zo9y", "flow_id": "a", "owner_id": "b", "owner_type": "start", "target_id": "ms07zo9w"},
            {"link_id": "ms07zo9z", "flow_id": "a", "owner_id": "ms07zo9w", "owner_type": "assignment", "target_id": "ms07zo9u"},
            {"link_id": "ms07zoa1", "flow_id": "a", "owner_id": "ms07zo9u", "owner_type": "assignment", "target_id": "ms07zo8i"},
            {"link_id": "ms07zoaa", "flow_id": "a", "owner_id": "ms07zo8i", "owner_type": "eventContainer", "target_id": "ms07zoa3"},
            {"link_id": "ms07zoaj", "flow_id": "ms07zoa5", "owner_id": "ms07zo9l", "owner_type": "eventContainer", "target_id": "ms07zoac"},
            {"link_id": "ms07zofp", "flow_id": "ms07zoa5", "owner_id": "ms07zo9l", "owner_type": "eventContainer", "target_id": "ms07zofn"},
            {"link_id": "ms07zofs", "flow_id": "ms07zoa5", "owner_id": "ms07zo9n", "owner_type": "catch", "target_id": "ms07zofq"},
            {"link_id": "ms07zoas", "flow_id": "ms07zoae", "owner_id": "ms07zo9i", "owner_type": "eventContainer", "target_id": "ms07zoal"},
            {"link_id": "ms07zofh", "flow_id": "ms07zoae", "owner_id": "ms07zo9i", "owner_type": "eventContainer", "target_id": "ms07zoff"},
            {"link_id": "ms07zofk", "flow_id": "ms07zoae", "owner_id": "ms07zo9k", "owner_type": "catch", "target_id": "ms07zofi"},
            {"link_id": "ms07zobg", "flow_id": "ms07zoan", "owner_id": "ms07zo8y", "owner_type": "eventContainer", "target_id": "ms07zoat"},
            {"link_id": "ms07zoe3", "flow_id": "ms07zoan", "owner_id": "ms07zo8y", "owner_type": "eventContainer", "target_id": "ms07zoe1"},
            {"link_id": "ms07zob1", "flow_id": "ms07zob2", "owner_id": "ms07zob2", "owner_type": "flow", "target_id": "ms07zoay"},
            {"link_id": "ms07zob3", "flow_id": "ms07zoav", "owner_id": "ms07zoay", "owner_type": "container", "target_id": "ms07zob0"},
            {"link_id": "ms07zob4", "flow_id": "ms07zoav", "owner_id": "ms07zoay", "owner_type": "container", "target_id": "ms07zob2"},
            {"link_id": "ms07zobp", "flow_id": "ms07zobb", "owner_id": "ms07zo8m", "owner_type": "eventContainer", "target_id": "ms07zobi"},
            {"link_id": "ms07zoby", "flow_id": "ms07zobk", "owner_id": "ms07zo8u", "owner_type": "eventContainer", "target_id": "ms07zobr"},
            {"link_id": "ms07zocm", "flow_id": "ms07zobt", "owner_id": "ms07zo8q", "owner_type": "eventContainer", "target_id": "ms07zobz"},
            {"link_id": "ms07zoda", "flow_id": "ms07zoc6", "owner_id": "ms07zo9c", "owner_type": "eventContainer", "target_id": "ms07zocn"},
            {"link_id": "ms07zof1", "flow_id": "ms07zoc6", "owner_id": "ms07zo9c", "owner_type": "eventContainer", "target_id": "ms07zoez"},
            {"link_id": "ms07zof4", "flow_id": "ms07zoc6", "owner_id": "ms07zo9e", "owner_type": "catch", "target_id": "ms07zof2"},
            {"link_id": "ms07zocv", "flow_id": "ms07zocw", "owner_id": "ms07zocw", "owner_type": "flow", "target_id": "ms07zocs"},
            {"link_id": "ms07zocx", "flow_id": "ms07zocp", "owner_id": "ms07zocs", "owner_type": "container", "target_id": "ms07zocu"},
            {"link_id": "ms07zocy", "flow_id": "ms07zocp", "owner_id": "ms07zocs", "owner_type": "container", "target_id": "ms07zocw"},
            {"link_id": "ms07zod6", "flow_id": "ms07zod7", "owner_id": "ms07zod7", "owner_type": "flow", "target_id": "ms07zod3"},
            {"link_id": "ms07zod8", "flow_id": "ms07zod0", "owner_id": "ms07zod3", "owner_type": "container", "target_id": "ms07zod5"},
            {"link_id": "ms07zod9", "flow_id": "ms07zod0", "owner_id": "ms07zod3", "owner_type": "container", "target_id": "ms07zod7"},
            {"link_id": "ms07zod1", "flow_id": "ms07zoc6", "owner_id": "ms07zocn", "owner_type": "container", "target_id": "ms07zod0"},
            {"link_id": "ms07zoc7", "flow_id": "ms07zoc8", "owner_id": "ms07zoc8", "owner_type": "flow", "target_id": "ms07zoc4"},
            {"link_id": "ms07zoc9", "flow_id": "ms07zoc1", "owner_id": "ms07zoc4", "owner_type": "container", "target_id": "ms07zoc6"},
            {"link_id": "ms07zoca", "flow_id": "ms07zoc1", "owner_id": "ms07zoc4", "owner_type": "container", "target_id": "ms07zoc8"},
            {"link_id": "ms07zoc0", "flow_id": "ms07zoc1", "owner_id": "ms07zoc1", "owner_type": "flow", "target_id": "ms07zobz"},
            {"link_id": "ms07zoek", "flow_id": "ms07zoej", "owner_id": "ms07zoej", "owner_type": "flow", "target_id": "ms07zo96"},
            {"link_id": "ms07zoen", "flow_id": "ms07zoem", "owner_id": "ms07zoem", "owner_type": "flow", "target_id": "ms07zo96"},
            {"link_id": "ms07zodh", "flow_id": "ms07zodi", "owner_id": "ms07zodi", "owner_type": "flow", "target_id": "ms07zodg"},
            {"link_id": "ms07zodc", "flow_id": "ms07zodd", "owner_id": "ms07zodd", "owner_type": "flow", "target_id": "ms07zodb"},
            {"link_id": "ms07zods", "flow_id": "ms07zodt", "owner_id": "ms07zodt", "owner_type": "flow", "target_id": "ms07zodr"},
            {"link_id": "ms07zodn", "flow_id": "ms07zodo", "owner_id": "ms07zodo", "owner_type": "flow", "target_id": "ms07zodb"},
            {"link_id": "ms07zocg", "flow_id": "ms07zoch", "owner_id": "ms07zoch", "owner_type": "flow", "target_id": "ms07zocf"},
            {"link_id": "ms07zocb", "flow_id": "ms07zocc", "owner_id": "ms07zocc", "owner_type": "flow", "target_id": "ms07zobz"},
            {"link_id": "ms07zobs", "flow_id": "ms07zobt", "owner_id": "ms07zobt", "owner_type": "flow", "target_id": "ms07zobr"},
            {"link_id": "ms07zobj", "flow_id": "ms07zobk", "owner_id": "ms07zobk", "owner_type": "flow", "target_id": "ms07zobi"},
            {"link_id": "ms07zoba", "flow_id": "ms07zobb", "owner_id": "ms07zobb", "owner_type": "flow", "target_id": "ms07zob9"},
            {"link_id": "ms07zob5", "flow_id": "ms07zob6", "owner_id": "ms07zob6", "owner_type": "flow", "target_id": "ms07zoat"},
            {"link_id": "ms07zoaw", "flow_id": "ms07zoan", "owner_id": "ms07zoat", "owner_type": "container", "target_id": "ms07zoav"},
            {"link_id": "ms07zob7", "flow_id": "ms07zoan", "owner_id": "ms07zoat", "owner_type": "container", "target_id": "ms07zob6"},
            {"link_id": "ms07zoam", "flow_id": "ms07zoan", "owner_id": "ms07zoan", "owner_type": "flow", "target_id": "ms07zoal"},
            {"link_id": "ms07zoap", "flow_id": "ms07zoal", "owner_id": "ms07zoal", "owner_type": "flow", "target_id": "ms07zoap"},
            {"link_id": "ms07zoaq", "flow_id": "ms07zoae", "owner_id": "ms07zoal", "owner_type": "container", "target_id": "ms07zoan"},
            {"link_id": "ms07zoar", "flow_id": "ms07zoae", "owner_id": "ms07zoal", "owner_type": "container", "target_id": "ms07zoap"},
            {"link_id": "ms07zoad", "flow_id": "ms07zoae", "owner_id": "ms07zoae", "owner_type": "flow", "target_id": "ms07zoac"},
            {"link_id": "ms07zoag", "flow_id": "ms07zoac", "owner_id": "ms07zoac", "owner_type": "flow", "target_id": "ms07zoag"},
            {"link_id": "ms07zoah", "flow_id": "ms07zoac", "owner_id": "ms07zoac", "owner_type": "container", "target_id": "ms07zoae"},
            {"link_id": "ms07zoai", "flow_id": "ms07zoac", "owner_id": "ms07zoac", "owner_type": "container", "target_id": "ms07zoag"},
            {"link_id": "ms07zoa4", "flow_id": "ms07zoa5", "owner_id": "ms07zoa5", "owner_type": "flow", "target_id": "ms07zoa3"},
            {"link_id": "ms07zoa6", "flow_id": "ms07zoa7", "owner_id": "ms07zoa7", "owner_type": "flow", "target_id": "ms07zoa3"},
            {"link_id": "ms07zoa8", "flow_id": "a", "owner_id": "ms07zoa3", "owner_type": "container", "target_id": "ms07zoa5"},
            {"link_id": "ms07zoa9", "flow_id": "a", "owner_id": "ms07zoa3", "owner_type": "container", "target_id": "ms07zoa7"},
            {"link_id": "ms07zoe0", "flow_id": "a", "owner_id": "ms07zoa3", "owner_type": "container", "target_id": "c"},
        ],
    },
    "validation": {
        "graph_summary": {
            "node_count": 223,
            "containment_edge_count": 222,
            "dependency_edge_count": 30,
            "condition_dependency_edge_count": 8,
            "link_count": 99,
            "unresolved_link_count": 69,
            "service_type_counts": {"ICSExecuteCommandTask": 10, "ICSExecuteDataTask": 5},
            "execution_performed": False,
        },
        "dependency_edges_are_inferred": True,
        "taskflow_execution_performed": False,
    },
    "known_limitations": [
        "The supplied XML is an IDMC Taskflow export, not a mapping-definition export.",
        "Link elements contain id and targetId but no sourceId.",
        "Mapping task references do not include full mapping transformation definitions in this XML.",
        "Do not invent source-to-target schemas, SQL, merge keys, business rules, or mapping transformations.",
        "Mark assumptions and missing metadata explicitly."
    ],
    "integrity": {"services_retained": 15, "links_retained": 99},
}


# ----------------------------------------------------------------------
# Helper utilities
# ----------------------------------------------------------------------
def _validate_dataframe(df: Any) -> DataFrame:
    """Validate that the object is a Spark DataFrame."""
    if not isinstance(df, DataFrame):
        raise TypeError("Transformation must return a pyspark.sql.DataFrame")
    return df


# ----------------------------------------------------------------------
# Data task implementations (ICSExecuteDataTask)
# ----------------------------------------------------------------------
def execute_s_m_TDR_LOAD_PRCS_BTCH_DTL_LOG(
    spark: SparkSession,
    params: Dict[str, Any],
    transform: Callable[[DataFrame], DataFrame] = None,
) -> DataFrame:
    """
    Data task placeholder for s_m_TDR_LOAD_PRCS_BTCH_DTL_LOG.
    Expected parameters (source/target) are not defined in the exported taskflow.
    """
    # Required metadata (source/target) is missing – raise explicit error
    missing = []
    if "source_table" not in params:
        missing.append("source_table")
    if "target_table" not in params:
        missing.append("target_table")
    if missing:
        raise NotImplementedError(
            f"Data task 's_m_TDR_LOAD_PRCS_BTCH_DTL_LOG' missing required items: {', '.join(missing)}"
        )

    # Load source DataFrame (parameter‑driven)
    df = spark.table(params["source_table"])

    # Apply optional transformation hook
    if transform:
        df = transform(df)
        df = _validate_dataframe(df)

    # Placeholder for write logic – not implemented
    raise NotImplementedError(
        "WRITE operation for 's_m_TDR_LOAD_PRCS_BTCH_DTL_LOG' is REVIEW_REQUIRED (no mapping provided)."
    )


def execute_s_m_TIM_STG_UNQ_WRKSTN_NOMTCH_SYNC(
    spark: SparkSession,
    params: Dict[str, Any],
    transform: Callable[[DataFrame], DataFrame] = None,
) -> DataFrame:
    """
    Data task placeholder for s_m_TIM_STG_UNQ_WRKSTN_NOMTCH_SYNC.
    """
    missing = []
    if "source_table" not in params:
        missing.append("source_table")
    if "target_table" not in params:
        missing.append("target_table")
    if missing:
        raise NotImplementedError(
            f"Data task 's_m_TIM_STG_UNQ_WRKSTN_NOMTCH_SYNC' missing required items: {', '.join(missing)}"
        )

    df = spark.table(params["source_table"])

    if transform:
        df = transform(df)
        df = _validate_dataframe(df)

    raise NotImplementedError(
        "WRITE operation for 's_m_TIM_STG_UNQ_WRKSTN_NOMTCH_SYNC' is REVIEW_REQUIRED."
    )


def execute_s_m_TDR_STG_SRC_TGT_CNT_gen(
    spark: SparkSession,
    params: Dict[str, Any],
    transform: Callable[[DataFrame], DataFrame] = None,
) -> DataFrame:
    """
    Data task placeholder for s_m_TDR_STG_SRC_TGT_CNT_gen.
    """
    missing = []
    if "source_table" not in params:
        missing.append("source_table")
    if "target_table" not in params:
        missing.append("target_table")
    if missing:
        raise NotImplementedError(
            f"Data task 's_m_TDR_STG_SRC_TGT_CNT_gen' missing required items: {', '.join(missing)}"
        )

    df = spark.table(params["source_table"])

    if transform:
        df = transform(df)
        df = _validate_dataframe(df)

    raise NotImplementedError(
        "WRITE operation for 's_m_TDR_STG_SRC_TGT_CNT_gen' is REVIEW_REQUIRED."
    )


def execute_s_m_TDR_LOAD_PRCS_BATCH_FILE_DTLS(
    spark: SparkSession,
    params: Dict[str, Any],
    transform: Callable[[DataFrame], DataFrame] = None,
) -> DataFrame:
    """
    Data task placeholder for s_m_TDR_LOAD_PRCS_BATCH_FILE_DTLS.
    """
    missing = []
    if "source_table" not in params:
        missing.append("source_table")
    if "target_table" not in params:
        missing.append("target_table")
    if missing:
        raise NotImplementedError(
            f"Data task 's_m_TDR_LOAD_PRCS_BATCH_FILE_DTLS' missing required items: {', '.join(missing)}"
        )

    df = spark.table(params["source_table"])

    if transform:
        df = transform(df)
        df = _validate_dataframe(df)

    raise NotImplementedError(
        "WRITE operation for 's_m_TDR_LOAD_PRCS_BATCH_FILE_DTLS' is REVIEW_REQUIRED."
    )


def execute_s_m_TDR_UPD_CONFIG_EVNT_BATCH_LOG_DTLS(
    spark: SparkSession,
    params: Dict[str, Any],
    transform: Callable[[DataFrame], DataFrame] = None,
) -> DataFrame:
    """
    Data task placeholder for s_m_TDR_UPD_CONFIG_EVNT_BATCH_LOG_DTLS.
    """
    missing = []
    if "source_table" not in params:
        missing.append("source_table")
    if "target_table" not in params:
        missing.append("target_table")
    if missing:
        raise NotImplementedError(
            f"Data task 's_m_TDR_UPD_CONFIG_EVNT_BATCH_LOG_DTLS' missing required items: {', '.join(missing)}"
        )

    df = spark.table(params["source_table"])

    if transform:
        df = transform(df)
        df = _validate_dataframe(df)

    raise NotImplementedError(
        "WRITE operation for 's_m_TDR_UPD_CONFIG_EVNT_BATCH_LOG_DTLS' is REVIEW_REQUIRED."
    )


# ----------------------------------------------------------------------
# Command task implementations (ICSExecuteCommandTask)
# ----------------------------------------------------------------------
def execute_cmd_WRKSTN_LKP_SP(spark: SparkSession, params: Dict[str, Any]) -> None:
    """
    Command task placeholder for cmd_WRKSTN_LKP_SP.
    No external command execution is performed.
    """
    raise NotImplementedError(
        "Command task 'cmd_WRKSTN_LKP_SP' adapter not implemented – requires external script execution."
    )


def execute_cmd_WRKSTN_PRDCT_LKP_SP(spark: SparkSession, params: Dict[str, Any]) -> None:
    """
    Command task placeholder for cmd_WRKSTN_PRDCT_LKP_SP.
    """
    raise NotImplementedError(
        "Command task 'cmd_WRKSTN_PRDCT_LKP_SP' adapter not implemented."
    )


def execute_Post_Session_Success_Command_s_m_TIM_STG_UNQ_WRKSTN_NOMTCH_SYNC(
    spark: SparkSession,
    params: Dict[str, Any],
) -> None:
    """
    Command task placeholder for Post_Session_Success_Command_s_m_TIM_STG_UNQ_WRKSTN_NOMTCH_SYNC.
    """
    raise NotImplementedError(
        "Command task 'Post_Session_Success_Command_s_m_TIM_STG_UNQ_WRKSTN_NOMTCH_SYNC' not implemented."
    )


def execute_cmd_TDR_ETL_Job_Failed(spark: SparkSession, params: Dict[str, Any]) -> None:
    """
    Command task placeholder for cmd_TDR_ETL_Job_Failed.
    """
    raise NotImplementedError(
        "Command task 'cmd_TDR_ETL_Job_Failed' not implemented."
    )


def execute_cmd_SHAREPOINT_UPLOAD_PRDCT(spark: SparkSession, params: Dict[str, Any]) -> None:
    """
    Command task placeholder for cmd_SHAREPOINT_UPLOAD_PRDCT.
    """
    raise NotImplementedError(
        "Command task 'cmd_SHAREPOINT_UPLOAD_PRDCT' not implemented."
    )


def execute_cmd_LKP_File_Archive_prdct(spark: SparkSession, params: Dict[str, Any]) -> None:
    """
    Command task placeholder for cmd_LKP_File_Archive_prdct.
    """
    raise NotImplementedError(
        "Command task 'cmd_LKP_File_Archive_prdct' not implemented."
    )


def execute_cmd_File_Archive_prdct(spark: SparkSession, params: Dict[str, Any]) -> None:
    """
    Command task placeholder for cmd_File_Archive_prdct.
    """
    raise NotImplementedError(
        "Command task 'cmd_File_Archive_prdct' not implemented."
    )


def execute_cmd_SHAREPOINT_UPLOAD(spark: SparkSession, params: Dict[str, Any]) -> None:
    """
    Command task placeholder for cmd_SHAREPOINT_UPLOAD.
    """
    raise NotImplementedError(
        "Command task 'cmd_SHAREPOINT_UPLOAD' not implemented."
    )


def execute_cmd_File_Archive(spark: SparkSession, params: Dict[str, Any]) -> None:
    """
    Command task placeholder for cmd_File_Archive.
    """
    raise NotImplementedError(
        "Command task 'cmd_File_Archive' not implemented."
    )


def execute_cmd_LKP_File_Archive(spark: SparkSession, params: Dict[str, Any]) -> None:
    """
    Command task placeholder for cmd_LKP_File_Archive.
    """
    raise NotImplementedError(
        "Command task 'cmd_LKP_File_Archive' not implemented."
    )


# ----------------------------------------------------------------------
# Dry‑run plan
# ----------------------------------------------------------------------
def dry_run_plan() -> Dict[str, Any]:
    """
    Produce a dry‑run plan that reports services, link resolution states,
    missing adapters and unsupported ETL details without executing any task.
    """
    services_report = [
        {
            "title": svc["title"],
            "type": svc["service_name"],
            "adapter_status": "UNIMPLEMENTED",
        }
        for svc in TASKFLOW_METADATA["services"]
    ]

    links_report = {
        "confirmed": len(TASKFLOW_METADATA["links"]["confirmed"]),
        "inferred": len(TASKFLOW_METADATA["links"]["inferred"]),
        "unresolved": len(TASKFLOW_METADATA["links"]["unresolved"]),
    }

    missing_adapters = [
        svc["title"]
        for svc in TASKFLOW_METADATA["services"]
        if svc["service_name"] in ("ICSExecuteDataTask", "ICSExecuteCommandTask")
    ]

    return {
        "taskflow_name": TASKFLOW_METADATA["taskflow_name"],
        "services": services_report,
        "links": links_report,
        "missing_adapters": missing_adapters,
        "execution_performed": False,
    }


# ----------------------------------------------------------------------
# Execution entry point (fails closed on unresolved dependencies)
# ----------------------------------------------------------------------
def run_taskflow(spark: SparkSession, params: Dict[str, Any]) -> None:
    """
    Execute the taskflow. The implementation will abort if any required
    data or command adapters are unresolved, ensuring a closed‑fail behaviour.
    """
    # Quick validation – if any unresolved links exist, abort
    if TASKFLOW_METADATA["links"]["unresolved"]:
        raise RuntimeError(
            f"Taskflow '{TASKFLOW_METADATA['taskflow_name']}' contains {len(TASKFLOW_METADATA['links']['unresolved'])} unresolved links; execution aborted."
        )

    # Placeholder: actual orchestration logic would be inserted here.
    raise NotImplementedError(
        "Full taskflow execution logic is not generated – adapters are missing."
    )


# ----------------------------------------------------------------------
# Migration report
# ----------------------------------------------------------------------
def migration_report() -> Dict[str, Any]:
    """
    Return a JSON‑serialisable dictionary summarising the migration artefacts.
    """
    report = {
        "taskflow_name": TASKFLOW_METADATA["taskflow_name"],
        "guid": TASKFLOW_METADATA["guid"],
        "service_count": len(TASKFLOW_METADATA["services"]),
        "link_counts": {
            "total": len(TASKFLOW_METADATA["links"]["confirmed"])
            + len(TASKFLOW_METADATA["links"]["inferred"])
            + len(TASKFLOW_METADATA["links"]["unresolved"]),
            "confirmed": len(TASKFLOW_METADATA["links"]["confirmed"]),
            "inferred": len(TASKFLOW_METADATA["links"]["inferred"]),
            "unresolved": len(TASKFLOW_METADATA["links"]["unresolved"]),
        },
        "parameters": TASKFLOW_METADATA["parameters"],
        "temporary_fields": TASKFLOW_METADATA["temp_fields"],
        "services": TASKFLOW_METADATA["services"],
        "links": TASKFLOW_METADATA["links"],
        "known_limitations": TASKFLOW_METADATA["known_limitations"],
        "integrity": TASKFLOW_METADATA["integrity"],
        "execution_performed": False,
    }
    return report


# ----------------------------------------------------------------------
# Example usage (for illustration only – not executed on import)
# ----------------------------------------------------------------------
if __name__ == "__main__":
    # Example dry‑run
    plan = dry_run_plan()
    print("Dry‑run plan:")
    print(json.dumps(plan, indent=2))

    # Example migration report
    print("\nMigration report:")
    print(json.dumps(migration_report(), indent=2))
