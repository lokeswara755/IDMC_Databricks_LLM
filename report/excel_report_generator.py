"""
IDMC to Databricks - Excel Migration Report Generator

Generates:
    1. IDMC_Repository_Analysis.xlsx
    2. migration_run_summary.json

The report uses the actual results returned by MigrationPipeline.run().
It does not execute migrations or write to target tables.
"""

import os
import json
import shutil
import xml.etree.ElementTree as ET

from datetime import datetime, timezone
from collections import Counter

from openpyxl import Workbook
from openpyxl.styles import (
    Font,
    PatternFill,
    Border,
    Side,
    Alignment
)
from openpyxl.utils import get_column_letter


# =====================================================================
# MAIN REPORT GENERATOR
# =====================================================================

def generate_excel_report(
    migration_results,
    xml_path,
    metadata_path,
    output_dir,
    spark,
    dbutils
):
    """
    Generate the Excel report and JSON summary after the migration run.

    Parameters
    ----------
    migration_results : list
        Results returned by MigrationPipeline.run().

    xml_path : str
        Path to the original IDMC XML file.

    metadata_path : str
        Path to parsed IDMC metadata JSON.

    output_dir : str
        Output directory used by the migration pipeline.

    spark : SparkSession
        Active Databricks Spark session.

    dbutils : DBUtils
        Active Databricks dbutils object.

    Returns
    -------
    dict
        Excel and JSON output paths.
    """

    # =================================================================
    # 1. CONFIGURATION
    # =================================================================

    if not isinstance(migration_results, list):
        raise TypeError(
            "migration_results must be a list returned by "
            "MigrationPipeline.run()."
        )

    if not migration_results:
        raise ValueError(
            "No migration results received. Report generation cancelled."
        )

    if spark is None or dbutils is None:
        raise ValueError(
            "Both spark and dbutils must be passed to the report generator."
        )

    if not os.path.isfile(xml_path):
        raise FileNotFoundError(
            f"IDMC XML file not found: {xml_path}"
        )

    if not os.path.isfile(metadata_path):
        raise FileNotFoundError(
            f"Parsed metadata JSON not found: {metadata_path}"
        )

    os.makedirs(output_dir, exist_ok=True)

    llm_dir = os.path.join(output_dir, "llm_results")
    review_dir = os.path.join(output_dir, "human_reviews")

    local_excel_path = "/tmp/IDMC_Repository_Analysis.xlsx"
    local_json_path = "/tmp/migration_run_summary.json"

    workspace_stage_dir = (
        "/Workspace/Users/"
        + spark.sql("SELECT current_user()").first()[0]
        + "/idmc_report_staging"
    )

    workspace_excel_path = os.path.join(
        workspace_stage_dir,
        "IDMC_Repository_Analysis.xlsx"
    )

    workspace_json_path = os.path.join(
        workspace_stage_dir,
        "migration_run_summary.json"
    )

    volume_path = "dbfs:" + output_dir.rstrip("/")

    volume_excel_path = (
        volume_path + "/IDMC_Repository_Analysis.xlsx"
    )

    volume_json_path = (
        volume_path + "/migration_run_summary.json"
    )

    target_schema = "idmc_poc.migration_test"

    # =================================================================
    # 2. MAPPING CONFIGURATION
    # =================================================================

    mappings = [
        {
            "mapping_name": "m_customer_standardize",
            "merge_keys": ["customer_id"],
            "review_id": "f5f2499d-d3c9-4475-bed5-3cf7cf2085ff",
            "target_table": (
                "idmc_poc.migration_test."
                "mig4_customer_standardized"
            ),
            "source_tables": [
                "idmc_poc.migration_test.src_customer_standardize",
                "idmc_poc.migration_test.src_customer_silver"
            ]
        },
        {
            "mapping_name": "m_order_enrichment",
            "merge_keys": ["order_id"],
            "review_id": "299d9780-71e1-4845-b685-9a2f05c5994f",
            "target_table": (
                "idmc_poc.migration_test."
                "mig4_order_enriched"
            ),
            "source_tables": [
                "idmc_poc.migration_test.src_orders",
                "idmc_poc.migration_test.src_customer_silver"
            ]
        },
        {
            "mapping_name": "m_inventory_reconciliation",
            "merge_keys": ["product_id", "warehouse_id"],
            "review_id": "80f32658-8b80-4279-b844-002bb4195c9f",
            "target_table": (
                "idmc_poc.migration_test."
                "mig4_inventory_position"
            ),
            "source_tables": [
                "idmc_poc.migration_test.src_inventory",
                "idmc_poc.migration_test.src_products"
            ]
        },
        {
            "mapping_name": "m_daily_sales_summary",
            "merge_keys": ["order_date"],
            "review_id": "c72222ce-55cd-4341-8982-2574a5b9aac7",
            "target_table": (
                "idmc_poc.migration_test."
                "mig4_daily_sales_summary"
            ),
            "source_tables": [
                "idmc_poc.migration_test.src_orders",
                "idmc_poc.migration_test.src_products"
            ]
        }
    ]

    # =================================================================
    # 3. HELPER FUNCTIONS
    # =================================================================

    def read_json(path):
        if not os.path.isfile(path):
            return None

        try:
            with open(path, "r", encoding="utf-8") as file:
                return json.load(file)
        except Exception as exc:
            print(f"Could not read JSON {path}: {exc}")
            return None

    def safe_value(value):
        if value is None:
            return ""

        if isinstance(value, (dict, list, tuple)):
            return json.dumps(
                value,
                ensure_ascii=False,
                default=str
            )

        if isinstance(value, (str, int, float, bool)):
            return value

        return str(value)

    def flatten_dict(data, parent_key=""):
        rows = []

        if isinstance(data, dict):
            for key, value in data.items():
                new_key = (
                    f"{parent_key}.{key}"
                    if parent_key else str(key)
                )
                rows.extend(flatten_dict(value, new_key))

        elif isinstance(data, list):
            for index, value in enumerate(data):
                rows.extend(
                    flatten_dict(
                        value,
                        f"{parent_key}[{index}]"
                    )
                )

        else:
            rows.append([
                parent_key or "Value",
                safe_value(data)
            ])

        return rows

    def local_tag(tag):
        return tag.split("}")[-1]

    def find_key_recursive(data, keys):
        if isinstance(data, dict):
            for key in keys:
                if key in data:
                    return data[key]

            for value in data.values():
                result = find_key_recursive(value, keys)
                if result is not None:
                    return result

        elif isinstance(data, list):
            for item in data:
                result = find_key_recursive(item, keys)
                if result is not None:
                    return result

        return None

    def find_review_file(review_id):
        if not os.path.isdir(review_dir):
            return None

        for root, _, files in os.walk(review_dir):
            for filename in files:
                if not filename.lower().endswith(".json"):
                    continue

                path = os.path.join(root, filename)

                try:
                    with open(path, "r", encoding="utf-8") as file:
                        review_data = json.load(file)

                    review_text = json.dumps(
                        review_data,
                        default=str
                    )

                    if review_id in review_text:
                        return path

                except Exception:
                    continue

        return None

    def get_approval(review_id):
        path = find_review_file(review_id)

        if not path:
            return {
                "review_id": review_id,
                "status": "NOT FOUND",
                "decision": "",
                "validations_passed": False,
                "can_execute": False,
                "approved": False,
                "review_file": "",
                "details": "Review file not found"
            }

        review = read_json(path)

        if not isinstance(review, dict):
            return {
                "review_id": review_id,
                "status": "INVALID",
                "decision": "",
                "validations_passed": False,
                "can_execute": False,
                "approved": False,
                "review_file": path,
                "details": "Review JSON could not be parsed"
            }

        decision_data = review.get("decision", {})

        if isinstance(decision_data, dict):
            decision = decision_data.get("decision", "")
        else:
            decision = decision_data

        status = review.get("status", "")
        validations_passed = (
            review.get("validations_passed") is True
        )
        can_execute = review.get("can_execute") is True

        approved = (
            status == "PASS"
            and decision == "APPROVE"
            and validations_passed
            and can_execute
        )

        return {
            "review_id": review_id,
            "status": status,
            "decision": decision,
            "validations_passed": validations_passed,
            "can_execute": can_execute,
            "approved": approved,
            "review_file": path,
            "details": (
                "Approval verified"
                if approved
                else "Approval conditions not fully satisfied"
            )
        }

    # =================================================================
    # 4. LOAD METADATA AND XML
    # =================================================================

    print("=" * 80)
    print("IDMC REPOSITORY ANALYSIS")
    print("=" * 80)

    metadata = read_json(metadata_path)

    if not isinstance(metadata, dict):
        raise ValueError("Parsed metadata JSON is not a valid dictionary.")

    xml_tree = ET.parse(xml_path)
    xml_root = xml_tree.getroot()

    repository = metadata.get("repository", {})

    if not isinstance(repository, dict):
        repository = {}

    # =================================================================
    # 5. XML INVENTORY
    # =================================================================

    xml_tag_counts = Counter(
        local_tag(element.tag)
        for element in xml_root.iter()
    )

    xml_aliases = {
        "Asset Folders": ["AssetFolder", "Folder"],
        "Mappings": ["Mapping", "MappingDefinition"],
        "Connections": ["Connection", "ConnectionDefinition"],
        "Mapping Tasks": ["MappingTask", "MappingTaskInstance"],
        "Taskflows": ["Taskflow"],
        "Reusable Taskflows": [
            "ReusableTaskflow",
            "ReusableTaskflowInstance"
        ],
        "Runtime Parameter Files": [
            "ParameterFile",
            "RuntimeParameterFile"
        ],
        "Data Ingestion Jobs": [
            "DataIngestionJob",
            "IngestionJob"
        ],
        "Data Quality Rules": [
            "DataQualityRule",
            "DQRule",
            "Rule"
        ],
        "APIs": ["API", "ApiDefinition"],
        "Application Integration": [
            "ApplicationIntegrationProcess",
            "Process"
        ],
        "Metadata Relationships": [
            "MetadataRelationship",
            "Relationship"
        ],
        "AI-assisted Recommendations": [
            "AIRecommendation",
            "Recommendation"
        ]
    }

    xml_inventory = {}

    for component, aliases in xml_aliases.items():
        xml_inventory[component] = sum(
            xml_tag_counts.get(alias, 0)
            for alias in aliases
        )

    # =================================================================
    # 6. PARSED METADATA INVENTORY
    # =================================================================

    parsed_inventory = {}

    parsed_key_aliases = {
        "Asset Folders": ["folders", "asset_folders"],
        "Mappings": ["mappings", "mapping_list"],
        "Connections": ["connections", "connection_list"],
        "Mapping Tasks": ["mapping_tasks"],
        "Taskflows": ["taskflows"],
        "Reusable Taskflows": ["reusable_taskflows"],
        "Runtime Parameter Files": [
            "runtime_parameter_files",
            "parameter_files"
        ],
        "Data Ingestion Jobs": [
            "data_ingestion_jobs",
            "ingestion_jobs"
        ],
        "Data Quality Rules": [
            "data_quality_rules",
            "dq_rules"
        ],
        "APIs": ["apis"],
        "Application Integration": [
            "application_integration",
            "application_integration_processes"
        ],
        "Metadata Relationships": [
            "metadata_relationships",
            "relationships"
        ],
        "AI-assisted Recommendations": [
            "ai_recommendations",
            "recommendations"
        ]
    }

    for component, keys in parsed_key_aliases.items():
        value = find_key_recursive(metadata, keys)

        if isinstance(value, list):
            parsed_inventory[component] = len(value)

        elif isinstance(value, dict):
            parsed_inventory[component] = len(value)

        elif isinstance(value, (int, float)):
            parsed_inventory[component] = int(value)

        else:
            parsed_inventory[component] = "Not Available"

    # =================================================================
    # 7. CURRENT MIGRATION RESULTS
    # =================================================================

    actual_results = {}

    for result in migration_results:
        if isinstance(result, dict) and result.get("mapping_name"):
            actual_results[result["mapping_name"]] = dict(result)

    results = []

    for mapping in mappings:
        name = mapping["mapping_name"]

        result = actual_results.get(name)

        if result is None:
            result = {
                "mapping_name": name,
                "success": False,
                "stage": "NOT_PROCESSED",
                "dry_run": None,
                "executed": False,
                "validation_status": "NOT_RUN",
                "error_count": 0,
                "warning_count": 0,
                "error": (
                    "Mapping was not present in the current pipeline run."
                )
            }

        result.setdefault("success", False)
        result.setdefault("stage", "NOT_REPORTED")
        result.setdefault("dry_run", None)
        result.setdefault("executed", False)
        result.setdefault("validation_status", "NOT_REPORTED")
        result.setdefault("error_count", 0)
        result.setdefault("warning_count", 0)

        result["result_source"] = (
            "Current MigrationPipeline.run() output"
        )

        results.append(result)

    successful = [
        item for item in results
        if item.get("success") is True
    ]

    failed = [
        item for item in results
        if item.get("success") is not True
    ]

    executed = [
        item for item in results
        if item.get("executed") is True
    ]

    validation_passed = [
        item for item in results
        if str(
            item.get("validation_status", "")
        ).upper() in ["PASS", "PASSED", "VALID"]
    ]

    total_mappings = len(results)

    # =================================================================
    # 8. HUMAN GATEWAY APPROVALS
    # =================================================================

    approvals = []

    for mapping in mappings:
        name = mapping["mapping_name"]

        current_result = actual_results.get(name, {})

        review_id = (
            current_result.get("review_id")
            or mapping["review_id"]
        )

        approval = get_approval(review_id)
        approval["mapping_name"] = name

        approvals.append(approval)

    approved_count = sum(
        1 for item in approvals
        if item["approved"]
    )

    # =================================================================
    # 9. CONNECTION INVENTORY
    # =================================================================

    connection_data = find_key_recursive(
        repository,
        ["connections", "connection_list"]
    )

    connection_rows = []

    if isinstance(connection_data, list):
        for connection in connection_data:
            if isinstance(connection, dict):
                connection_rows.append([
                    connection.get("name", ""),
                    connection.get("type", ""),
                    connection.get("connection_type", ""),
                    connection.get("description", ""),
                    connection.get("id", "")
                ])

    if not connection_rows:
        connection_rows = [
            ["No connection records extracted", "", "", "", ""]
        ]

    # =================================================================
    # 10. TRANSFORMATION DETAILS
    # =================================================================

    transformation_rows = []

    for mapping_element in xml_root.iter():
        if local_tag(mapping_element.tag) not in [
            "Mapping",
            "MappingDefinition"
        ]:
            continue

        mapping_name = (
            mapping_element.attrib.get("name")
            or mapping_element.attrib.get("id")
            or "Unknown Mapping"
        )

        for element in mapping_element.iter():
            tag = local_tag(element.tag)

            if tag in ["Mapping", "MappingDefinition"]:
                continue

            attributes = dict(element.attrib)
            text_value = (element.text or "").strip()

            if not attributes and not text_value:
                continue

            transformation_rows.append([
                mapping_name,
                tag,
                attributes.get("name", ""),
                attributes.get("type", ""),
                attributes.get("id", ""),
                text_value[:500],
                safe_value(attributes)
            ])

    if not transformation_rows:
        transformation_rows = [
            ["No transformation details extracted", "", "", "", "", "", ""]
        ]

    # =================================================================
    # 11. LLM MIGRATION DETAILS
    # =================================================================

    llm_rows = []

    for mapping in mappings:
        name = mapping["mapping_name"]

        llm_path = os.path.join(
            llm_dir,
            name + "_llm_result.json"
        )

        llm_data = read_json(llm_path)

        if isinstance(llm_data, dict):
            llm_rows.append([
                name,
                llm_path,
                safe_value(llm_data.get("migration_plan", "")),
                safe_value(llm_data.get("transformation_logic", "")),
                safe_value(llm_data.get("assumptions_or_gaps", "")),
                "Available"
            ])
        else:
            llm_rows.append([
                name,
                llm_path,
                "",
                "",
                "LLM JSON not available",
                "Not Available"
            ])

    # =================================================================
    # 12. GENERATED ARTIFACT INVENTORY
    # =================================================================

    artifact_rows = []

    for mapping in mappings:
        name = mapping["mapping_name"]

        pyspark_path = os.path.join(
            output_dir,
            name + ".py"
        )

        llm_path = os.path.join(
            llm_dir,
            name + "_llm_result.json"
        )

        artifact_rows.append([
            name,
            pyspark_path,
            os.path.isfile(pyspark_path),
            llm_path,
            os.path.isfile(llm_path)
        ])

    # =================================================================
    # 13. CROSS-REFERENCE VALIDATION
    # =================================================================

    cross_reference = find_key_recursive(
        metadata,
        ["cross_reference_validation"]
    )

    if isinstance(cross_reference, (dict, list)):
        cross_reference_rows = flatten_dict(cross_reference)
    else:
        cross_reference_rows = [
            ["Cross-reference validation", "Not available in metadata"]
        ]

    # =================================================================
    # 14. INVENTORY COMPARISON
    # =================================================================

    inventory_rows = []

    for component in xml_aliases:
        xml_count = xml_inventory.get(component, 0)
        parsed_count = parsed_inventory.get(
            component,
            "Not Available"
        )

        if isinstance(parsed_count, int):
            difference = xml_count - parsed_count
            status = (
                "MATCH"
                if difference == 0
                else "REVIEW"
            )
        else:
            difference = "N/A"
            status = "NOT AVAILABLE"

        inventory_rows.append([
            component,
            xml_count,
            parsed_count,
            difference,
            status
        ])

    # =================================================================
    # 15. MAPPING AND EXECUTION RESULTS
    # =================================================================

    mapping_rows = []
    execution_rows = []

    for mapping in mappings:
        name = mapping["mapping_name"]

        result = next(
            item for item in results
            if item["mapping_name"] == name
        )

        approval = next(
            item for item in approvals
            if item["mapping_name"] == name
        )

        mapping_rows.append([
            name,
            result.get("success"),
            result.get("stage"),
            result.get("validation_status"),
            result.get("executed"),
            ", ".join(mapping["merge_keys"]),
            mapping["target_table"],
            "APPROVED" if approval["approved"] else "NOT APPROVED"
        ])

        execution_rows.append([
            name,
            result.get("success"),
            result.get("stage"),
            result.get("dry_run"),
            result.get("executed"),
            result.get("validation_status"),
            result.get("error_count"),
            result.get("warning_count"),
            result.get("generated_pyspark", ""),
            result.get("llm_result_path", ""),
            result.get("error", "")
        ])

    # =================================================================
    # 16. APPROVAL REPORT ROWS
    # =================================================================

    approval_rows = []

    for approval in approvals:
        approval_rows.append([
            approval["mapping_name"],
            approval["review_id"],
            approval["status"],
            approval["decision"],
            approval["validations_passed"],
            approval["can_execute"],
            approval["approved"],
            approval["review_file"],
            approval["details"]
        ])

    # =================================================================
    # 17. ISSUES, WARNINGS AND ASSUMPTIONS
    # =================================================================

    issue_rows = []

    for result in results:
        if result.get("error"):
            issue_rows.append([
                result["mapping_name"],
                "ERROR",
                result["error"]
            ])

        if result.get("warning_count", 0):
            issue_rows.append([
                result["mapping_name"],
                "WARNING",
                str(result["warning_count"])
            ])

    issue_rows.extend([
        [
            "Merge keys",
            "ASSUMPTION",
            "Configured merge keys are POC keys and are not production-certified business keys."
        ],
        [
            "Target writes",
            "LIMITATION",
            "Target writes are not independently verified by this report."
        ],
        [
            "Data validation",
            "LIMITATION",
            "Validation details are limited to the information returned by the migration pipeline."
        ],
        [
            "Inventory",
            "LIMITATION",
            "Parsed metadata counts are shown as Not Available when the corresponding data is absent."
        ]
    ])

    if not issue_rows:
        issue_rows.append([
            "Migration",
            "INFO",
            "No errors or warnings were returned by the current pipeline results."
        ])

    # =================================================================
    # 18. SOURCE AND TARGET TABLE INVENTORY
    # =================================================================

    source_target_rows = []

    for mapping in mappings:
        for source in mapping["source_tables"]:
            source_target_rows.append([
                mapping["mapping_name"],
                "SOURCE",
                source
            ])

        source_target_rows.append([
            mapping["mapping_name"],
            "TARGET",
            mapping["target_table"]
        ])

    # =================================================================
    # 19. RAW XML TAG INVENTORY
    # =================================================================

    raw_tag_rows = [
        [tag, count]
        for tag, count in sorted(xml_tag_counts.items())
    ]

    # =================================================================
    # 20. REPORT DEFINITIONS AND LIMITATIONS
    # =================================================================

    definition_rows = [
        [
            "Migration results",
            "Results are taken from the current MigrationPipeline.run() output."
        ],
        [
            "Migration execution",
            "This report generator does not execute migrations."
        ],
        [
            "Human Gateway",
            "Approval status is based on the review JSON files available in the human_reviews directory."
        ],
        [
            "Approval criteria",
            "APPROVED requires PASS status, APPROVE decision, validations_passed=True and can_execute=True."
        ],
        [
            "Merge keys",
            "Configured merge keys are POC assumptions and are not production-certified business keys."
        ],
        [
            "XML inventory",
            "Counts are calculated from XML tags using the configured aliases."
        ],
        [
            "Parsed metadata inventory",
            "Counts are extracted from the parsed metadata JSON. Missing values are reported as Not Available."
        ],
        [
            "Transformation details",
            "Transformation details are extracted from XML elements and attributes."
        ],
        [
            "LLM details",
            "LLM details are included when the corresponding JSON files exist."
        ],
        [
            "Target writes",
            "Target writes are not independently verified by this report."
        ]
    ]

    # =================================================================
    # 21. CREATE EXCEL WORKBOOK
    # =================================================================

    print("\nCreating Excel workbook...")

    workbook = Workbook()
    worksheet = workbook.active
    worksheet.title = "IDMC Analysis"

    dark_blue = "17365D"
    medium_blue = "1F4E78"
    white = "FFFFFF"
    border_color = "D9E1F2"

    thin_side = Side(
        style="thin",
        color=border_color
    )

    current_row = 1

    def add_section_title(title):
        nonlocal current_row

        worksheet.merge_cells(
            start_row=current_row,
            start_column=1,
            end_row=current_row,
            end_column=8
        )

        cell = worksheet.cell(
            row=current_row,
            column=1,
            value=title
        )

        cell.font = Font(
            bold=True,
            color=white,
            size=12
        )

        cell.fill = PatternFill(
            fill_type="solid",
            fgColor=dark_blue
        )

        cell.alignment = Alignment(
            horizontal="left",
            vertical="center"
        )

        worksheet.row_dimensions[current_row].height = 23

        current_row += 1

    def add_headers(headers):
        nonlocal current_row

        for column_number, header in enumerate(headers, start=1):
            cell = worksheet.cell(
                row=current_row,
                column=column_number,
                value=header
            )

            cell.font = Font(
                bold=True,
                color=white
            )

            cell.fill = PatternFill(
                fill_type="solid",
                fgColor=medium_blue
            )

            cell.alignment = Alignment(
                horizontal="center",
                vertical="center",
                wrap_text=True
            )

            cell.border = Border(
                bottom=thin_side
            )

        worksheet.row_dimensions[current_row].height = 30
        current_row += 1

    def add_rows(rows):
        nonlocal current_row

        for row_data in rows:
            for column_number, value in enumerate(row_data, start=1):
                cell = worksheet.cell(
                    row=current_row,
                    column=column_number,
                    value=safe_value(value)
                )

                cell.alignment = Alignment(
                    vertical="top",
                    wrap_text=True
                )

                cell.border = Border(
                    bottom=thin_side
                )

            current_row += 1

        current_row += 1

    def add_section(title, headers, rows):
        nonlocal current_row

        current_row += 1
        add_section_title(title)
        add_headers(headers)
        add_rows(rows)

    # =================================================================
    # 22. BUILD THE 14 REPORT SECTIONS
    # =================================================================

    add_section(
        "1. OVERALL MIGRATION KPI",
        ["Metric", "Value"],
        [
            ["Repository", "RetailOperationsPOC"],
            ["XML File", xml_path],
            ["Metadata File", metadata_path],
            ["Target Schema", target_schema],
            ["Total Mappings", total_mappings],
            ["Successful Mappings", len(successful)],
            ["Failed Mappings", len(failed)],
            ["Validation Passed", len(validation_passed)],
            ["Executed Mappings", len(executed)],
            ["Human Approvals Verified", approved_count],
            [
                "Report Generated UTC",
                datetime.now(timezone.utc).isoformat()
            ],
            [
                "Result Source",
                "Current MigrationPipeline.run() output"
            ],
            [
                "Target Writes Independently Verified",
                "NO"
            ]
        ]
    )

    add_section(
        "2. XML INVENTORY VS PARSED METADATA",
        [
            "Component",
            "XML Count",
            "Parsed Count",
            "Difference",
            "Status"
        ],
        inventory_rows
    )

    add_section(
        "3. MAPPING-LEVEL CONVERSION RESULTS",
        [
            "Mapping Name",
            "Success",
            "Stage",
            "Validation",
            "Executed",
            "Merge Keys",
            "Target Table",
            "Human Approval"
        ],
        mapping_rows
    )

    add_section(
        "4. DETAILED EXECUTION RESULTS",
        [
            "Mapping",
            "Success",
            "Stage",
            "Dry Run",
            "Executed",
            "Validation",
            "Errors",
            "Warnings",
            "Generated PySpark",
            "LLM Result",
            "Error Details"
        ],
        execution_rows
    )

    add_section(
        "5. TRANSFORMATION DETAILS",
        [
            "Mapping",
            "XML Element",
            "Name",
            "Type",
            "ID",
            "Text",
            "Attributes"
        ],
        transformation_rows
    )

    add_section(
        "6. LLM MIGRATION DETAILS",
        [
            "Mapping",
            "LLM JSON Path",
            "Migration Plan",
            "Transformation Logic",
            "Assumptions / Gaps",
            "Availability"
        ],
        llm_rows
    )

    add_section(
        "7. GENERATED ARTIFACT INVENTORY",
        [
            "Mapping",
            "PySpark Artifact",
            "PySpark Exists",
            "LLM JSON",
            "LLM JSON Exists"
        ],
        artifact_rows
    )

    add_section(
        "8. HUMAN GATEWAY APPROVALS",
        [
            "Mapping",
            "Review ID",
            "Status",
            "Decision",
            "Validations Passed",
            "Can Execute",
            "Approved",
            "Review JSON",
            "Details"
        ],
        approval_rows
    )

    add_section(
        "9. CONNECTION INVENTORY",
        [
            "Connection Name",
            "Type",
            "Connection Type",
            "Description",
            "Connection ID"
        ],
        connection_rows
    )

    add_section(
        "10. CROSS-REFERENCE VALIDATION",
        [
            "Validation Metric",
            "Value"
        ],
        cross_reference_rows
    )

    add_section(
        "11. MIGRATION ISSUES, WARNINGS AND ASSUMPTIONS",
        [
            "Area",
            "Type",
            "Details"
        ],
        issue_rows
    )

    add_section(
        "12. SOURCE AND TARGET TABLE INVENTORY",
        [
            "Mapping",
            "Table Type",
            "Table Name"
        ],
        source_target_rows
    )

    add_section(
        "13. RAW XML TAG INVENTORY",
        [
            "XML Tag",
            "Count"
        ],
        raw_tag_rows
    )

    add_section(
        "14. REPORT DEFINITIONS AND LIMITATIONS",
        [
            "Area",
            "Description"
        ],
        definition_rows
    )

    # =================================================================
    # 23. WORKBOOK FORMATTING
    # =================================================================

    column_widths = {
        "A": 32,
        "B": 35,
        "C": 35,
        "D": 35,
        "E": 30,
        "F": 35,
        "G": 35,
        "H": 35,
        "I": 35,
        "J": 35,
        "K": 35
    }

    for column, width in column_widths.items():
        worksheet.column_dimensions[column].width = width

    worksheet.freeze_panes = "A2"
    worksheet.sheet_view.showGridLines = False

    worksheet.page_setup.orientation = "landscape"
    worksheet.page_setup.fitToWidth = 1
    worksheet.page_setup.fitToHeight = 0

    worksheet.sheet_properties.pageSetUpPr.fitToPage = True

    # =================================================================
    # 24. SAVE EXCEL LOCALLY
    # =================================================================

    workbook.save(local_excel_path)

    if not os.path.isfile(local_excel_path):
        raise RuntimeError("Excel workbook was not created.")

    if os.path.getsize(local_excel_path) == 0:
        raise RuntimeError("Excel workbook is empty.")

    print(
        "Excel workbook created:",
        local_excel_path
    )

    # =================================================================
    # 25. CREATE JSON SUMMARY
    # =================================================================

    migration_summary = {
        "repository": "RetailOperationsPOC",
        "xml_path": xml_path,
        "metadata_path": metadata_path,
        "target_schema": target_schema,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "migration_results_source": (
            "Current MigrationPipeline.run() output"
        ),
        "total_mappings": total_mappings,
        "successful_mappings": len(successful),
        "failed_mappings": len(failed),
        "validation_passed": len(validation_passed),
        "executed_mappings": len(executed),
        "human_approvals_verified": approved_count,
        "target_writes_independently_verified": False,
        "mapping_results": results,
        "human_gateway_approvals": approvals,
        "xml_inventory": xml_inventory,
        "parsed_inventory": parsed_inventory,
        "cross_reference_validation": cross_reference,
        "generated_artifacts": artifact_rows
    }

    with open(
        local_json_path,
        "w",
        encoding="utf-8"
    ) as file:
        json.dump(
            migration_summary,
            file,
            indent=4,
            ensure_ascii=False,
            default=str
        )

    # =================================================================
    # 26. STAGE FILES IN WORKSPACE
    # =================================================================

    os.makedirs(
        workspace_stage_dir,
        exist_ok=True
    )

    shutil.copy2(
        local_excel_path,
        workspace_excel_path
    )

    shutil.copy2(
        local_json_path,
        workspace_json_path
    )

    if not os.path.isfile(workspace_excel_path):
        raise RuntimeError(
            "Excel workbook was not staged in Workspace."
        )

    if not os.path.isfile(workspace_json_path):
        raise RuntimeError(
            "JSON summary was not staged in Workspace."
        )

    # =================================================================
    # 27. COPY FILES TO UNITY CATALOG VOLUME
    # =================================================================

    print("\nCopying report files to Unity Catalog Volume...")

    dbutils.fs.mkdirs(volume_path)

    dbutils.fs.cp(
        "file:" + workspace_excel_path,
        volume_excel_path,
        True
    )

    dbutils.fs.cp(
        "file:" + workspace_json_path,
        volume_json_path,
        True
    )

    # =================================================================
    # 28. VERIFY OUTPUT FILES
    # =================================================================

    print("\nVerifying output files...")

    destination_files = dbutils.fs.ls(volume_path)

    found_files = {
        item.name.rstrip("/")
        for item in destination_files
    }

    expected_files = {
        "IDMC_Repository_Analysis.xlsx",
        "migration_run_summary.json"
    }

    missing_files = expected_files - found_files

    if missing_files:
        raise RuntimeError(
            "Report files missing from output Volume: "
            + str(missing_files)
        )

    for item in destination_files:
        if item.name.rstrip("/") in expected_files:
            print(
                item.name,
                "| VERIFIED",
                "| Size:",
                item.size,
                "bytes"
            )

    # =================================================================
    # 29. FINAL SUMMARY
    # =================================================================

    print("\n" + "=" * 80)
    print("IDMC REPOSITORY ANALYSIS COMPLETED")
    print("=" * 80)

    print("Total mappings:", total_mappings)
    print("Successful mappings:", len(successful))
    print("Failed mappings:", len(failed))
    print("Validation passed:", len(validation_passed))
    print("Executed mappings:", len(executed))
    print("Human approvals verified:", approved_count)

    print("\nExcel report:")
    print(volume_excel_path)

    print("\nJSON summary:")
    print(volume_json_path)

    print("\nTarget writes independently verified: NO")

    print("=" * 80)

    # =================================================================
    # 30. RETURN OUTPUT PATHS
    # =================================================================

    return {
        "excel_path": volume_excel_path,
        "summary_path": volume_json_path
    }