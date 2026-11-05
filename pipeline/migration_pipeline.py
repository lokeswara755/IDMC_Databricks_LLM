from parser.xmlreader import XMLReader

from parser.repository_parser import RepositoryParser

from parser.graph_builder import GraphBuilder
from parser.MappingTaskParser import MappingTaskParser
from parser.Taskflow_parser import TaskflowParser
from parser.RuntimeParameterParser import RuntimeParameterParser
from parser.DataQualityRuleParser import DataQualityRuleParser
from parser.DataIngestionJobParser import DataIngestionJobParser
from parser.ApplicationIntegrationProcessParser import ApplicationIntegrationProcessParser
from parser.APIParser import APIParser
from parser.AssetFolderParser import AssetFolderParser
from parser.AIRecommendationParser import AIRecommendationParser
from parser.RepositoryMetadataParser import RepositoryMetadataParser
from parser.MetadataRelationshipParser import MetadataRelationshipParser
from parser.CrossReferenceValidator import CrossReferenceValidator

from LLM.Input_builder import LLMInputBuilder

from LLM.LLMGenerator import LLMGenerator

from LLM.LLM_PySpark_Validator import LLMPySparkValidator

from Data_Validation.DataValidator import DataValidator

from Human_GateWay.human_gateway import HumanGateway, PASS, REVIEW_REQUIRED, REWORK

import os

import re

import json
import traceback
import inspect


from datetime import date, datetime
from decimal import Decimal
from dataclasses import asdict, is_dataclass


def make_json_serializable(obj):
    """Convert parsed IDMC metadata into values supported by json.dump."""
    if obj is None or isinstance(obj, (str, int, float, bool)):
        return obj
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if isinstance(obj, Decimal):
        return str(obj)
    if is_dataclass(obj):
        return make_json_serializable(asdict(obj))
    if isinstance(obj, dict):
        return {
            str(key): make_json_serializable(value)
            for key, value in obj.items()
        }
    if isinstance(obj, (list, tuple, set)):
        values = list(obj)
        if isinstance(obj, set):
            values = sorted(values, key=repr)
        return [make_json_serializable(value) for value in values]
    if hasattr(obj, "to_dict") and callable(obj.to_dict):
        return make_json_serializable(obj.to_dict())
    if hasattr(obj, "__dict__"):
        return make_json_serializable(vars(obj))
    return str(obj)



class MigrationPipeline:

    def __init__(

        self,

        xml_path,

        spark,

        output_dir="/Volumes/idmc_poc/bronze/idmc_files/generated_pyspark",

        workspace_url=None,

        databricks_token=None,

        llm_model="databricks-gpt-oss-120b",

        selected_mapping_name=None,

        csv_defaults=None,

        source_overrides=None,
        dry_run=True,
        execution_approved=False,
        allow_synthetic_merge_keys=False,
        verbose=False,
        human_review_dir=None,
        approved_review_id=None,
        approved_review_ids=None,
    ):

        self.xml_path = xml_path

        self.spark = spark

        self.output_dir = output_dir
        self.selected_mapping_name = selected_mapping_name
        self.csv_defaults = dict(csv_defaults or {"header": "true", "inferSchema": "true"})
        # Explicit source overrides only. Keys should match a source logical_name,
        # name, or current physical_table; values must be approved table identifiers.
        # Example: {"some_logical_source": "idmc_poc.bronze.some_table"}
        self.source_overrides = dict(source_overrides or {})
        # Safe by default: generation/validation only. Execution requires explicit approval.
        self.dry_run = bool(dry_run)
        self.execution_approved = bool(execution_approved)
        self.allow_synthetic_merge_keys = bool(allow_synthetic_merge_keys)
        self.verbose = bool(verbose)
        self.human_review_dir = human_review_dir or os.path.join(
            self.output_dir, "human_reviews"
        )
        self.human_gateway = HumanGateway(review_dir=self.human_review_dir)
        # One review ID per mapping enables repository-wide processing. The
        # singular parameter remains supported for backward compatibility.
        self.approved_review_id = approved_review_id
        self.approved_review_ids = dict(approved_review_ids or {})

        if not self.dry_run and not self.execution_approved:
            raise ValueError(
                "Execution blocked: set execution_approved=True only after reviewing "
                "the generated code and approving target writes."
            )

        if not workspace_url:

            raise ValueError(

                "workspace_url is required."

            )

        if not databricks_token:

            raise ValueError(

                "databricks_token is required."

            )

        if not llm_model:

            raise ValueError(

                "llm_model is required."

            )

        self.xml_reader = XMLReader(

            xml_path

        )

        self.repository_parser = RepositoryParser()

        self.metadata_bundle = {}

        self.graph_builder = GraphBuilder()

        self.llm_input_builder = LLMInputBuilder(
            csv_defaults=self.csv_defaults,
            metadata_xml_path=self.xml_path,
        )

        self.llm_generator = LLMGenerator(

            workspace_url=workspace_url,

            databricks_token=databricks_token,

            model=llm_model

        )

    def _human_review_result(
        self, mapping_name, category, message, *, validations_passed=True,
        evidence=None, scope="MAPPING", live_execution=None
    ):
        """Create a persisted Human Gateway item and return a pipeline result."""
        review = self.human_gateway.create_review_item(
            mapping_name=mapping_name,
            category=category,
            message=str(message),
            severity="BLOCKER",
            evidence=evidence,
            validations_passed=validations_passed,
            live_execution=(not self.dry_run if live_execution is None else live_execution),
            scope=scope,
        )
        return {
            "success": False,
            "mapping_name": mapping_name,
            "stage": "HUMAN_REVIEW" if review["status"] == REVIEW_REQUIRED else "HUMAN_REWORK",
            "human_review": review,
            "review_id": review["review_id"],
            "error": str(message),
            "dry_run": self.dry_run,
            "executed": False,
        }

    def record_human_decision(self, review_id, decision, reviewer, comment=""):
        """Record a reviewer decision against a persisted review ID."""
        return self.human_gateway.record_decision_by_id(
            review_id=review_id, decision=decision, reviewer=reviewer, comment=comment
        )

    @staticmethod
    def _map100_expected_approval_evidence():
        """Return the exact, narrowly scoped MAP100 merge-key approval payload."""
        return {
            "approval_type": "POC_MERGE_KEY_APPROVAL",
            "mapping_name": "m_customer_360_enrichment",
            "merge_keys": ["customer_id"],
            "targets": [
                "idmc_poc.curated.customer_gold",
                "idmc_poc.curated.customer_standard",
                "idmc_poc.curated.customer_low_value",
            ],
            "approval_scope": "MAP100",
            "provenance": "HUMAN_APPROVED_POC_ASSUMPTION",
            "xml_authoritative": False,
            "execution_approved": False,
        }

    def _approved_review_matches(
        self, mapping_name, category, expected_evidence=None, scope=None
    ):
        """Validate a persisted approval, including its exact evidence and scope.

        A review category or mapping-name match alone is never sufficient.
        Technical validation failures remain non-overridable by HumanGateway.
        """
        review_id = (
            self.approved_review_ids.get(mapping_name)
            or self.approved_review_id
        )
        if not review_id:
            return False

        try:
            review = self.human_gateway.get_review(review_id)
        except Exception:
            return False

        if review.get("mapping_name") != mapping_name:
            return False
        if scope is not None and review.get("scope") != scope:
            return False

        decision = review.get("decision") or {}
        if not isinstance(decision, dict):
            return False
        if str(decision.get("decision", "")).upper() != "APPROVE":
            return False

        if review.get("status") != PASS:
            return False
        if review.get("validations_passed") is not True:
            return False

        resolved_categories = {
            str(value).upper()
            for value in (review.get("resolved_categories") or [])
        }
        if category.upper() not in resolved_categories:
            return False

        if (
            isinstance(expected_evidence, dict)
            and expected_evidence.get("approval_type") == "MAPPING_MERGE_KEY_APPROVAL"
            and any(
                not item.get("merge_keys")
                for item in (expected_evidence.get("targets") or [])
                if isinstance(item, dict)
            )
        ):
            # A reviewer cannot approve an unspecified key. The mapping metadata
            # must first be corrected with an explicit candidate key.
            return False

        if expected_evidence is not None:
            evidence_matches = False
            for issue in (review.get("resolved_issues") or []):
                if not isinstance(issue, dict):
                    continue
                if str(issue.get("category", "")).upper() != category.upper():
                    continue
                evidence = issue.get("evidence") or {}
                if isinstance(evidence, dict) and evidence == expected_evidence:
                    evidence_matches = True
                    break
            if not evidence_matches:
                return False

        return bool(
            self.human_gateway.can_proceed(
                review, execution_approved=self.execution_approved
            )
        )

    def _map100_approval_required_result(self):
        """Create a reviewable MAP100 approval request without running the LLM."""
        evidence = self._map100_expected_approval_evidence()
        message = (
            "Approve customer_id as the POC business merge key for exactly "
            "customer_gold, customer_standard and customer_low_value. "
            "This is not authoritative IDMC XML metadata and does not approve execution."
        )
        review = self.human_gateway.create_review_item(
            mapping_name="m_customer_360_enrichment",
            category="UNVERIFIED_MERGE_KEY",
            message=message,
            severity="WARNING",
            evidence=evidence,
            validations_passed=True,
            live_execution=False,
            scope="MAP100",
        )
        return {
            "success": False,
            "mapping_name": "m_customer_360_enrichment",
            "stage": "HUMAN_REVIEW",
            "human_review": review,
            "review_id": review["review_id"],
            "error": message,
            "dry_run": self.dry_run,
            "executed": False,
        }


    @staticmethod
    def _generic_merge_key_approval_evidence(mapping_name, llm_input):
        """Build deterministic approval evidence for unverified keys in any mapping.

        This records only keys already present in parsed metadata. It never
        invents a key for a target whose merge key is missing.
        """
        approved_targets = []
        for target in (llm_input.get("targets", []) or []):
            if not isinstance(target, dict):
                continue
            attributes = target.get("attributes") or {}
            if not isinstance(attributes, dict):
                attributes = {}
            status = (
                target.get("merge_key_status")
                or attributes.get("merge_key_status")
                or target.get("merge_key_provenance")
                or attributes.get("merge_key_provenance")
                or ""
            )
            status_text = str(status).upper()
            authoritative = target.get("merge_key_authoritative_from_xml")
            keys = target.get("merge_keys") or attributes.get("merge_keys") or []
            if isinstance(keys, str):
                keys = [keys]
            keys = sorted({str(k).strip() for k in keys if str(k).strip()})
            # Only gate keys explicitly marked as synthetic/unverified/non-XML.
            is_unverified = (
                status_text in {
                    "SYNTHETIC_POC",
                    "APPROVED_POC_ASSUMPTION",
                    "UNVERIFIED",
                    "UNVERIFIED_MERGE_KEY",
                    "HUMAN_APPROVED_POC_ASSUMPTION",
                }
                or authoritative is False
            )
            if not is_unverified:
                continue
            target_name = (
                target.get("physical_table")
                or target.get("target")
                or target.get("logical_name")
                or target.get("name")
                or target.get("target_name")
            )
            if not target_name:
                continue
            approved_targets.append({
                "target": str(target_name),
                "merge_keys": keys,
                "source_status": status_text or "NOT_XML_AUTHORITATIVE",
            })

        if not approved_targets:
            return None

        approved_targets.sort(key=lambda item: item["target"].casefold())
        return {
            "approval_type": "MAPPING_MERGE_KEY_APPROVAL",
            "mapping_name": str(mapping_name),
            "targets": approved_targets,
            "provenance": "HUMAN_APPROVED_POC_ASSUMPTION",
            "xml_authoritative": False,
            "execution_approved": False,
        }

    def _generic_merge_key_approval_required_result(self, mapping_name, evidence):
        """Persist a generic merge-key review request for the current mapping."""
        empty_keys = [
            item["target"] for item in evidence.get("targets", [])
            if not item.get("merge_keys")
        ]
        if empty_keys:
            message = (
                "Merge keys are not declared for these targets: "
                + ", ".join(empty_keys)
                + ". Add the intended business key(s) to the mapping metadata "
                  "before requesting approval; Human Gateway must not invent keys."
            )
        else:
            message = (
                "Review and approve the existing candidate merge keys for mapping "
                f"{mapping_name}: "
                + "; ".join(
                    f'{item["target"]} => {item["merge_keys"]}'
                    for item in evidence.get("targets", [])
                )
                + ". Approval applies only to these exact targets and keys; "
                  "it does not authorize execution."
            )
        review = self.human_gateway.create_review_item(
            mapping_name=mapping_name,
            category="UNVERIFIED_MERGE_KEY",
            message=message,
            severity="WARNING",
            evidence=evidence,
            validations_passed=True,
            live_execution=False,
            scope="MAPPING",
        )
        return {
            "success": False,
            "mapping_name": mapping_name,
            "stage": "HUMAN_REVIEW",
            "human_review": review,
            "review_id": review["review_id"],
            "error": message,
            "dry_run": self.dry_run,
            "executed": False,
        }

    @staticmethod
    def _apply_generic_merge_key_approval(llm_input, evidence):
        """Annotate only the exact target/key pairs present in approved evidence."""
        evidence_targets = {
            (item["target"].casefold(), tuple(item.get("merge_keys") or []))
            for item in evidence.get("targets", [])
        }
        for target in (llm_input.get("targets", []) or []):
            if not isinstance(target, dict):
                continue
            target_name = (
                target.get("physical_table")
                or target.get("target")
                or target.get("logical_name")
                or target.get("name")
                or target.get("target_name")
            )
            if not target_name:
                continue
            attributes = target.get("attributes") or {}
            keys = target.get("merge_keys") or (
                attributes.get("merge_keys") if isinstance(attributes, dict) else []
            ) or []
            if isinstance(keys, str):
                keys = [keys]
            key_tuple = tuple(sorted({str(k).strip() for k in keys if str(k).strip()}))
            if (str(target_name).casefold(), key_tuple) not in evidence_targets:
                continue
            target["merge_key_status"] = "HUMAN_APPROVED_POC_ASSUMPTION"
            target["merge_key_provenance"] = (
                "HUMAN_APPROVED_POC_ASSUMPTION; not authoritative IDMC XML metadata."
            )
            target["merge_key_authoritative_from_xml"] = False
        llm_input["human_approved_merge_key_evidence"] = evidence

    def parse_specialized_metadata(self, root, repository):
        """Parse additional IDMC assets and validate cross-references."""
        print("\nParsing specialized IDMC metadata...")

        metadata_bundle = {
            "repository": repository,
            "mapping_tasks": MappingTaskParser().parse(root),
            "taskflows": TaskflowParser().parse(root),
            "runtime_parameters": RuntimeParameterParser().parse(root),
            "data_quality_rules": DataQualityRuleParser().parse(root),
            "data_ingestion_jobs": DataIngestionJobParser().parse(root),
            "application_integration_processes": ApplicationIntegrationProcessParser().parse(root),
            "apis": APIParser().parse(root),
            "asset_folders": AssetFolderParser().parse(root),
            "ai_recommendations": AIRecommendationParser().parse(root),
            "repository_metadata": RepositoryMetadataParser().parse(root),
            "metadata_relationships": MetadataRelationshipParser().parse(root),
        }

        print("\nValidating cross-references...")
        validation_result = CrossReferenceValidator().validate(root)
        metadata_bundle["cross_reference_validation"] = validation_result

        for asset_name, result in metadata_bundle.items():
            if asset_name == "repository":
                continue
            if isinstance(result, dict) and "summary" in result:
                print(f"{asset_name}: {result['summary']}")

        print("\nCross-reference validation summary:")
        summary = (
            validation_result.get("summary", validation_result)
            if isinstance(validation_result, dict)
            else validation_result
        )
        print(json.dumps(summary, indent=2, ensure_ascii=False, default=str))

        self.metadata_bundle = metadata_bundle
        return metadata_bundle

    # ============================================================

    # MAIN PIPELINE

    # ============================================================

    def run(self):

        """

        Run the complete LLM migration pipeline.

        """

        print("\n" + "=" * 60)

        print("IDMC TO DATABRICKS - LLM MIGRATION")

        print("=" * 60)
        print(f"Execution mode: {'DRY RUN (no generated PySpark execution)' if self.dry_run else 'LIVE EXECUTION (generated PySpark may write to targets)'}")

        try:

            # ----------------------------------------------------

            # 1. READ XML

            # ----------------------------------------------------

            print("\n[1/5] Reading IDMC XML...")

            try:
                root = self.xml_reader.read()
            except Exception as xml_error:
                review_result = self._human_review_result(
                    mapping_name="XML_IMPORT",
                    category="CORRUPTED_XML",
                    message=f"Unable to read or parse the IDMC XML: {xml_error}",
                    validations_passed=False,
                    evidence={"xml_path": str(self.xml_path), "exception_type": type(xml_error).__name__},
                    scope="REPOSITORY",
                    live_execution=False,
                )
                print("XML parsing failed; Human Gateway review record created:", review_result["review_id"])
                raise RuntimeError(
                    f"IDMC XML could not be parsed. Human review: {review_result['review_id']}"
                ) from xml_error

            print(

                f"      XML loaded successfully "

                f"(root={root.tag})"

            )

            # ----------------------------------------------------

            # 2. PARSE REPOSITORY

            # ----------------------------------------------------

            print("\n[2/5] Parsing IDMC repository...")

            repository = self.repository_parser.parse(

                root

            )

            folders = repository.get(

                "folders",

                []

            )

            print(

                f"      Repository : "

                f"{repository.get('name')}"

            )

            print(

                f"      Folders    : "

                f"{len(folders)}"

            )

            # ----------------------------------------------------
            # 2.1 PARSE SPECIALIZED METADATA
            # ----------------------------------------------------

            print("\n[2.1/5] Parsing specialized metadata...")
            metadata_bundle = self.parse_specialized_metadata(
                root=root,
                repository=repository
            )
            print("Specialized metadata parsing completed.")

            # ----------------------------------------------------
            # 2.2 SAVE AND PRINT PARSED IDMC METADATA (BEFORE LLM)
            # ----------------------------------------------------
            print("\n[2.2/5] Saving parsed IDMC metadata...")

            parsed_objects = make_json_serializable(metadata_bundle)
            parsed_dir = "/Volumes/idmc_poc/bronze/idmc_files/parsed_metadata"
            os.makedirs(parsed_dir, exist_ok=True)

            parsed_path = os.path.join(
                parsed_dir,
                "idmc_parsed_objects.json"
            )

            with open(parsed_path, "w", encoding="utf-8") as parsed_file:
                json.dump(
                    parsed_objects,
                    parsed_file,
                    indent=2,
                    ensure_ascii=False
                )

            print(f"Saved parsed metadata: {parsed_path}")
            print("\nParsed object summary:")
            for object_name, object_value in parsed_objects.items():
                if isinstance(object_value, (dict, list)):
                    print(f"  {object_name}: {len(object_value)} items/keys")
                else:
                    print(f"  {object_name}: {type(object_value).__name__}")

            print("\nParsed metadata preview (first 12,000 characters):")
            print(
                json.dumps(parsed_objects, indent=2, ensure_ascii=False)[:12000]
            )


            # ----------------------------------------------------

            # 3. PROCESS MAPPINGS

            # ----------------------------------------------------

            print("\n[3/5] Processing mappings...")

            migration_results = []
            selected_mapping_name = self.selected_mapping_name
            selected_mapping_found = False
            processed_mapping_count = 0

            if selected_mapping_name:
                print(f"Running only selected mapping: {selected_mapping_name}")
            else:
                print("No mapping filter configured; processing all mappings found in the repository.")

            for folder in folders:

                folder_name = folder.get(

                    "name",

                    "Unknown"

                )

                mappings = folder.get(

                    "mappings",

                    []

                )

                print(

                    f"\n      Folder: {folder_name}"

                )

                for mapping in mappings:
                    mapping_name = (
                        mapping.get("name")
                        or mapping.get("mapping_name")
                    )

                    # If a specific mapping was requested, process only that one.
                    # If selected_mapping_name is None/empty, process every mapping.
                    if selected_mapping_name and mapping_name != selected_mapping_name:
                        print(f"Skipping mapping (not selected): {mapping_name}")
                        continue

                    selected_mapping_found = True
                    processed_mapping_count += 1
                    if selected_mapping_name:
                        print(f"Running selected mapping: {mapping_name}")
                    else:
                        print(f"Running mapping: {mapping_name}")

                    result = self.process_mapping(mapping)
                    migration_results.append(result)

            if selected_mapping_name and not selected_mapping_found:
                raise ValueError(
                    f"Selected mapping '{selected_mapping_name}' was not found "
                    "in the parsed repository. Check the mapping name returned "
                    "by RepositoryParser."
                )

            if not selected_mapping_name and processed_mapping_count == 0:
                raise ValueError(
                    "No mappings were found in the parsed repository. "
                    "Check RepositoryParser output and the input XML."
                )

            # ----------------------------------------------------

            # 4. FINAL SUMMARY

            # ----------------------------------------------------

            print(

                "\n[4/5] Migration summary..."

            )

            self.print_final_summary(

                migration_results

            )

            # ----------------------------------------------------

            # 5. COMPLETE

            # ----------------------------------------------------

            print(

                "\n[5/5] Migration completed."

            )

            # Create an Excel summary from the actual per-mapping pipeline results.
            report_paths = self.write_migration_report(migration_results)
            self.last_report_paths = report_paths
            print("Migration report files:", report_paths)

            # Preserve the original public return contract: a list of compact rows.
            return [self._compact_result(result) for result in migration_results]

        except Exception as error:

            print(

                "\nMigration pipeline failed:"

            )

            print(

                f"Error: {error}"

            )
            print("\nFull traceback:")
            traceback.print_exc()

            raise

    # ============================================================

    # PROCESS MAPPING

    # ============================================================

    def process_mapping(

        self,

        mapping

    ):

        """

        Process one IDMC mapping through the complete

        LLM migration flow.

        """

        mapping_name = mapping.get(

            "name",

            "unknown_mapping"

        )

        print("\n" + "-" * 60)

        print(

            f"Mapping: {mapping_name}"

        )

        print("-" * 60)

        # --------------------------------------------------------

        # 1. BUILD GRAPH

        # --------------------------------------------------------

        try:

            graph = self.graph_builder.build(

                mapping

            )

            print(

                f"Graph built "

                f"({len(graph.get('nodes', []))} nodes, "

                f"{len(graph.get('edges', []))} edges)"

            )

        except Exception as error:

            print(

                "Graph build: FAILED"

            )

            print(

                f"Error: {error}"

            )
            error_traceback = traceback.format_exc()
            print("\nFull traceback:")
            print(error_traceback)

            return self._human_review_result(
                mapping_name, "GRAPH_BUILD_FAILED", str(error),
                validations_passed=False, evidence={"traceback": error_traceback}
            )

        # --------------------------------------------------------

        # 2. BUILD LLM INPUT

        # --------------------------------------------------------

        try:
            if self.verbose:
                print(f"Building LLM input for {mapping_name} from {self.xml_path}")

            llm_input = self.llm_input_builder.build(
                mapping=mapping,
                graph=graph,
                xml_path=self.xml_path,
                specialized_metadata=self.metadata_bundle,
            )

            # MAP100 has a separately approved, mapping-specific assumption.
            # Keep that approval exact; do not reuse it for any other mapping.
            if mapping_name == "m_customer_360_enrichment":
                expected_evidence = self._map100_expected_approval_evidence()
                if not self._approved_review_matches(
                    mapping_name=mapping_name,
                    category="UNVERIFIED_MERGE_KEY",
                    expected_evidence=expected_evidence,
                    scope="MAP100",
                ):
                    return self._map100_approval_required_result()

            # General Human Gateway merge-key review for every other mapping.
            # It only approves candidate keys already present in metadata.
            else:
                generic_evidence = self._generic_merge_key_approval_evidence(
                    mapping_name, llm_input
                )
                if generic_evidence is not None:
                    if not self._approved_review_matches(
                        mapping_name=mapping_name,
                        category="UNVERIFIED_MERGE_KEY",
                        expected_evidence=generic_evidence,
                        scope="MAPPING",
                    ):
                        return self._generic_merge_key_approval_required_result(
                            mapping_name, generic_evidence
                        )
                    self._apply_generic_merge_key_approval(
                        llm_input, generic_evidence
                    )

            # Apply only explicit POC overrides. Mapping-specific assumptions
            # remain scoped and are never generalized to unrelated mappings.
            self._apply_approved_poc_overrides(llm_input, mapping_name)

            if self.verbose:
                print("LLM input built.")

            # Validate/resolve source tables BEFORE calling the LLM. This prevents
            # predictable TABLE_OR_VIEW_NOT_FOUND and missing-column failures.
            source_preflight = self._preflight_sources(llm_input)
            print(
                f"Source preflight: {'PASS' if source_preflight.get('ready') else 'BLOCKED'} "
                f"({len(source_preflight.get('checked_sources', []))} source(s), "
                f"{len(source_preflight.get('blocking_issues', []))} blocker(s))"
            )
            if self.verbose and source_preflight.get("blocking_issues"):
                for issue in source_preflight["blocking_issues"]:
                    print(f"  - {issue}")

            if not source_preflight.get("ready", False):
                review_result = self._human_review_result(
                    mapping_name, "SOURCE_PREFLIGHT_FAILED",
                    "Required source resolution/schema checks failed; LLM generation was skipped.",
                    validations_passed=False,
                    evidence={"blocking_issues": source_preflight.get("blocking_issues", [])},
                )
                review_result["source_preflight"] = source_preflight
                review_result["blocking_issues"] = source_preflight.get("blocking_issues", [])
                return review_result

            if self.verbose:
                print("Enriching target metadata from the catalog...")
            self._enrich_target_metadata_from_catalog(llm_input)

            if self.verbose:
                print("Target metadata enrichment complete.")
            self.print_llm_input_summary(
                mapping_name,
                llm_input,
            )

            # ----------------------------------------------------

            # DEBUG: PRINT COMPLETE TARGET METADATA

            # ----------------------------------------------------
            if self.verbose:
                print("Target metadata:")
                print(json.dumps(llm_input.get("targets", []), indent=2, ensure_ascii=False, default=str))

        except Exception as error:
            print("[ERROR] LLM input stage failed:", repr(error))
            traceback.print_exc()

            return {
                "success": False,
                "mapping_name": mapping_name,
                "stage": "LLM_INPUT_BUILD",
                "error": repr(error),
                "traceback": traceback.format_exc(),
            }

        # --------------------------------------------------------

        # 3. GENERATE PYSPARK WITH LLM

        # --------------------------------------------------------

        try:

            print(

                "\nLLM generation: RUNNING..."

            )

            # Let the LLM repair validation failures before the pipeline saves
            # the final generated PySpark. The validator is never bypassed.
            llm_result = self.llm_generator.generate(
                llm_input,
                validator_factory=lambda code, metadata: LLMPySparkValidator(
                    generated_code=code,
                    llm_input=metadata,
                    spark=self.spark,
                ),
                max_repair_attempts=3,
            )

            if llm_result.get("generation_status") != "VALIDATED":
                print("LLM generation: NOT VALIDATED; final execution gate will remain blocked.")
                print("Generation status:", llm_result.get("generation_status"))
                print("Final generator validation:", llm_result.get("validation", {}))
                print("Validation history:", llm_result.get("validation_history", []))
            else:
                print("LLM generation: repair-loop validation passed; independent final validation is still required.")

        except Exception as error:

            print(

                "LLM generation: FAILED"

            )

            print(

                f"Error: {error}"

            )
            error_traceback = traceback.format_exc()
            print("\nFull traceback:")
            print(error_traceback)

            return self._human_review_result(
                mapping_name, "LLM_GENERATION_FAILED", str(error),
                validations_passed=False, evidence={"traceback": error_traceback}
            )

        # --------------------------------------------------------

        # 4. SAVE LLM RESULT

        # --------------------------------------------------------

        try:

            llm_result_path = (

                self.save_llm_result(

                    mapping_name,

                    llm_result

                )

            )

            print(

                f"LLM result saved: "

                f"{llm_result_path}"

            )

        except Exception as error:

            print(

                "LLM result save: FAILED"

            )

            print(

                f"Error: {error}"

            )

            return {

                "success": False,

                "mapping_name": mapping_name,

                "stage": "SAVE_LLM_RESULT",

                "error": str(error)

            }

        # --------------------------------------------------------

        # 5. EXTRACT GENERATED PYSPARK

        # --------------------------------------------------------

        generated_code = llm_result.get(

            "pyspark_code",

            ""

        )

        if not generated_code.strip():

            error_message = (

                "LLM generated empty PySpark code."

            )

            print(

                f"PySpark generation: FAILED - "

                f"{error_message}"

            )

            return {

                "success": False,

                "mapping_name": mapping_name,

                "stage": "PYSPARK_GENERATION",

                "llm_result": llm_result,

                "llm_result_path": llm_result_path,

                "error": error_message

            }

        print(
            "PySpark artifact generated; validation is still required before it can be considered ready."
        )

        # --------------------------------------------------------

        # 6. SAVE GENERATED PYSPARK

        # --------------------------------------------------------

        try:

            pyspark_path = (

                self.save_generated_pyspark(

                    mapping_name,

                    generated_code

                )

            )

            print(

                f"PySpark saved: "

                f"{pyspark_path}"

            )

        except Exception as error:

            print(

                "PySpark save: FAILED"

            )

            print(

                f"Error: {error}"

            )

            return {

                "success": False,

                "mapping_name": mapping_name,

                "stage": "SAVE_PYSPARK",

                "llm_result": llm_result,

                "llm_result_path": llm_result_path,

                "error": str(error)

            }

        # --------------------------------------------------------

        # 7. VALIDATE GENERATED PYSPARK

        # --------------------------------------------------------

        validation_result = (

            self.validate_llm_pyspark(

                mapping_name=mapping_name,

                llm_input=llm_input,

                generated_code=generated_code,

                spark=self.spark

            )

        )

        # Fail closed across both gates. The generator's repair-loop validation
        # includes mapping-aware semantic checks; the standalone final validator
        # must not erase those errors merely because its own checks pass.
        generation_status = llm_result.get("generation_status")
        generation_validation = llm_result.get("validation", {}) or {}
        generation_errors = generation_validation.get("errors", []) or []
        generation_gate_passed = (
            generation_status == "VALIDATED"
            and bool(generation_validation.get("valid"))
            and not generation_errors
        )
        if not generation_gate_passed:
            validation_result = dict(validation_result)
            combined_errors = list(validation_result.get("errors", []) or [])
            if not generation_errors:
                generation_errors = [
                    f"Generator validation did not pass (generation_status={generation_status!r})."
                ]
            for error in generation_errors:
                message = "Generator validation: " + str(error)
                if message not in combined_errors:
                    combined_errors.append(message)
            validation_result["errors"] = combined_errors
            validation_result["valid"] = False
            validation_result.setdefault("warnings", [])

        if not validation_result.get(

            "valid",

            False

        ):

            print(

                "PySpark validation: FAILED"

            )

            print(

                "Execution skipped."

            )

            review_result = self._human_review_result(
                mapping_name, "PYSPARK_VALIDATION_FAILED",
                "Generated PySpark failed mandatory validation; execution is blocked.",
                validations_passed=False, evidence=validation_result,
            )
            review_result.update({
                "llm_result": llm_result,
                "llm_result_path": llm_result_path,
                "validation": validation_result,
                "generated_pyspark": pyspark_path,
            })
            return review_result

        print(

            "PySpark validation: VALID"

        )

        # --------------------------------------------------------
        # 7. HUMAN GATEWAY: inspect synthetic merge keys AFTER
        # mandatory PySpark validation, but BEFORE the dry-run exit.
        # --------------------------------------------------------

        targets = llm_input.get("targets", []) or []
        synthetic_targets = []

        for target in targets:
            if not isinstance(target, dict):
                continue

            attributes = target.get("attributes", {}) or {}
            key_status = (
                target.get("merge_key_status")
                or attributes.get("merge_key_status")
            )

            if str(key_status or "").upper() == "SYNTHETIC_POC":
                synthetic_targets.append(
                    target.get("physical_table")
                    or target.get("target")
                    or target.get("name")
                    or "unknown target"
                )

        if synthetic_targets:
            message = (
                "Target merge keys are marked SYNTHETIC_POC: "
                + ", ".join(map(str, synthetic_targets))
                + ". A reviewer must confirm their use for this POC."
            )

            post_generation_evidence = {
                "approval_type": "MAPPING_MERGE_KEY_APPROVAL",
                "mapping_name": str(mapping_name),
                "targets": sorted(
                    [
                        {
                            "target": str(target.get("physical_table")
                                          or target.get("target")
                                          or target.get("name")
                                          or "unknown target"),
                            "merge_keys": sorted(
                                [str(k).strip() for k in (
                                    target.get("merge_keys")
                                    or (target.get("attributes") or {}).get("merge_keys")
                                    or []
                                ) if str(k).strip()]
                            ),
                            "source_status": "SYNTHETIC_POC",
                        }
                        for target in targets
                        if isinstance(target, dict)
                        and str(
                            target.get("merge_key_status")
                            or (target.get("attributes") or {}).get("merge_key_status")
                            or ""
                        ).upper() == "SYNTHETIC_POC"
                    ],
                    key=lambda item: item["target"].casefold(),
                ),
                "provenance": "HUMAN_APPROVED_POC_ASSUMPTION",
                "xml_authoritative": False,
                "execution_approved": False,
            }
            approved = self._approved_review_matches(
                mapping_name=mapping_name,
                category="UNVERIFIED_MERGE_KEY",
                expected_evidence=post_generation_evidence,
                scope="MAPPING",
            )

            if not approved:
                review_result = self._human_review_result(
                    mapping_name,
                    "UNVERIFIED_MERGE_KEY",
                    message,
                    validations_passed=True,
                    evidence={"synthetic_targets": synthetic_targets},
                    live_execution=not self.dry_run,
                )

                review_result.update({
                    "validation": validation_result,
                    "llm_result": llm_result,
                    "llm_result_path": llm_result_path,
                    "generated_pyspark": pyspark_path,
                })

                if self.dry_run:
                    review_result["stage"] = "DRY_RUN_REVIEW_REQUIRED"

                return review_result

            # Human approval does not automatically authorize live execution.
            if not self.dry_run and not self.allow_synthetic_merge_keys:
                return {
                    "success": False,
                    "mapping_name": mapping_name,
                    "stage": "EXECUTION_SAFETY_GATE",
                    "dry_run": False,
                    "executed": False,
                    "error": (
                        "Human review is approved, but execution remains blocked. "
                        "Set allow_synthetic_merge_keys=True only for an "
                        "explicitly approved POC run."
                    ),
                    "review_id": (self.approved_review_ids.get(mapping_name) or self.approved_review_id),
                    "validation": validation_result,
                }

        # --------------------------------------------------------
        # 7.1 SAFE DRY-RUN EXIT: never execute generated code in dry-run mode.
        # --------------------------------------------------------

        if self.dry_run:
            print(
                "DRY RUN: validation passed. Generated PySpark was NOT "
                "executed; no target writes were initiated by this pipeline."
            )
            return {
                "success": True,
                "mapping_name": mapping_name,
                "stage": "DRY_RUN_VALIDATION",
                "dry_run": True,
                "executed": False,
                "validation": validation_result,
                "llm_result": llm_result,
                "llm_result_path": llm_result_path,
                "generated_pyspark": pyspark_path,
                "blocking_issues": llm_input.get("blocking_issues", []),
            }

        # 8. EXECUTE GENERATED PYSPARK

        # --------------------------------------------------------

        try:

            print(

                "PySpark execution: RUNNING..."

            )

            execution_result = self.execute_pyspark(mapping_name)

            print(

                "PySpark execution: SUCCESS"

            )

        except Exception as error:

            error_traceback = traceback.format_exc()
            print(

                "PySpark execution: FAILED"

            )

            print(

                f"Error: {error}"

            )
            print("\nFull traceback:")
            print(error_traceback)

            return {

                "success": False,

                "mapping_name": mapping_name,

                "stage": "PYSPARK_EXECUTION",

                "llm_result": llm_result,

                "llm_result_path": llm_result_path,

                "validation": validation_result,

                "generated_pyspark": pyspark_path,

                "error": str(error),
                "traceback": error_traceback

            }

        # --------------------------------------------------------

        # 9. GENERIC DATA VALIDATION

        # --------------------------------------------------------

        try:

            print(

                "\nData validation: RUNNING..."

            )

            data_validator = DataValidator(

                spark=self.spark

            )

            data_validation_result = (

                data_validator.validate(

                    mapping=mapping,

                    llm_input=llm_input,

                    llm_result=llm_result

                )

            )

            data_validator.print_report(

                data_validation_result

            )

            if (

                data_validation_result.get(

                    "overall_status"

                )

                != "PASS"

            ):

                print(

                    "Data validation: FAILED"

                )

                review_result = self._human_review_result(
                    mapping_name, "DATA_VALIDATION_FAILED",
                    "Post-execution data validation did not pass. Target writes may already have occurred; investigate and remediate.",
                    validations_passed=False, evidence=data_validation_result,
                )
                review_result.update({
                    "stage": "DATA_VALIDATION",
                    "executed": True,
                    "llm_result": llm_result,
                    "llm_result_path": llm_result_path,
                    "validation": validation_result,
                    "data_validation": data_validation_result,
                    "generated_pyspark": pyspark_path,
                })
                return review_result

            print(

                "Data validation: PASS"

            )

        except Exception as error:

            error_traceback = traceback.format_exc()
            print(

                "Data validation: FAILED"

            )

            print(

                f"Error: {error}"

            )
            print("\nFull traceback:")
            print(error_traceback)

            return {

                "success": False,

                "mapping_name": mapping_name,

                "stage": "DATA_VALIDATION",

                "llm_result": llm_result,

                "llm_result_path": llm_result_path,

                "validation": validation_result,

                "generated_pyspark": pyspark_path,

                "error": str(error),
                "traceback": error_traceback

            }

        # --------------------------------------------------------

        # 10. SUCCESS

        # --------------------------------------------------------

        print(

            f"Mapping migration: SUCCESS - "

            f"{mapping_name}"

        )

        return {
            "success": True,
            "mapping_name": mapping_name,
            "stage": "COMPLETED",
            "dry_run": False,
            "executed": True,
            "execution_result": str(execution_result)[:500] if execution_result is not None else None,
            "llm_result": llm_result,

            "llm_result_path": llm_result_path,

            "validation": validation_result,

            "generated_pyspark": pyspark_path

        }

    @staticmethod
    def _source_column_names(columns):
        """Extract declared source column names from common parser metadata shapes."""
        names = []
        if not isinstance(columns, list):
            return names
        for item in columns:
            if isinstance(item, str):
                name = item
            elif isinstance(item, dict):
                name = (
                    item.get("name")
                    or item.get("column_name")
                    or item.get("field_name")
                    or item.get("port_name")
                    or item.get("column")
                )
            else:
                name = None
            if name and str(name).strip():
                names.append(str(name).strip())
        # Preserve order while removing duplicates case-insensitively.
        result = []
        seen = set()
        for name in names:
            key = name.casefold()
            if key not in seen:
                seen.add(key)
                result.append(name)
        return result

    def _preflight_sources(self, llm_input):
        """Resolve explicitly configured source overrides and validate table schemas.

        This method is read-only. It does not create tables, mutate data, infer
        business equivalence, or substitute a source unless source_overrides was
        explicitly supplied by the caller.
        """
        result = {
            "ready": True,
            "checked_sources": [],
            "warnings": [],
            "blocking_issues": [],
            "applied_overrides": [],
        }
        if not isinstance(llm_input, dict):
            result["ready"] = False
            result["blocking_issues"].append("LLM input is not a dictionary; source preflight cannot run.")
            return result

        sources = llm_input.get("sources", [])
        if not isinstance(sources, list):
            result["ready"] = False
            result["blocking_issues"].append("LLM input 'sources' is not a list.")
            return result
        if not sources:
            result["ready"] = False
            result["blocking_issues"].append("No sources were declared in the LLM input.")
            return result

        for index, source in enumerate(sources):
            if not isinstance(source, dict):
                result["blocking_issues"].append(f"Source at index {index} is not a metadata dictionary.")
                continue

            logical_name = source.get("logical_name") or source.get("source") or source.get("name") or source.get("object") or f"source_{index}"
            original_physical = source.get("physical_table")
            override = None
            override_key = None
            for candidate in (logical_name, source.get("name"), source.get("source"), original_physical):
                if candidate is not None and str(candidate) in self.source_overrides:
                    override = self.source_overrides[str(candidate)]
                    override_key = str(candidate)
                    break

            if override is not None:
                override = str(override).strip()
                parts = override.split(".")
                if len(parts) != 3 or any(not part.strip() for part in parts):
                    result["blocking_issues"].append(
                        f"Invalid source override for {logical_name!r}: {override!r}. "
                        "Use a fully qualified catalog.schema.table identifier."
                    )
                    continue
                source["physical_table"] = override
                source["file_path"] = None
                source["location_type"] = "table"
                source["location_resolution"] = f"Explicit source override configured for {override_key}."
                source["source_override_applied"] = True
                result["applied_overrides"].append({
                    "logical_name": logical_name,
                    "from": original_physical,
                    "to": override,
                    "override_key": override_key,
                })

            location_type = str(source.get("location_type") or "unresolved").lower()
            physical_table = source.get("physical_table")

            if location_type == "table" or physical_table:
                if not physical_table:
                    result["blocking_issues"].append(
                        f"Source {logical_name!r} is marked as a table but has no physical_table identifier."
                    )
                    continue
                table_name = str(physical_table)
                item = {
                    "logical_name": logical_name,
                    "location_type": "table",
                    "physical_table": table_name,
                    "status": "unchecked",
                }
                try:
                    dataframe = self.spark.table(table_name)
                    actual_columns = [field.name for field in dataframe.schema.fields]
                    actual_by_casefold = {name.casefold(): name for name in actual_columns}
                    item["status"] = "available"
                    item["actual_columns"] = actual_columns
                    source["catalog_lookup_status"] = "available"
                    source["catalog_columns"] = actual_columns

                    declared_columns = self._source_column_names(
                        source.get("columns") or source.get("fields") or source.get("ports") or []
                    )
                    # Only compare when source metadata explicitly declares columns.
                    if declared_columns:
                        missing = [name for name in declared_columns if name.casefold() not in actual_by_casefold]
                        item["declared_columns"] = declared_columns
                        item["missing_declared_columns"] = missing
                        if missing:
                            item["status"] = "schema_mismatch"
                            issue = (
                                f"Source {logical_name!r} resolves to {table_name!r}, but declared columns "
                                f"are missing from the catalog table: {missing}."
                            )
                            result["blocking_issues"].append(issue)
                    source["source_preflight_status"] = item["status"]
                except Exception as exc:
                    item["status"] = "unavailable"
                    item["error"] = str(exc)[:500]
                    source["catalog_lookup_status"] = "unavailable"
                    source["source_preflight_status"] = "unavailable"
                    result["blocking_issues"].append(
                        f"Source {logical_name!r} table {table_name!r} is unavailable: {str(exc)[:300]}"
                    )
                result["checked_sources"].append(item)
                continue

            if location_type == "file":
                file_path = source.get("resolved_file_path") or source.get("file_path")
                item = {
                    "logical_name": logical_name,
                    "location_type": "file",
                    "file_path": file_path,
                    "status": "unchecked",
                }
                if not file_path or not isinstance(file_path, str):
                    item["status"] = "unavailable"
                    item["error"] = "No resolved file_path was supplied in source metadata."
                    result["blocking_issues"].append(
                        f"File source {logical_name!r} has no resolved file_path; configure an explicit path."
                    )
                    result["checked_sources"].append(item)
                    continue

                # Only infer formats from unambiguous file extensions. Do not
                # guess a base path or replace a missing file with another source.
                clean_path = file_path.split("?", 1)[0].rstrip("/")
                extension = os.path.splitext(clean_path)[1].lower()
                declared_format = source.get("format") or source.get("file_format")
                format_map = {".csv": "csv", ".parquet": "parquet", ".json": "json", ".jsonl": "json"}
                file_format = str(declared_format).lower() if declared_format else format_map.get(extension)
                if file_format not in {"csv", "parquet", "json", "delta", "orc", "avro"}:
                    item["status"] = "unavailable"
                    item["error"] = f"Cannot resolve a supported file format from metadata/path (extension={extension!r})."
                    result["blocking_issues"].append(
                        f"File source {logical_name!r} has no explicit supported format and its extension is not recognized."
                    )
                    result["checked_sources"].append(item)
                    continue

                try:
                    reader = self.spark.read.format(file_format)
                    if file_format == "csv":
                        reader = reader.options(**self.csv_defaults)
                    dataframe = reader.load(file_path)
                    actual_columns = [field.name for field in dataframe.schema.fields]
                    actual_by_casefold = {name.casefold(): name for name in actual_columns}
                    item["status"] = "available"
                    item["format"] = file_format
                    item["actual_columns"] = actual_columns
                    declared_columns = self._source_column_names(
                        source.get("columns") or source.get("fields") or source.get("ports") or []
                    )
                    if declared_columns:
                        missing = [name for name in declared_columns if name.casefold() not in actual_by_casefold]
                        item["declared_columns"] = declared_columns
                        item["missing_declared_columns"] = missing
                        if missing:
                            item["status"] = "schema_mismatch"
                            result["blocking_issues"].append(
                                f"File source {logical_name!r} at {file_path!r} is missing declared columns: {missing}."
                            )
                    source["source_preflight_status"] = item["status"]
                    source["resolved_file_format"] = file_format
                except Exception as exc:
                    item["status"] = "unavailable"
                    item["error"] = str(exc)[:500]
                    source["source_preflight_status"] = "unavailable"
                    result["blocking_issues"].append(
                        f"File source {logical_name!r} at {file_path!r} cannot be read: {str(exc)[:300]}"
                    )
                result["checked_sources"].append(item)
                continue

            result["blocking_issues"].append(
                f"Source {logical_name!r} has unresolved location metadata. "
                "Configure its source location or provide an explicit source override."
            )

        result["ready"] = not result["blocking_issues"]
        # Keep the resolved source metadata in the LLM input for traceability.
        llm_input["source_preflight"] = result
        existing_blockers = llm_input.get("blocking_issues", [])
        if not isinstance(existing_blockers, list):
            existing_blockers = [str(existing_blockers)]
        llm_input["blocking_issues"] = list(existing_blockers) + list(result["blocking_issues"])
        return result

    def _apply_approved_poc_overrides(self, llm_input, mapping_name):
        """Apply narrowly scoped, user-approved POC assumptions.

        This does not invent missing target schemas, join/filter conditions,
        aggregation grouping, or SCD2 validity semantics. The override is
        explicitly labelled as a POC assumption in the LLM input and reports.
        """
        if not isinstance(llm_input, dict):
            return
        normalized_mapping_name = str(mapping_name or "").strip()

        # MAP100: the user approved customer_id as the POC business match key
        # for the three Customer 360 UPSERT targets. This remains an explicitly
        # labelled POC assumption, not authoritative IDMC metadata.
        if normalized_mapping_name == "m_customer_360_enrichment":
            approved_map100 = {
                "mapping_name": normalized_mapping_name,
                "status": "APPROVED_POC_ASSUMPTION",
                "approved_by_user": True,
                "assumptions": [{
                    "targets": [
                        "idmc_poc.curated.customer_gold",
                        "idmc_poc.curated.customer_standard",
                        "idmc_poc.curated.customer_low_value",
                    ],
                    "merge_keys": ["customer_id"],
                    "purpose": "User-approved POC match key for the three Customer 360 targets.",
                    "provenance": "HUMAN_APPROVED_POC_ASSUMPTION; not authoritative XML metadata.",
                }],
            }
            for target in (llm_input.get("targets", []) or []):
                if not isinstance(target, dict):
                    continue
                physical = str(target.get("physical_table") or "").strip().casefold()
                logical = str(target.get("logical_name") or target.get("target") or target.get("name") or "").strip().casefold()
                if physical in {
                    "idmc_poc.curated.customer_gold",
                    "idmc_poc.curated.customer_standard",
                    "idmc_poc.curated.customer_low_value",
                } or logical in {
                    "curated.customer_gold", "curated.customer_standard",
                    "curated.customer_low_value", "tgt_customer_gold",
                    "tgt_customer_standard", "tgt_customer_low_value",
                }:
                    target["merge_keys"] = ["customer_id"]
                    target["merge_key_status"] = "APPROVED_POC_ASSUMPTION"
                    target["merge_key_provenance"] = (
                        "HUMAN_APPROVED_POC_ASSUMPTION; not authoritative XML metadata."
                    )
                    target["merge_key_authoritative_from_xml"] = False

            # The XML sequence is id=SEQ1, name=SEQ_CUSTOMER_SK, output customer_sk,
            # startValue=100000, incrementBy=1. Attach the parsed semantics by name.
            for transformation in (llm_input.get("transformations", []) or []):
                if not isinstance(transformation, dict):
                    continue
                name = str(transformation.get("name") or transformation.get("id") or "").strip().casefold()
                kind = str(transformation.get("type") or transformation.get("transformation_type") or "").casefold()
                if name in {"seq_customer_sk", "seq1"} or ("sequence" in kind and "customer_sk" in str(transformation).casefold()):
                    transformation["sequence_field"] = "customer_sk"
                    transformation["output_field"] = "customer_sk"
                    transformation["business_key"] = "customer_id"
                    transformation["sequence_start"] = 100000
                    transformation["sequence_increment"] = 1
                    transformation["sequence_semantics_provenance"] = (
                        "Parsed from IDMC SequenceTransformation SEQ_CUSTOMER_SK."
                    )

            # MAP100 reject-target audit fields are derivable from the mapping-local
            # DQR109 rule reference and the declared reject target schema. Keep each
            # derivation explicit and provenance-labelled; do not infer arbitrary
            # target columns or apply repository-wide DQ rules.
            mapping_rule_ids = set()
            for rule in (llm_input.get("data_quality_rules", []) or []):
                if not isinstance(rule, dict):
                    continue
                rule_id = str(rule.get("id") or rule.get("object_id") or rule.get("rule_id") or "").strip()
                if rule_id:
                    mapping_rule_ids.add(rule_id)
            transformations_for_dq = llm_input.get("transformations", []) or []
            mapping_rule_ids.update(
                str(rule_id).strip()
                for transformation in transformations_for_dq
                if isinstance(transformation, dict)
                for rule_id in (transformation.get("rule_refs") or transformation.get("rule_ids") or [])
                if str(rule_id).strip()
            )

            reject_derivations = {
                "dq_rule_id": {
                    "expression": "literal DQR109",
                    "value": "DQR109",
                    "provenance": "Mapping-local RuleRef in DQ_GATE_MAP100; corrected XML.",
                },
                "failure_reason": {
                    "expression": "literal descriptive reason for DQR109 failure",
                    "value": "Email failed DQR109 email-format validation",
                    "provenance": "Derived from DQR109 RuleExpression and REJECT route.",
                },
                "rejected_at": {
                    "expression": "F.current_timestamp()",
                    "provenance": "Standard event timestamp for the reject record.",
                },
                "source_mapping": {
                    "expression": "literal mapping name",
                    "value": "m_customer_360_enrichment",
                    "provenance": "Selected mapping identity.",
                },
            }
            for target in (llm_input.get("targets", []) or []):
                if not isinstance(target, dict):
                    continue
                physical = str(target.get("physical_table") or "").strip().casefold()
                if physical.endswith("quality.customer_360_rejects"):
                    declared_columns = {
                        str(column.get("name") or column.get("column_name") or "").strip()
                        if isinstance(column, dict) else str(column).strip()
                        for column in (target.get("columns") or target.get("ports") or [])
                    }
                    required_audit_columns = set(reject_derivations)
                    if required_audit_columns.issubset(declared_columns) and "DQR109" in mapping_rule_ids:
                        target["column_derivations"] = reject_derivations
                        target["column_derivations_provenance"] = (
                            "Mapping-local DQR109 RuleRef plus explicit MAP100 POC audit-field derivations."
                        )
                        target["reject_rule_id"] = "DQR109"
                        target["reject_route"] = "REJECT"
                        target["notification_route"] = "NOTIFY"

            llm_input["approved_poc_overrides"] = approved_map100

            # The XML/input builder may have emitted a pre-override blocker saying
            # these UPSERT targets have no merge keys. That statement is stale only
            # for the three explicitly approved MAP100 POC targets. Remove that
            # exact class of blocker after attaching the keys; do not alter XML
            # metadata or claim the keys are XML-authoritative.
            approved_targets = {
                "idmc_poc.curated.customer_gold",
                "idmc_poc.curated.customer_standard",
                "idmc_poc.curated.customer_low_value",
            }
            retained_blockers = []
            for blocker in (llm_input.get("blocking_issues", []) or []):
                message = str(blocker)
                lower = message.casefold()
                refers_to_approved_target = any(name in lower for name in approved_targets)
                is_missing_merge_key_blocker = (
                    "merge key" in lower
                    and any(token in lower for token in (
                        "no merge keys", "does not define merge keys",
                        "merge keys are not declared", "merge keys are declared",
                        "without merge keys", "has no merge keys",
                    ))
                )
                if refers_to_approved_target and is_missing_merge_key_blocker:
                    continue
                retained_blockers.append(message)
            llm_input["blocking_issues"] = retained_blockers

            # DQ blockers come from mapping-scoped XML RuleRefs resolved by
            # LLMInputBuilder. Do not inject repository-level rules that are
            # not referenced by this mapping's transformations.
            return

        if normalized_mapping_name != "m_orders_enrichment_scd2":
            return

        approved = {
            "mapping_name": "m_orders_enrichment_scd2",
            "status": "APPROVED_POC_ASSUMPTION",
            "approved_by_user": True,
            "assumptions": [
                {
                    "transformation": "EXP_NORMALIZE_ORDER",
                    "field_alias": "business_key",
                    "source_field": "order_id",
                    "expression": "UPPER(TRIM(CAST(order_id AS STRING)))",
                    "purpose": "Resolve the XML expression's undefined business_key reference.",
                    "provenance": "User-approved POC assumption; not declared by IDMC XML.",
                },
                {
                    "target": "TGT_ORDER_SILVER",
                    "merge_keys": ["order_id"],
                    "purpose": "POC MERGE key for the silver orders target.",
                    "provenance": "User-approved POC assumption; not declared by IDMC XML.",
                },
            ],
        }

        targets = llm_input.get("targets", []) or []
        matched_target = None
        for target in targets:
            if not isinstance(target, dict):
                continue
            names = {
                str(target.get(key) or "").strip().casefold()
                for key in ("logical_name", "name", "target", "object", "target_name")
            }
            physical = str(target.get("physical_table") or "").strip().casefold()
            if "tgt_order_silver" in names or physical.endswith(".silver.orders"):
                matched_target = target
                break

        if matched_target is not None:
            matched_target["merge_keys"] = ["order_id"]
            matched_target["merge_key_provenance"] = "APPROVED_POC_ASSUMPTION"
            matched_target["merge_key_authoritative_from_xml"] = False

        transformations = llm_input.get("transformations", []) or []
        matched_transformation = None
        for transformation in transformations:
            if not isinstance(transformation, dict):
                continue
            name = str(
                transformation.get("name") or transformation.get("id") or ""
            ).strip().casefold()
            if name == "exp_normalize_order":
                matched_transformation = transformation
                break

        if matched_transformation is not None:
            matched_transformation["expression"] = (
                "UPPER(TRIM(CAST(order_id AS STRING)))"
            )
            matched_transformation["expression_source"] = "APPROVED_POC_ASSUMPTION"
            matched_transformation["approved_poc_expression_override"] = (
                "UPPER(TRIM(CAST(order_id AS STRING)))"
            )
            matched_transformation["approved_poc_expression_override_provenance"] = (
                "APPROVED_POC_ASSUMPTION; user-approved alias business_key=order_id; "
                "not declared by IDMC XML."
            )
            # Keep a normalized alias for downstream transformations/codegen.
            matched_transformation["approved_poc_output_alias"] = "normalized_key"

        # The repository declares the logical file source as landing/orders_delta,
        # but the actual landing asset in this workspace is the orders_delta.csv
        # directory. Configure the exact observed location and CSV format before
        # source preflight. Spark can read a directory containing CSV part-files.
        # This is a workspace-specific source-location override, not an XML fact.
        orders_source_path = (
            "/Volumes/idmc_poc/bronze/idmc_files/"
            "landing/orders_delta.csv"
        )
        source_overrides_applied = []
        for source in (llm_input.get("sources", []) or []):
            if not isinstance(source, dict):
                continue
            source_identifiers = {
                str(source.get(key) or "").strip().casefold()
                for key in (
                    "logical_name", "name", "source", "object",
                    "source_name", "file_path", "resolved_file_path"
                )
            }
            if any(
                identifier == "landing/orders_delta"
                or identifier.endswith("/landing/orders_delta")
                for identifier in source_identifiers
                if identifier
            ):
                source["file_path"] = orders_source_path
                source["resolved_file_path"] = orders_source_path
                source["format"] = "csv"
                source["file_format"] = "csv"
                source["source_location_override"] = (
                    "WORKSPACE_POC_OVERRIDE; observed landing/orders_delta.csv directory"
                )
                source_overrides_applied.append(
                    source.get("logical_name") or source.get("name") or "orders source"
                )

        approved["source_location_overrides"] = [
            {
                "logical_source": name,
                "path": orders_source_path,
                "format": "csv",
                "provenance": (
                    "Workspace inspection showed landing/orders_delta.csv as a directory; "
                    "configured as a CSV directory for this POC."
                ),
            }
            for name in source_overrides_applied
        ]

        llm_input["approved_poc_overrides"] = approved

        # Remove only the exact merge-key blocker for the target we just resolved.
        # All other blockers remain intact and must still fail closed.
        blockers = llm_input.get("blocking_issues", []) or []
        filtered = []
        for blocker in blockers:
            text = str(blocker)
            is_this_target = (
                matched_target is not None
                and (
                    "tgt_order_silver" in text.casefold()
                    or "silver.orders" in text.casefold()
                )
            )
            is_missing_merge_key = (
                "merge" in text.casefold()
                and "merge key" in text.casefold()
                and ("no merge" in text.casefold() or "without" in text.casefold())
            )
            if is_this_target and is_missing_merge_key:
                continue
            filtered.append(text)
        llm_input["blocking_issues"] = filtered

        if self.verbose:
            print(
                "Approved POC override applied: "
                "EXP_NORMALIZE_ORDER.business_key=order_id; "
                "TGT_ORDER_SILVER merge key=order_id."
            )
            if source_overrides_applied:
                print(
                    "Orders source override applied: "
                    f"{orders_source_path} (CSV); matched={source_overrides_applied}"
                )
            else:
                print(
                    "WARNING: orders source path override did not match a source "
                    "metadata record; source preflight will remain blocked."
                )
            if matched_target is None:
                print("WARNING: TGT_ORDER_SILVER target instance was not matched; key override recorded but not attached.")
            if matched_transformation is None:
                print("WARNING: EXP_NORMALIZE_ORDER was not matched; expression override recorded but not attached.")

    def _enrich_target_metadata_from_catalog(self, llm_input):
        """Best-effort target schema enrichment; never fail a mapping because
        a not-yet-created target table is absent or Spark Connect defers analysis.

        Existing XML metadata is preserved. This method does not invent merge keys
        and does not create or write target tables.
        """
        if not isinstance(llm_input, dict):
            return

        targets = llm_input.get("targets", [])
        if not isinstance(targets, list):
            targets = []

        for target in targets:
            if not isinstance(target, dict):
                continue

            table_name = target.get("physical_table")
            if not table_name:
                continue

            operation = str(target.get("operation", "")).upper()
            requires_merge_keys = operation in {"UPSERT", "MERGE"}

            # XML already supplied the necessary metadata: don't query the catalog.
            if target.get("columns") and (
                target.get("merge_keys") or not requires_merge_keys
            ):
                continue

            try:
                df = self.spark.table(str(table_name))
                # Spark Connect may defer missing-table errors until schema is read.
                catalog_columns = [
                    {
                        "name": field.name,
                        "type": field.dataType.simpleString(),
                        "nullable": bool(field.nullable),
                    }
                    for field in df.schema.fields
                ]
            except Exception as exc:
                target["catalog_lookup_status"] = "unavailable"
                target["catalog_lookup_error"] = str(exc)[:500]
                print(
                    f"[DEBUG] Catalog schema unavailable for target {table_name}; "
                    "preserving XML metadata and continuing."
                )
                continue

            target["catalog_lookup_status"] = "available"
            if not target.get("columns") and catalog_columns:
                target["columns"] = catalog_columns
                target["target_schema_source"] = "existing_catalog_table"

            # Deliberately do not infer merge keys from column names or data.

        # Clear only blockers that are actually resolved by supplied metadata.
        blockers = llm_input.get("blocking_issues", []) or []
        filtered_blockers = []
        for blocker in blockers:
            blocker_text = str(blocker)
            resolved = False
            for target in targets:
                if not isinstance(target, dict):
                    continue
                table_name = target.get("physical_table")
                if not table_name or str(table_name) not in blocker_text:
                    continue
                if "has no target columns/schema declared" in blocker_text and target.get("columns"):
                    resolved = True
                    break
                if "does not define merge keys" in blocker_text and target.get("merge_keys"):
                    resolved = True
                    break
            if not resolved:
                filtered_blockers.append(blocker_text)
        llm_input["blocking_issues"] = filtered_blockers

    # ============================================================

    # LLM INPUT SUMMARY

    # ============================================================

    @staticmethod

    def print_llm_input_summary(

        mapping_name,

        llm_input

    ):

        """

        Print only the important metadata summary.

        """

        sources = llm_input.get(

            "sources",

            []

        )

        transformations = llm_input.get(

            "transformations",

            []

        )

        targets = llm_input.get(

            "targets",

            []

        )

        links = llm_input.get(

            "links",

            []

        )

        graph = llm_input.get(

            "graph",

            {}

        )

        execution_order = graph.get(

            "execution_order",

            []

        )

        transformation_types = sorted(

            {

                transformation.get("type")

                for transformation in transformations

                if isinstance(

                    transformation,

                    dict

                )

                and transformation.get("type")

            }

        )

        stored_procedures = [

            transformation

            for transformation in transformations

            if isinstance(

                transformation,

                dict

            )

            and str(

                transformation.get(

                    "type",

                    ""

                )

            ).upper()

            == "STORED_PROCEDURE"

        ]

        print(

            f"\nLLM input ready: {mapping_name}"

        )

        print(

            f"  Sources            : {len(sources)}"

        )

        print(

            f"  Transformations    : "

            f"{len(transformations)}"

        )

        print(

            f"  Targets            : {len(targets)}"

        )

        print(

            f"  Links              : {len(links)}"

        )

        print(

            f"  Execution nodes    : "

            f"{len(execution_order)}"

        )

        # --------------------------------------------------------

        # Transformation types

        # --------------------------------------------------------

        transformation_type_text = (

            ", ".join(

                transformation_types

            )

            if transformation_types

            else "None"

        )

        print(

            f"  Transformation types: "

            f"{transformation_type_text}"

        )

        # --------------------------------------------------------

        # Stored procedures

        # --------------------------------------------------------

        if stored_procedures:

            print(

                f"  Stored procedures  : "

                f"{len(stored_procedures)}"

            )

            for procedure in stored_procedures:

                procedure_name = (

                    procedure.get(

                        "procedureName"

                    )

                    or procedure.get(

                        "procedure_name"

                    )

                    or procedure.get(

                        "name"

                    )

                )

                print(

                    f"    - {procedure_name}"

                )

                print(

                    "      SP logic will be "

                    "converted to PySpark by LLM."

                )

    # ============================================================

    # PYSPARK VALIDATION

    # ============================================================

    @staticmethod

    def validate_llm_pyspark(

        mapping_name,

        llm_input,

        generated_code,

        spark=None

    ):

        """

        Validate LLM-generated PySpark code.

        """

        print(

            "\nPySpark validation:"

        )

        validator = LLMPySparkValidator(

            generated_code=generated_code,

            llm_input=llm_input,

            spark=spark

        )

        validation_result = (

            validator.validate()

        )

        errors = validation_result.get(

            "errors",

            []

        )

        warnings = validation_result.get(

            "warnings",

            []

        )

        validation_status = (

            "VALID"

            if validation_result.get(

                "valid"

            )

            else "INVALID"

        )

        print(

            f"  Status   : {validation_status}"

        )

        print(

            f"  Errors   : {len(errors)}"

        )

        print(

            f"  Warnings : {len(warnings)}"

        )

        if errors:

            for error in errors:

                print(

                    f"    ERROR: {error}"

                )

        if warnings:

            for warning in warnings:

                print(

                    f"    WARNING: {warning}"

                )

        return validation_result

    # ============================================================

    # SAVE LLM RESULT

    # ============================================================

    def save_llm_result(

        self,

        mapping_name,

        llm_result

    ):

        """

        Save complete LLM response as JSON.

        """

        output_directory = os.path.join(

            self.output_dir,

            "llm_results"

        )

        os.makedirs(

            output_directory,

            exist_ok=True

        )

        safe_name = self._safe_filename(

            mapping_name

        )

        filename = (

            f"{safe_name}_llm_result.json"

        )

        output_path = os.path.join(

            output_directory,

            filename

        )

        with open(

            output_path,

            "w",

            encoding="utf-8"

        ) as file_handle:

            json.dump(

                llm_result,

                file_handle,

                indent=2,

                ensure_ascii=False

            )

        return output_path

    # ============================================================

    # SAVE GENERATED PYSPARK

    # ============================================================

    def save_generated_pyspark(

        self,

        mapping_name,

        generated_code

    ):

        """

        Save generated PySpark as a .py artifact.

        """

        os.makedirs(

            self.output_dir,

            exist_ok=True

        )

        safe_name = self._safe_filename(

            mapping_name

        )

        output_path = os.path.join(

            self.output_dir,

            f"{safe_name}.py"

        )

        with open(

            output_path,

            "w",

            encoding="utf-8"

        ) as file_handle:

            file_handle.write(

                generated_code.rstrip()

                + "\n"

            )

        return output_path

    # ============================================================

    # EXECUTE GENERATED PYSPARK

    # ============================================================

    def execute_pyspark(self, mapping_name):
        """Execute exactly one mapping-specific generated PySpark entry point.

        Accepts the exact mapping name or a constrained execution prefix
        (migrate_, run_, execute_) followed by that exact mapping name.
        Only top-level functions are eligible; nested helper functions are not.
        """
        import ast

        artifact_path = os.path.join(
            self.output_dir,
            f"{self._safe_filename(mapping_name)}.py",
        )

        if not os.path.isfile(artifact_path):
            raise FileNotFoundError(
                f"Generated PySpark artifact not found: {artifact_path}"
            )

        with open(artifact_path, "r", encoding="utf-8") as handle:
            persisted_code = handle.read()

        if not persisted_code.strip():
            raise ValueError(
                f"Generated PySpark artifact is empty: {artifact_path}"
            )

        def normalize(value):
            return re.sub(
                r"[^a-z0-9]+", "_", str(value).lower()
            ).strip("_")

        normalized_mapping = normalize(mapping_name)

        # Prefixes are constrained to the exact normalized mapping name.
        # Explicit legacy aliases remain for earlier generated artifacts.
        approved_names = {
            normalized_mapping,
            f"migrate_{normalized_mapping}",
            f"run_{normalized_mapping}",
            f"execute_{normalized_mapping}",
            # Some valid LLM-generated artifacts expose a generic entry point.
            # The artifact filename is already scoped to this mapping, and we
            # still require exactly one top-level match before executing it.
            "run_migration",
        }
        legacy_aliases = {
            "m_customer_standardize": {"migrate_customer_standardize"},
            "m_order_enrichment": {"migrate_orders", "migrate_order_enrichment"},
            "m_inventory_reconciliation": {"migrate_inventory_reconciliation"},
            "m_daily_sales_summary": {"migrate_daily_sales_summary"},
        }
        approved_names.update(
            normalize(name)
            for name in legacy_aliases.get(normalized_mapping, set())
        )

        try:
            syntax_tree = ast.parse(persisted_code, filename=artifact_path)
        except SyntaxError as exc:
            raise RuntimeError(
                f"Generated PySpark has a syntax error in {artifact_path}: {exc}"
            ) from exc

        # Only top-level function definitions can be migration entry points.
        top_level_functions = [
            node.name
            for node in syntax_tree.body
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        ]
        matching_names = [
            name for name in top_level_functions
            if normalize(name) in approved_names
        ]

        if len(matching_names) != 1:
            raise RuntimeError(
                f"Expected exactly one mapping-specific entry point for "
                f"{mapping_name!r}; found {matching_names}. "
                f"Top-level functions: {top_level_functions}. "
                f"Approved names: {sorted(approved_names)}. "
                f"Review {artifact_path} before execution."
            )

        function_name = matching_names[0]

        # The expected entry point is confirmed before module execution.
        namespace = {
            "__builtins__": __builtins__,
            "__name__": "__migration_generated__",
            "spark": self.spark,
        }
        exec(
            compile(persisted_code, artifact_path, "exec"),
            namespace,
            namespace,
        )

        function = namespace.get(function_name)
        if not inspect.isfunction(function):
            raise RuntimeError(
                f"Entry point {function_name!r} did not load as a function."
            )

        if inspect.iscoroutinefunction(function):
            raise RuntimeError(
                f"Async entry point {function_name!r} is not supported."
            )

        parameters = list(inspect.signature(function).parameters.values())

        if not parameters:
            # Supports older generated functions that use injected global spark.
            return function()

        if (
            len(parameters) == 1
            and parameters[0].name in {"spark", "spark_session", "session"}
            and parameters[0].kind in {
                inspect.Parameter.POSITIONAL_ONLY,
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
            }
        ):
            return function(self.spark)

        raise RuntimeError(
            f"Unsupported signature for {function_name!r}: "
            f"{inspect.signature(function)}. "
            "Expected no parameters or exactly one Spark-session parameter "
            "named spark, spark_session, or session."
        )

    @staticmethod
    def _compact_result(result):
        """Compact notebook summary that retains Human Gateway review details."""
        validation = result.get("validation", {}) or {}
        stage = result.get("stage", "UNKNOWN")
        human_review = result.get("human_review") or {}

        return {
            "mapping_name": result.get("mapping_name", "Unknown"),
            "success": bool(result.get("success")),
            "stage": stage,
            "dry_run": result.get("dry_run"),
            "executed": result.get(
                "executed",
                stage in {"COMPLETED", "DATA_VALIDATION"},
            ),
            "validation_status": (
                "VALID"
                if validation.get("valid")
                else ("INVALID" if validation else "NOT_REPORTED")
            ),
            "error_count": len(validation.get("errors", []) or []),
            "warning_count": len(validation.get("warnings", []) or []),
            "review_id": result.get("review_id") or human_review.get("review_id"),
            "review_status": human_review.get("status"),
            "review_scope": human_review.get("scope"),
            "review_issues": human_review.get("issues", []),
            "review_decision": human_review.get("decision"),
            "generated_pyspark": result.get("generated_pyspark"),
            "llm_result_path": result.get("llm_result_path"),
            "error": result.get("error"),
        }

    def write_migration_report(self, migration_results):
        """Write an Excel report using actual pipeline outcomes; never infer PASS."""
        rows = []
        for result in migration_results or []:
            validation = result.get("validation") or {}
            generator_validation = (
                (result.get("llm_result") or {}).get("validation") or {}
            )
            source_preflight = result.get("source_preflight") or {}
            rows.append({
                "Mapping": result.get("mapping_name"),
                "Status": "PASS" if result.get("success") else "FAIL",
                "Stage": result.get("stage"),
                "Dry Run": result.get("dry_run", self.dry_run),
                "Executed": result.get("executed", False),
                "Final Validator": (
                    "PASS" if validation.get("valid") else
                    "FAIL" if validation else "NOT RUN"
                ),
                "Generator Validator": (
                    "PASS" if generator_validation.get("valid") else
                    "FAIL" if generator_validation else "NOT RUN"
                ),
                "Source Preflight": (
                    "PASS" if source_preflight.get("ready") else
                    "FAIL" if source_preflight else "NOT RECORDED"
                ),
                "Blocking Issues": " | ".join(
                    str(x) for x in (
                        result.get("blocking_issues")
                        or validation.get("errors")
                        or generator_validation.get("errors")
                        or []
                    )
                ),
                "Error": str(result.get("error") or ""),
                "Generated PySpark": str(result.get("generated_pyspark") or ""),
                "LLM Result": str(result.get("llm_result_path") or ""),
                "Review ID": str(result.get("review_id") or ""),
            })

        report_dir = os.path.join(self.output_dir, "reports")
        os.makedirs(report_dir, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        base_path = os.path.join(report_dir, f"migration_report_{timestamp}")
        json_path = base_path + ".json"
        csv_path = base_path + ".csv"
        xlsx_path = base_path + ".xlsx"

        with open(json_path, "w", encoding="utf-8") as handle:
            json.dump(rows, handle, indent=2, ensure_ascii=False, default=str)

        import csv
        fieldnames = list(rows[0].keys()) if rows else [
            "Mapping", "Status", "Stage", "Dry Run", "Executed",
            "Final Validator", "Generator Validator", "Source Preflight",
            "Blocking Issues", "Error", "Generated PySpark", "LLM Result",
            "Review ID",
        ]
        with open(csv_path, "w", newline="", encoding="utf-8-sig") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

        report_result = {
            "json": json_path,
            "csv": csv_path,
            "xlsx": None,
            "row_count": len(rows),
        }
        try:
            from openpyxl import Workbook
            from openpyxl.styles import Font, PatternFill, Alignment
            from openpyxl.utils import get_column_letter

            workbook = Workbook()
            sheet = workbook.active
            sheet.title = "Migration Results"
            sheet.append(fieldnames)
            for row in rows:
                sheet.append([row.get(field) for field in fieldnames])

            for cell in sheet[1]:
                cell.font = Font(bold=True, color="FFFFFF")
                cell.fill = PatternFill("solid", fgColor="1F4E78")
                cell.alignment = Alignment(wrap_text=True, vertical="top")

            sheet.freeze_panes = "A2"
            sheet.auto_filter.ref = sheet.dimensions
            for column_cells in sheet.columns:
                max_length = max(
                    (len(str(cell.value or "")) for cell in column_cells),
                    default=10,
                )
                width = min(max(max_length + 2, 12), 60)
                sheet.column_dimensions[
                    get_column_letter(column_cells[0].column)
                ].width = width
            for row_cells in sheet.iter_rows(min_row=2):
                for cell in row_cells:
                    cell.alignment = Alignment(wrap_text=True, vertical="top")

            try:
                workbook.save(xlsx_path)
                report_result["xlsx"] = xlsx_path
            except OSError as exc:
                report_result["xlsx_error"] = str(exc)
                print(
                    "Excel report could not be written to the configured output "
                    f"location ({exc}). JSON/CSV reports remain available."
                )
        except ImportError:
            print(
                "openpyxl is unavailable. JSON and CSV reports were created; "
                "install openpyxl to create the Excel workbook."
            )
        return report_result

    # FINAL SUMMARY

    # ============================================================

    @staticmethod

    def print_final_summary(

        migration_results

    ):

        """

        Print concise final migration summary.

        """

        total = len(

            migration_results

        )

        successful = sum(

            1

            for result in migration_results

            if result.get("success")

        )

        failed = (

            total

            - successful

        )

        print(

            "\n" + "=" * 60

        )

        print(

            "MIGRATION SUMMARY"

        )

        print(

            "=" * 60

        )

        print(

            f"Mappings Processed : {total}"

        )

        print(

            f"Successful         : {successful}"

        )

        print(

            f"Failed             : {failed}"

        )

        if migration_results:

            print(

                "\nMapping Results:"

            )

            for result in migration_results:

                mapping_name = result.get(

                    "mapping_name",

                    "Unknown"

                )

                status = (

                    "SUCCESS"

                    if result.get(

                        "success"

                    )

                    else "FAILED"

                )

                print(

                    f"  {mapping_name} : {status}"

                )

                if not result.get(

                    "success"

                ):

                    print(

                        f"    Stage : "

                        f"{result.get('stage')}"

                    )

                    if result.get(

                        "error"

                    ):

                        print(

                            f"    Error : "

                            f"{result.get('error')}"

                        )

        print(

            "\n" + "=" * 60

        )

    # ============================================================

    # SAFE FILE NAME

    # ============================================================

    @staticmethod

    def _safe_filename(

        name

    ):

        """

        Convert mapping name into a safe file name.

        """

        value = str(

            name or "migration"

        )

        value = re.sub(

            r"[^A-Za-z0-9._-]+",

            "_",

            value

        )

        value = value.strip(

            "._"

        )

        return (

            value

            or "migration"

        )
