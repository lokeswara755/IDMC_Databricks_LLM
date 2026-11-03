import json
import pytest

from Human_GateWay.human_gateway import (
    HumanGateway,
    PASS,
    REVIEW_REQUIRED,
    REWORK,
)


# ============================================================
# BASIC STATUS TESTS
# ============================================================

def test_clean_mapping_passes():

    gateway = HumanGateway()

    result = gateway.evaluate(
        "m_customer_standardize",
        issues=[],
        validations_passed=True,
    )

    assert result["status"] == PASS
    assert gateway.can_proceed(result)


def test_info_issue_does_not_require_review():

    gateway = HumanGateway()

    result = gateway.evaluate(
        "m_customer_standardize",
        issues=[
            {
                "category": "INFORMATION",
                "message": "Informational note",
                "severity": "INFO",
            }
        ],
    )

    assert result["status"] == PASS


def test_business_issue_requires_review():

    gateway = HumanGateway()

    result = gateway.create_review_item(
        "m_order_enrichment",
        "UNVERIFIED_MERGE_KEY",
        "SME confirmation required",
    )

    assert result["status"] == REVIEW_REQUIRED
    assert not gateway.can_proceed(result)


def test_warning_requires_review():

    gateway = HumanGateway()

    result = gateway.create_review_item(
        "m_customer_standardize",
        "BUSINESS_RULE_AMBIGUITY",
        "Confirm null handling",
        severity="WARNING",
    )

    assert result["status"] == REVIEW_REQUIRED


# ============================================================
# TECHNICAL FAILURE TESTS
# ============================================================

def test_corrupted_xml_cannot_be_approved():

    gateway = HumanGateway()

    result = gateway.create_review_item(
        "XML_IMPORT",
        "CORRUPTED_XML",
        "Malformed XML",
        validations_passed=False,
    )

    assert result["status"] == REWORK

    approved = gateway.record_decision(
        result,
        "APPROVE",
        "reviewer",
    )

    assert approved["status"] == REWORK
    assert not gateway.can_proceed(approved)


def test_pyspark_validation_failure_cannot_be_approved():

    gateway = HumanGateway()

    result = gateway.create_review_item(
        "m_daily_sales_summary",
        "PYSPARK_VALIDATION_FAILED",
        "Syntax error",
        validations_passed=False,
    )

    approved = gateway.record_decision(
        result,
        "APPROVE",
        "reviewer",
    )

    assert approved["status"] == REWORK
    assert not approved["can_execute"]
    assert not gateway.can_proceed(approved)


def test_data_validation_failure_cannot_be_approved():

    gateway = HumanGateway()

    result = gateway.create_review_item(
        "m_customer_standardize",
        "DATA_VALIDATION_FAILED",
        "Target validation failed.",
        validations_passed=False,
    )

    assert result["status"] == REWORK

    approved = gateway.record_decision(
        result,
        "APPROVE",
        "reviewer",
    )

    assert approved["status"] == REWORK
    assert not gateway.can_proceed(approved)


# ============================================================
# HUMAN DECISION TESTS
# ============================================================

def test_approval_resolves_business_issue():

    gateway = HumanGateway()

    result = gateway.create_review_item(
        "m_order_enrichment",
        "UNVERIFIED_MERGE_KEY",
        "Confirm merge key",
    )

    approved = gateway.record_decision(
        result,
        "APPROVE",
        "data-engineering-sme",
        "Confirmed for POC",
    )

    assert approved["status"] == PASS
    assert gateway.can_proceed(approved)

    assert len(approved["resolved_issues"]) == 1
    assert approved["issues"] == []


def test_rejection_blocks_execution():

    gateway = HumanGateway()

    result = gateway.create_review_item(
        "m_customer_standardize",
        "BUSINESS_RULE_AMBIGUITY",
        "Confirm null handling",
    )

    rejected = gateway.record_decision(
        result,
        "REJECT",
        "reviewer",
        "Not approved",
    )

    assert rejected["status"] == REWORK
    assert not gateway.can_proceed(rejected)


def test_request_changes_blocks_execution():

    gateway = HumanGateway()

    result = gateway.create_review_item(
        "m_customer_standardize",
        "BUSINESS_RULE_AMBIGUITY",
        "Clarify transformation logic",
    )

    updated = gateway.record_decision(
        result,
        "REQUEST_CHANGES",
        "reviewer",
        "Please correct null handling.",
    )

    assert updated["status"] == REWORK
    assert not gateway.can_proceed(updated)


def test_invalid_decision_rejected():

    gateway = HumanGateway()

    result = gateway.create_review_item(
        "m_customer_standardize",
        "BUSINESS_RULE_AMBIGUITY",
        "Review required",
    )

    with pytest.raises(ValueError):

        gateway.record_decision(
            result,
            "MAYBE",
            "reviewer",
        )


def test_missing_reviewer_rejected():

    gateway = HumanGateway()

    result = gateway.create_review_item(
        "m_customer_standardize",
        "BUSINESS_RULE_AMBIGUITY",
        "Review required",
    )

    with pytest.raises(ValueError):

        gateway.record_decision(
            result,
            "APPROVE",
            "",
        )


# ============================================================
# DUPLICATE DECISION PROTECTION
# ============================================================

def test_duplicate_decision_is_rejected(tmp_path):

    gateway = HumanGateway(str(tmp_path))

    result = gateway.create_review_item(
        "m_customer_standardize",
        "BUSINESS_RULE_AMBIGUITY",
        "Review required",
    )

    gateway.record_decision_by_id(
        result["review_id"],
        "APPROVE",
        "reviewer",
    )

    with pytest.raises(ValueError):

        gateway.record_decision_by_id(
            result["review_id"],
            "REJECT",
            "another-reviewer",
        )


def test_decision_history_is_preserved(tmp_path):

    gateway = HumanGateway(str(tmp_path))

    result = gateway.create_review_item(
        "m_order_enrichment",
        "UNVERIFIED_MERGE_KEY",
        "Confirm key",
    )

    approved = gateway.record_decision_by_id(
        result["review_id"],
        "APPROVE",
        "reviewer",
        "Approved for POC",
    )

    assert len(approved["decision_history"]) == 1

    decision = approved["decision_history"][0]

    assert decision["decision"] == "APPROVE"
    assert decision["reviewer"] == "reviewer"


# ============================================================
# PERSISTENCE TESTS
# ============================================================

def test_review_persistence(tmp_path):

    gateway = HumanGateway(str(tmp_path))

    result = gateway.create_review_item(
        "m_order_enrichment",
        "BUSINESS_RULE_AMBIGUITY",
        "Review required",
    )

    loaded = gateway.get_review(result["review_id"])

    assert loaded["review_id"] == result["review_id"]
    assert loaded["status"] == REVIEW_REQUIRED


def test_decision_persistence(tmp_path):

    gateway = HumanGateway(str(tmp_path))

    result = gateway.create_review_item(
        "m_order_enrichment",
        "BUSINESS_RULE_AMBIGUITY",
        "Review required",
    )

    approved = gateway.record_decision_by_id(
        result["review_id"],
        "APPROVE",
        "reviewer",
        "Approved",
    )

    persisted = json.loads(
        (
            tmp_path / f"{result['review_id']}.json"
        ).read_text(encoding="utf-8")
    )

    assert persisted["status"] == PASS
    assert persisted["decision"]["decision"] == "APPROVE"


def test_pending_reviews_retrieval(tmp_path):

    gateway = HumanGateway(str(tmp_path))

    first = gateway.create_review_item(
        "m_customer_standardize",
        "BUSINESS_RULE_AMBIGUITY",
        "Review one",
    )

    second = gateway.create_review_item(
        "m_order_enrichment",
        "UNVERIFIED_MERGE_KEY",
        "Review two",
    )

    pending = gateway.list_pending_reviews()

    pending_ids = {
        review["review_id"]
        for review in pending
    }

    assert first["review_id"] in pending_ids
    assert second["review_id"] in pending_ids


def test_pending_reviews_excludes_approved_records(tmp_path):

    gateway = HumanGateway(str(tmp_path))

    result = gateway.create_review_item(
        "m_customer_standardize",
        "BUSINESS_RULE_AMBIGUITY",
        "Review required",
    )

    gateway.record_decision_by_id(
        result["review_id"],
        "APPROVE",
        "reviewer",
    )

    pending = gateway.list_pending_reviews()

    assert all(
        item["review_id"] != result["review_id"]
        for item in pending
    )


def test_list_reviews_filter_by_mapping(tmp_path):

    gateway = HumanGateway(str(tmp_path))

    gateway.create_review_item(
        "m_customer_standardize",
        "BUSINESS_RULE_AMBIGUITY",
        "Review one",
    )

    gateway.create_review_item(
        "m_order_enrichment",
        "UNVERIFIED_MERGE_KEY",
        "Review two",
    )

    results = gateway.list_reviews(
        mapping_name="m_customer_standardize"
    )

    assert len(results) == 1
    assert results[0]["mapping_name"] == "m_customer_standardize"


def test_get_review_by_mapping(tmp_path):

    gateway = HumanGateway(str(tmp_path))

    result = gateway.create_review_item(
        "m_customer_standardize",
        "BUSINESS_RULE_AMBIGUITY",
        "Review required",
    )

    loaded = gateway.get_review_by_mapping(
        "m_customer_standardize",
        status=REVIEW_REQUIRED,
    )

    assert loaded["review_id"] == result["review_id"]


# ============================================================
# SECURITY / INTEGRITY TESTS
# ============================================================

def test_invalid_review_id_rejected():

    gateway = HumanGateway()

    with pytest.raises(ValueError):

        gateway.get_review("../../invalid")


def test_nonexistent_review_rejected(tmp_path):

    gateway = HumanGateway(str(tmp_path))

    with pytest.raises(FileNotFoundError):

        gateway.get_review(
            "372970c3-de37-4ce1-9614-7853464764df"
        )


def test_modified_review_dict_cannot_overwrite_persisted_record(tmp_path):

    gateway = HumanGateway(str(tmp_path))

    result = gateway.create_review_item(
        "m_customer_standardize",
        "BUSINESS_RULE_AMBIGUITY",
        "Original review",
    )

    # Attempt to modify the caller-side review object.
    forged = dict(result)
    forged["status"] = PASS
    forged["validations_passed"] = True

    with pytest.raises(ValueError):

        gateway.record_decision(
            forged,
            "APPROVE",
            "reviewer",
        )

    stored = gateway.get_review(result["review_id"])

    assert stored["status"] == REVIEW_REQUIRED
    assert stored["decision"] is None


# ============================================================
# EXECUTION SAFETY TESTS
# ============================================================

def test_live_execution_requires_pipeline_approval():

    gateway = HumanGateway()

    result = gateway.evaluate(
        "m_customer_standardize",
        live_execution=True,
    )

    assert result["status"] == PASS

    assert not gateway.can_proceed(
        result,
        execution_approved=False,
    )

    assert gateway.can_proceed(
        result,
        execution_approved=True,
    )


def test_review_approval_does_not_replace_execution_approval(tmp_path):

    gateway = HumanGateway(str(tmp_path))

    result = gateway.create_review_item(
        "m_customer_standardize",
        "UNVERIFIED_MERGE_KEY",
        "Confirm synthetic merge key",
        live_execution=True,
    )

    approved = gateway.record_decision_by_id(
        result["review_id"],
        "APPROVE",
        "reviewer",
    )

    assert approved["status"] == PASS

    assert not gateway.can_proceed(
        approved,
        execution_approved=False,
    )

    assert gateway.can_proceed(
        approved,
        execution_approved=True,
    )