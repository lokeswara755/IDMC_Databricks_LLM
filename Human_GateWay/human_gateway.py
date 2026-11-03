
"""
Human Gateway for the IDMC-to-Databricks migration platform.

Responsibilities:
- Evaluate migration issues and determine PASS, REVIEW_REQUIRED, or REWORK.
- Persist review records as JSON.
- Record human decisions with an audit history.
- Prevent human approval from bypassing technical validation.
- Provide review lookup and pending-review retrieval.
- Keep live-execution approval separate from human approval.

This module is independent of Spark and the LLM.
"""

from __future__ import annotations

import json
import os
import tempfile
import uuid

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional


# ---------------------------------------------------------------------
# STATUS CONSTANTS
# ---------------------------------------------------------------------

PASS = "PASS"
REVIEW_REQUIRED = "REVIEW_REQUIRED"
REWORK = "REWORK"


# ---------------------------------------------------------------------
# DECISION CONSTANTS
# ---------------------------------------------------------------------

VALID_DECISIONS = {
    "APPROVE",
    "REJECT",
    "REQUEST_CHANGES",
}

VALID_SEVERITIES = {
    "BLOCKER",
    "WARNING",
    "INFO",
}


# ---------------------------------------------------------------------
# TECHNICAL ISSUES THAT HUMAN APPROVAL CANNOT OVERRIDE
# ---------------------------------------------------------------------

NON_OVERRIDABLE_CATEGORIES = {
    "CORRUPTED_XML",
    "MALFORMED_XML",
    "PYSPARK_VALIDATION_FAILED",
    "SOURCE_PREFLIGHT_FAILED",
    "GRAPH_BUILD_FAILED",
    "LLM_GENERATION_FAILED",
    "VALIDATION_FAILED",
    "DATA_VALIDATION_FAILED",
    "APPROVAL_CANNOT_BYPASS_VALIDATION",
}


# ---------------------------------------------------------------------
# BUSINESS / MANUAL REVIEW CATEGORIES
# ---------------------------------------------------------------------

REVIEWABLE_CATEGORIES = {
    "UNVERIFIED_MERGE_KEY",
    "BUSINESS_RULE_AMBIGUITY",
    "SCHEMA_MAPPING_AMBIGUITY",
    "DATA_TYPE_MAPPING_AMBIGUITY",
    "NULL_HANDLING_AMBIGUITY",
    "SCD_RULE_AMBIGUITY",
    "TARGET_MAPPING_AMBIGUITY",
    "MANUAL_REVIEW_REQUIRED",
}


class HumanGateway:
    """
    Create, persist, retrieve, and evaluate human review records.

    A human APPROVE decision can resolve a reviewable business issue.

    It cannot bypass a technical validation failure.

    Live execution additionally requires explicit pipeline approval.
    """

    def __init__(self, review_dir: Optional[str] = None):

        self.review_dir = str(review_dir) if review_dir else None

        # In-memory storage supports unit tests and non-persistent use.
        self._memory_reviews: Dict[str, Dict[str, Any]] = {}

        if self.review_dir:
            os.makedirs(self.review_dir, exist_ok=True)

    # -----------------------------------------------------------------
    # COMMON HELPERS
    # -----------------------------------------------------------------

    @staticmethod
    def _now() -> str:
        return datetime.now(timezone.utc).isoformat()

    @staticmethod
    def _validate_review_id(review_id: str) -> str:
        """
        Validate review ID before accessing storage.

        UUID validation also prevents path traversal.
        """

        if not review_id or not isinstance(review_id, str):
            raise ValueError("A valid review_id is required.")

        try:
            parsed = uuid.UUID(review_id)
        except (ValueError, TypeError, AttributeError):
            raise ValueError("Invalid review_id. Expected a UUID.")

        if str(parsed) != review_id.lower():
            raise ValueError("Invalid review_id format.")

        return review_id

    @staticmethod
    def _normalize_issue(raw: Any) -> Dict[str, Any]:

        if isinstance(raw, dict):
            item = dict(raw)
        else:
            item = {
                "category": "UNCLASSIFIED",
                "message": str(raw),
            }

        item["category"] = str(
            item.get("category") or "UNCLASSIFIED"
        ).strip().upper()

        item["message"] = str(
            item.get("message") or "No issue description supplied"
        )

        severity = str(
            item.get("severity") or "BLOCKER"
        ).strip().upper()

        if severity not in VALID_SEVERITIES:
            severity = "BLOCKER"

        item["severity"] = severity

        return item

    @staticmethod
    def _has_technical_blocker(issues: List[Dict[str, Any]]) -> bool:

        return any(
            issue["severity"] == "BLOCKER"
            and issue["category"] in NON_OVERRIDABLE_CATEGORIES
            for issue in issues
        )

    def _store_memory(self, review: Dict[str, Any]) -> None:

        self._memory_reviews[review["review_id"]] = dict(review)

    # -----------------------------------------------------------------
    # REVIEW EVALUATION
    # -----------------------------------------------------------------

    def evaluate(
        self,
        mapping_name: str,
        issues: Optional[Iterable[Dict[str, Any]]] = None,
        *,
        validations_passed: bool = True,
        live_execution: bool = False,
        scope: str = "MAPPING",
    ) -> Dict[str, Any]:
        """
        Evaluate mapping issues.

        PASS:
            No issues and mandatory validations passed.

        REVIEW_REQUIRED:
            A human decision is required for a reviewable issue.

        REWORK:
            A mandatory technical validation failed.
        """

        normalized = [
            self._normalize_issue(issue)
            for issue in (issues or [])
        ]

        # A failed mandatory validation must always create a blocker.
        if not validations_passed and not any(
            issue["category"] in NON_OVERRIDABLE_CATEGORIES
            for issue in normalized
        ):
            normalized.append({
                "category": "VALIDATION_FAILED",
                "message": (
                    "One or more mandatory technical validations failed."
                ),
                "severity": "BLOCKER",
            })

        technical_failure = (
            not validations_passed
            or self._has_technical_blocker(normalized)
        )

        if technical_failure:
            status = REWORK
        elif any(issue["severity"] != "INFO" for issue in normalized):
            # Informational issues are retained for visibility, but they
            # do not require human review or block execution.
            status = REVIEW_REQUIRED
        else:
            status = PASS

        now = self._now()

        review = {
            "schema_version": 1,
            "review_id": str(uuid.uuid4()),
            "scope": str(scope).upper(),
            "mapping_name": str(mapping_name),
            "status": status,
            "can_execute": (
                status == PASS and bool(validations_passed)
            ),
            "live_execution": bool(live_execution),
            "validations_passed": bool(validations_passed),
            "issues": normalized,
            "resolved_issues": [],
            "resolved_categories": [],
            "decision": None,
            "decision_history": [],
            "created_at": now,
            "updated_at": now,
        }

        self._store_memory(review)

        if self.review_dir:
            self._persist(review)

        return dict(review)

    # -----------------------------------------------------------------
    # CREATE REVIEW ITEM
    # -----------------------------------------------------------------

    def create_review_item(
        self,
        mapping_name: str,
        category: str,
        message: str,
        *,
        severity: str = "BLOCKER",
        evidence: Any = None,
        validations_passed: bool = True,
        live_execution: bool = False,
        scope: str = "MAPPING",
    ) -> Dict[str, Any]:
        """
        Create a review record for a specific issue.

        Business ambiguity normally creates REVIEW_REQUIRED.

        A technical failure must be supplied with
        validations_passed=False or a non-overridable category.
        """

        issue = {
            "category": category,
            "message": message,
            "severity": severity,
        }

        if evidence is not None:
            issue["evidence"] = evidence

        return self.evaluate(
            mapping_name,
            issues=[issue],
            validations_passed=validations_passed,
            live_execution=live_execution,
            scope=scope,
        )

    # -----------------------------------------------------------------
    # PERSISTENCE
    # -----------------------------------------------------------------

    def _persist(self, review: Dict[str, Any]) -> None:
        """
        Persist JSON using an atomic file replacement.
        """

        if not self.review_dir:
            return

        directory = Path(self.review_dir)
        directory.mkdir(parents=True, exist_ok=True)

        review_id = self._validate_review_id(
            review["review_id"]
        )

        destination = directory / f"{review_id}.json"

        fd, temp_path = tempfile.mkstemp(
            prefix=".review_",
            suffix=".tmp",
            dir=str(directory),
        )

        try:
            with os.fdopen(
                fd,
                "w",
                encoding="utf-8",
            ) as handle:

                json.dump(
                    review,
                    handle,
                    indent=2,
                    ensure_ascii=False,
                    default=str,
                )

                handle.flush()
                os.fsync(handle.fileno())

            os.replace(temp_path, destination)

        finally:
            if os.path.exists(temp_path):
                os.remove(temp_path)

    # -----------------------------------------------------------------
    # LOAD REVIEW
    # -----------------------------------------------------------------

    def get_review(self, review_id: str) -> Dict[str, Any]:
        """
        Retrieve a review by UUID.

        ID validation happens before storage configuration checks.
        """

        review_id = self._validate_review_id(review_id)

        if not self.review_dir:
            if review_id in self._memory_reviews:
                return dict(self._memory_reviews[review_id])

            raise RuntimeError(
                "review_dir is not configured. "
                "Persistent review operations are unavailable."
            )

        path = Path(self.review_dir) / f"{review_id}.json"

        if not path.is_file():
            raise FileNotFoundError(
                f"Review record not found: {review_id}"
            )

        with path.open("r", encoding="utf-8") as handle:
            review = json.load(handle)

        if review.get("review_id") != review_id:
            raise ValueError(
                "Review ID does not match stored record."
            )

        return review

    # -----------------------------------------------------------------
    # LIST REVIEWS
    # -----------------------------------------------------------------

    def list_reviews(
        self,
        mapping_name: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Return review records.

        Optionally filter by mapping name.
        """

        reviews = {}

        # Include in-memory records.
        for review_id, review in self._memory_reviews.items():
            reviews[review_id] = dict(review)

        # Persistent records are authoritative.
        if self.review_dir:
            directory = Path(self.review_dir)

            if directory.exists():
                for path in directory.glob("*.json"):
                    try:
                        with path.open(
                            "r",
                            encoding="utf-8",
                        ) as handle:
                            review = json.load(handle)

                        review_id = review.get("review_id")

                        if review_id:
                            reviews[review_id] = review

                    except (
                        OSError,
                        json.JSONDecodeError,
                        TypeError,
                    ):
                        # Ignore incomplete or invalid review files.
                        continue

        result = list(reviews.values())

        if mapping_name is not None:
            result = [
                review
                for review in result
                if review.get("mapping_name") == mapping_name
            ]

        result.sort(
            key=lambda item: item.get("created_at", ""),
            reverse=True,
        )

        return result

    # -----------------------------------------------------------------
    # LIST PENDING REVIEWS
    # -----------------------------------------------------------------

    def list_pending_reviews(
        self,
        mapping_name: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Return only undecided REVIEW_REQUIRED records.

        REWORK records are technical blockers, not pending
        human approvals.
        """

        reviews = self.list_reviews(mapping_name=mapping_name)

        return [
            review
            for review in reviews
            if (
                review.get("status") == REVIEW_REQUIRED
                and review.get("decision") is None
            )
        ]

    # -----------------------------------------------------------------
    # GET LATEST REVIEW FOR A MAPPING
    # -----------------------------------------------------------------

    def get_review_by_mapping(
        self,
        mapping_name: str,
        status: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        Return the latest review record for a mapping.

        Optionally filter by status (for example, REVIEW_REQUIRED).
        Returns None when no matching record exists.
        """

        reviews = self.list_reviews(mapping_name=mapping_name)

        if status is not None:
            expected_status = str(status).strip().upper()
            reviews = [
                review
                for review in reviews
                if str(review.get("status", "")).strip().upper()
                == expected_status
            ]

        if not reviews:
            return None

        return reviews[0]

    # -----------------------------------------------------------------
    # RECORD HUMAN DECISION
    # -----------------------------------------------------------------

    def record_decision(
        self,
        review: Dict[str, Any],
        decision: str,
        reviewer: str,
        comment: str = "",
    ) -> Dict[str, Any]:
        """
        Record a human decision.

        Persisted data is authoritative when storage is configured.

        A modified caller-side review dictionary is rejected.

        Technical REWORK records can record an attempted APPROVE
        for audit purposes, but they remain REWORK.
        """

        decision = str(decision).strip().upper()

        if decision not in VALID_DECISIONS:
            raise ValueError(
                "decision must be APPROVE, REJECT, or REQUEST_CHANGES"
            )

        if not reviewer or not str(reviewer).strip():
            raise ValueError("reviewer is required")

        if not isinstance(review, dict):
            raise TypeError("review must be a dictionary")

        review_id = self._validate_review_id(
            review.get("review_id")
        )

        # Always retrieve the authoritative record.
        if self.review_dir:
            current = self.get_review(review_id)
        else:
            current = self._memory_reviews.get(review_id)

            if current is None:
                raise FileNotFoundError(
                    f"Review record not found: {review_id}"
                )

            current = dict(current)

        # Reject tampering with critical caller-side fields.
        protected_fields = (
            "review_id",
            "mapping_name",
            "status",
            "validations_passed",
            "issues",
            "live_execution",
        )

        for field in protected_fields:
            if field in review and review[field] != current.get(field):
                raise ValueError(
                    f"Review data was modified by the caller: {field}"
                )

        current_status = current.get("status")

        if current_status not in {REVIEW_REQUIRED, REWORK}:
            raise ValueError(
                "A decision can only be recorded for a "
                "REVIEW_REQUIRED or REWORK record."
            )

        if current_status == REWORK and decision != "APPROVE":
            raise ValueError(
                "Only an attempted APPROVE decision can be "
                "recorded for a REWORK record."
            )

        if current.get("decision") is not None:
            raise ValueError(
                "A decision has already been recorded for this review."
            )

        issues = [
            self._normalize_issue(issue)
            for issue in current.get("issues", [])
        ]

        categories = {
            issue["category"]
            for issue in issues
        }

        validations_passed = bool(
            current.get("validations_passed", False)
        )

        now = self._now()

        decision_record = {
            "decision": decision,
            "reviewer": str(reviewer).strip(),
            "comment": str(comment or ""),
            "decided_at": now,
        }

        current["decision"] = decision_record

        current.setdefault("decision_history", []).append(
            dict(decision_record)
        )

        current["updated_at"] = now

        if decision in {"REJECT", "REQUEST_CHANGES"}:

            current["status"] = REWORK

        elif (
            categories & NON_OVERRIDABLE_CATEGORIES
            or not validations_passed
            or self._has_technical_blocker(issues)
        ):
            # Technical failures can never be approved away.
            current["status"] = REWORK

            if not any(
                issue["category"]
                == "APPROVAL_CANNOT_BYPASS_VALIDATION"
                for issue in issues
            ):
                issues.append({
                    "category": "APPROVAL_CANNOT_BYPASS_VALIDATION",
                    "message": (
                        "Approval cannot replace a mandatory "
                        "technical fix and revalidation."
                    ),
                    "severity": "BLOCKER",
                })

        else:
            # Human approval resolves reviewable business blockers.
            current["status"] = PASS

            current["resolved_categories"] = sorted(
                set(current.get("resolved_categories", []))
                | categories
            )

            current["resolved_issues"] = (
                current.get("resolved_issues", []) + issues
            )

            issues = [
                issue
                for issue in issues
                if issue["severity"] != "BLOCKER"
            ]

        current["issues"] = issues

        current["can_execute"] = (
            current["status"] == PASS
            and validations_passed
        )

        self._store_memory(current)

        if self.review_dir:
            self._persist(current)

        return dict(current)

    # -----------------------------------------------------------------
    # RECORD DECISION USING REVIEW ID
    # -----------------------------------------------------------------

    def record_decision_by_id(
        self,
        review_id: str,
        decision: str,
        reviewer: str,
        comment: str = "",
    ) -> Dict[str, Any]:

        review = self.get_review(review_id)

        return self.record_decision(
            review=review,
            decision=decision,
            reviewer=reviewer,
            comment=comment,
        )

    # -----------------------------------------------------------------
    # EXECUTION GATE
    # -----------------------------------------------------------------

    @staticmethod
    def can_proceed(
        review: Dict[str, Any],
        *,
        execution_approved: bool = False,
    ) -> bool:
        """
        Check whether the migration may proceed.

        Human approval does not automatically authorize live execution.
        """

        if not isinstance(review, dict):
            return False

        if review.get("status") != PASS:
            return False

        if not review.get("validations_passed", False):
            return False

        if review.get("can_execute") is False:
            return False

        decision = review.get("decision")

        if decision:
            if decision.get("decision") != "APPROVE":
                return False

        if (
            review.get("live_execution", False)
            and not execution_approved
        ):
            return False

        return True
