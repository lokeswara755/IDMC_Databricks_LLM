
class DataQualityRuleParser:
    def parse(self, root):
        assets = root.find("./Project/Assets")

        if assets is None:
            raise ValueError("Project/Assets not found in XML")

        rules = [
            self._parse_rule(element)
            for element in list(assets)
            if element.tag == "DataQualityRule"
        ]

        return {
            "data_quality_rules": rules,
            "summary": {
                "rule_count": len(rules),
                "enabled_count": sum(
                    rule["enabled"] == "true"
                    for rule in rules
                ),
                "severity_counts": self._count_severity(rules),
                "missing_expression_count": sum(
                    not rule["rule_expression"]
                    for rule in rules
                ),
            },
        }

    def _parse_rule(self, element):
        expression = element.find("RuleExpression")
        failure_action = element.find("FailureAction")

        return {
            "id": element.get("id"),
            "name": element.get("name"),
            "severity": element.get("severity"),
            "enabled": element.get("enabled"),
            "attributes": dict(element.attrib),

            "rule_expression": (
                expression.text.strip()
                if expression is not None and expression.text
                else None
            ),

            "failure_action": (
                failure_action.text.strip()
                if failure_action is not None and failure_action.text
                else None
            ),

            # Preserve additional XML children without losing
            # their nested attributes or text.
            "extensions": [
                self._element_to_dict(child)
                for child in list(element)
                if child.tag not in {
                    "RuleExpression",
                    "FailureAction",
                }
            ],

            # Retain the complete original XML structure.
            "raw_xml": self._element_to_dict(element),
        }

    @staticmethod
    def _count_severity(rules):
        counts = {}

        for rule in rules:
            severity = rule["severity"] or "UNSPECIFIED"
            counts[severity] = counts.get(severity, 0) + 1

        return counts

    @staticmethod
    def _element_to_dict(element):
        return {
            "tag": element.tag,
            "attributes": dict(element.attrib),
            "text": (element.text or "").strip() or None,
            "children": [
                DataQualityRuleParser._element_to_dict(child)
                for child in list(element)
            ],
        }
